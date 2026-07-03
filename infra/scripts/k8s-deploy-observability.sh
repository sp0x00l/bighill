#!/usr/bin/env bash
set -euo pipefail

ENVIRONMENT="${1:-}"
REGION="${AWS_REGION:-us-east-1}"

if [ -z "$ENVIRONMENT" ]; then
  echo "Usage: $0 <staging|prod>"
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
CLUSTER_NAME="ml-ops-${ENVIRONMENT}"
NAMESPACE="observability"
PUBLIC_ROOT_DOMAIN="${PUBLIC_ROOT_DOMAIN:-bighill.example}"
GRAFANA_HOSTNAME="observability.${ENVIRONMENT}.${PUBLIC_ROOT_DOMAIN}"

configure_kubectl() {
  local CLUSTER="$1"
  local REGION_ARG="$2"

  echo "Configuring kubectl for EKS cluster '${CLUSTER}'..."
  if [ -n "${AWS_PROFILE:-}" ]; then
    aws eks update-kubeconfig --name "${CLUSTER}" --region "${REGION_ARG}" --profile "${AWS_PROFILE}"
  else
    aws eks update-kubeconfig --name "${CLUSTER}" --region "${REGION_ARG}"
  fi
}

create_namespace() {
  local NS="$1"

  echo "Creating namespace '${NS}' if it doesn't exist..."
  kubectl create namespace "${NS}" --dry-run=client -o yaml | kubectl apply -f -
  kubectl label namespace "${NS}" name="${NS}" --overwrite
}

get_grafana_password() {
  local SSM_PARAM_NAME="/bighill/${ENVIRONMENT}/grafana-admin-password"
  local PASSWORD=""

  PASSWORD=$(aws ssm get-parameter \
    --name "${SSM_PARAM_NAME}" \
    --with-decryption \
    --query 'Parameter.Value' \
    --output text \
    --region "${REGION}" 2>/dev/null || echo "")

  if [ -z "$PASSWORD" ]; then
    echo "Warning: Grafana password not found in SSM. Using default." >&2
    PASSWORD="admin"
  fi

  echo "$PASSWORD"
}

create_grafana_secret() {
  local NS="$1"
  local ADMIN_USER="admin"
  local ADMIN_PASSWORD=$(get_grafana_password)

  echo "Creating Grafana admin credentials secret..."
  kubectl create secret generic grafana-admin-credentials \
    --namespace "${NS}" \
    --from-literal=admin-user="${ADMIN_USER}" \
    --from-literal=admin-password="${ADMIN_PASSWORD}" \
    --dry-run=client -o yaml | kubectl apply -f -
}

apply_grafana_dashboards() {
  local NS="$1"
  local DASHBOARD_DIR="${PROJECT_ROOT}/infra/grafana"

  if [ ! -d "${DASHBOARD_DIR}" ]; then
    echo "No Grafana dashboard directory found at ${DASHBOARD_DIR}; skipping dashboard ConfigMaps."
    return
  fi

  shopt -s nullglob
  local DASHBOARDS=("${DASHBOARD_DIR}"/*.json)
  shopt -u nullglob

  if [ "${#DASHBOARDS[@]}" -eq 0 ]; then
    echo "No Grafana dashboard JSON files found in ${DASHBOARD_DIR}; skipping dashboard ConfigMaps."
    return
  fi

  echo "Applying Grafana dashboard ConfigMaps from ${DASHBOARD_DIR}..."
  local DASHBOARD=""
  for DASHBOARD in "${DASHBOARDS[@]}"; do
    local BASENAME=""
    local NAME=""
    BASENAME="$(basename "${DASHBOARD}" .json)"
    NAME="grafana-dashboard-$(echo "${BASENAME}" | tr '[:upper:]_' '[:lower:]-' | sed 's/[^a-z0-9-]/-/g; s/--*/-/g; s/^-//; s/-$//')"
    if [ "${#NAME}" -gt 63 ]; then
      NAME="${NAME:0:63}"
      NAME="${NAME%-}"
    fi

    if command -v jq >/dev/null 2>&1; then
      jq empty "${DASHBOARD}"
    fi

    kubectl create configmap "${NAME}" \
      --namespace "${NS}" \
      --from-file="${BASENAME}.json=${DASHBOARD}" \
      --dry-run=client -o yaml \
      | kubectl label --local -f - -o yaml \
          grafana_dashboard=1 \
          app.kubernetes.io/managed-by=ml-ops-infra \
      | kubectl annotate --local -f - -o yaml \
          grafana_folder=/tmp/dashboards/BigHill \
      | kubectl apply -f -
  done
}

install_prometheus_stack() {
  local NS="$1"
  local HOSTNAME="$2"
  local VALUES_FILE="$3"
  local ACM_CERT_ARN="$4"

  echo "Installing kube-prometheus-stack..."
  helm repo add prometheus-community https://prometheus-community.github.io/helm-charts 2>/dev/null || true
  helm repo update prometheus-community

  # Clean up stuck Helm release
  cleanup_stuck_helm_release "kube-prometheus-stack" "${NS}"

  helm upgrade --install kube-prometheus-stack prometheus-community/kube-prometheus-stack \
    --namespace "${NS}" \
    --version "80.0.0" \
    --values "${VALUES_FILE}" \
    --set "grafana.ingress.hosts[0]=${HOSTNAME}" \
    --set "grafana.ingress.annotations.external-dns\\.alpha\\.kubernetes\\.io/hostname=${HOSTNAME}" \
    --set "grafana.ingress.annotations.alb\\.ingress\\.kubernetes\\.io/certificate-arn=${ACM_CERT_ARN}" \
    --set "grafana.admin.existingSecret=grafana-admin-credentials" \
    --set "grafana.admin.userKey=admin-user" \
    --set "grafana.admin.passwordKey=admin-password" \
    --wait \
    --timeout 15m
}

cleanup_statefulset() {
  local NS="$1"
  local NAME="$2"
  local PVC_NAME="$3"

  # Check if the PVC exists and is pending (stuck due to AZ mismatch or no storage class)
  local PVC_STATUS=""
  PVC_STATUS=$(kubectl get pvc "${PVC_NAME}" -n "${NS}" -o jsonpath='{.status.phase}' 2>/dev/null || echo "")

  if [ "$PVC_STATUS" = "Pending" ]; then
    echo "Found pending PVC ${PVC_NAME}..."
    echo "Forcing cleanup of ${NAME} StatefulSet and pending PVC..."
    kubectl delete statefulset "${NAME}" -n "${NS}" --cascade=foreground --ignore-not-found 2>/dev/null || true
    kubectl wait --for=delete pod/"${NAME}-0" -n "${NS}" --timeout=60s 2>/dev/null || true
    kubectl delete pvc "${PVC_NAME}" -n "${NS}" --ignore-not-found 2>/dev/null || true
    echo "Cleanup complete for ${NAME} - will be recreated with fresh storage"
  fi
}

cleanup_stuck_helm_release() {
  local RELEASE="$1"
  local NS="$2"
  
  local STATUS=""
  STATUS=$(helm status "${RELEASE}" -n "${NS}" -o json 2>/dev/null | grep -o '"status":"[^"]*"' | cut -d'"' -f4 || echo "")
  
  if [ "$STATUS" = "pending-install" ] || [ "$STATUS" = "pending-upgrade" ] || [ "$STATUS" = "pending-rollback" ]; then
    echo "Found stuck Helm release ${RELEASE} in state: ${STATUS}"
    echo "Force uninstalling release (skipping hooks)..."
    helm uninstall "${RELEASE}" -n "${NS}" --no-hooks --timeout 30s 2>/dev/null || {
      echo "Helm uninstall failed, deleting secrets directly..."
      kubectl delete secret -n "${NS}" -l "owner=helm,name=${RELEASE}" --ignore-not-found 2>/dev/null || true
    }
    # Wait for resources to clean up
    sleep 5
    echo "Cleanup complete for ${RELEASE}"
  fi
}

install_tempo() {
  local NS="$1"
  local VALUES_FILE="$2"
  local TEMPO_RETENTION_ARGS=()

  echo "Installing Grafana Tempo..."
  helm repo add grafana https://grafana.github.io/helm-charts 2>/dev/null || true
  helm repo update grafana

  # Clean up stuck Helm release
  cleanup_stuck_helm_release "tempo" "${NS}"
  
  # Clean up if PVC is stuck or pod is crashlooping
  cleanup_statefulset "${NS}" "tempo" "storage-tempo-0"

  if [ "${ENVIRONMENT}" = "staging" ]; then
    local STAGING_TEMPO_RETENTION="${TEMPO_RETENTION:-3h}"
    echo "Using staging Tempo retention: ${STAGING_TEMPO_RETENTION}"
    TEMPO_RETENTION_ARGS+=(--set "tempo.retention=${STAGING_TEMPO_RETENTION}")
  fi

  helm upgrade --install tempo grafana/tempo \
    --namespace "${NS}" \
    --version "1.24.1" \
    --values "${VALUES_FILE}" \
    "${TEMPO_RETENTION_ARGS[@]}" \
    --timeout 5m
}

install_loki() {
  local NS="$1"
  local VALUES_FILE="$2"

  echo "Installing Grafana Loki..."
  helm repo add grafana https://grafana.github.io/helm-charts 2>/dev/null || true
  helm repo update grafana

  # Clean up stuck Helm release
  cleanup_stuck_helm_release "loki" "${NS}"

  # Clean up if PVC is stuck or StatefulSet needs recreation
  cleanup_statefulset "${NS}" "loki" "storage-loki-0"
  
  # Clean up old cache pods and canary from previous distributed install
  kubectl delete statefulset loki-chunks-cache loki-results-cache -n "${NS}" --ignore-not-found 2>/dev/null || true
  kubectl delete daemonset loki-canary -n "${NS}" --ignore-not-found 2>/dev/null || true
  # Also delete any remaining pods from these controllers
  kubectl delete pods -n "${NS}" -l app.kubernetes.io/component=memcached-chunks-cache --ignore-not-found 2>/dev/null || true
  kubectl delete pods -n "${NS}" -l app.kubernetes.io/component=memcached-results-cache --ignore-not-found 2>/dev/null || true
  kubectl delete pods -n "${NS}" -l app.kubernetes.io/component=canary --ignore-not-found 2>/dev/null || true

  helm upgrade --install loki grafana/loki \
    --namespace "${NS}" \
    --version "6.49.0" \
    --values "${VALUES_FILE}" \
    --wait \
    --timeout 10m
}

install_otel_collector() {
  local NS="$1"
  local VALUES_FILE="$2"

  echo "Installing OpenTelemetry Collector..."
  helm repo add open-telemetry https://open-telemetry.github.io/opentelemetry-helm-charts 2>/dev/null || true
  helm repo update open-telemetry

  # Clean up stuck Helm release
  cleanup_stuck_helm_release "otel-collector" "${NS}"

  helm upgrade --install otel-collector open-telemetry/opentelemetry-collector \
    --namespace "${NS}" \
    --version "0.141.0" \
    --values "${VALUES_FILE}" \
    --timeout 5m \
    --wait=false
}

install_promtail() {
  local NS="$1"
  local VALUES_FILE="$2"

  echo "Installing Promtail log collector..."
  helm repo add grafana https://grafana.github.io/helm-charts 2>/dev/null || true
  helm repo update grafana

  # Clean up stuck Helm release
  cleanup_stuck_helm_release "promtail" "${NS}"

  helm upgrade --install promtail grafana/promtail \
    --namespace "${NS}" \
    --version "6.16.6" \
    --values "${VALUES_FILE}" \
    --wait \
    --timeout 5m
}

verify_installations() {
  local NS="$1"

  echo ""
  echo "=========================================="
  echo "  POST-DEPLOYMENT HEALTH CHECKS"
  echo "=========================================="
  echo ""

  local ERRORS=0

  # Check all pods are running
  echo "📦 Checking pod status..."
  local NOT_RUNNING=""
  NOT_RUNNING=$(kubectl get pods -n "${NS}" --no-headers 2>/dev/null | grep -v "Running\|Completed" || echo "")
  if [ -n "$NOT_RUNNING" ]; then
    echo "❌ Some pods are not running:"
    echo "$NOT_RUNNING"
    ERRORS=$((ERRORS + 1))
  else
    echo "All pods are Running"
  fi
  echo ""

  # Check Prometheus is scraping targets
  echo "📊 Checking Prometheus targets..."
  local PROM_POD=""
  PROM_POD=$(kubectl get pods -n "${NS}" -l app.kubernetes.io/name=prometheus -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
  if [ -n "$PROM_POD" ]; then
    local TARGETS_UP=""
    TARGETS_UP=$(kubectl exec -n "${NS}" "${PROM_POD}" -c prometheus -- wget -qO- http://localhost:9090/api/v1/targets 2>/dev/null | grep -o '"health":"up"' | wc -l || echo "0")
    echo "  - Prometheus has ${TARGETS_UP} healthy targets"
    if [ "$TARGETS_UP" -lt 5 ]; then
      echo " Warning: Expected more targets to be up"
    fi
  else
    echo "❌ Prometheus pod not found"
    ERRORS=$((ERRORS + 1))
  fi
  echo ""

  # Check OTEL collector is running and has correct labels
  echo "🔭 Checking OpenTelemetry Collector..."
  local OTEL_POD=""
  OTEL_POD=$(kubectl get pods -n "${NS}" -l app.kubernetes.io/name=opentelemetry-collector -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
  if [ -n "$OTEL_POD" ]; then
    local OTEL_STATUS=""
    OTEL_STATUS=$(kubectl get pod -n "${NS}" "${OTEL_POD}" -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
    if [ "$OTEL_STATUS" = "Running" ]; then
      echo "OTEL Collector pod is running"
    else
      echo "❌ OTEL Collector pod status: ${OTEL_STATUS}"
      ERRORS=$((ERRORS + 1))
    fi
    
    # Check ServiceMonitor selector labels
    local SVC_LABELS=""
    SVC_LABELS=$(kubectl get svc -n "${NS}" -l app.kubernetes.io/name=opentelemetry-collector -o jsonpath='{.items[0].metadata.labels}' 2>/dev/null || echo "")
    echo "  - Service labels: ${SVC_LABELS}"
  else
    echo "❌ OTEL Collector pod not found"
    ERRORS=$((ERRORS + 1))
  fi
  echo ""

  # Check Grafana is accessible
  echo "📈 Checking Grafana..."
  local GRAFANA_POD=""
  GRAFANA_POD=$(kubectl get pods -n "${NS}" -l app.kubernetes.io/name=grafana -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
  if [ -n "$GRAFANA_POD" ]; then
    local GRAFANA_READY=""
    GRAFANA_READY=$(kubectl get pod -n "${NS}" "${GRAFANA_POD}" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || echo "")
    if [ "$GRAFANA_READY" = "True" ]; then
      echo "Grafana pod is ready"
    else
      echo "❌ Grafana pod not ready"
      ERRORS=$((ERRORS + 1))
    fi
  else
    echo "❌ Grafana pod not found"
    ERRORS=$((ERRORS + 1))
  fi
  echo ""

  # Check Loki is accepting logs
  echo "📝 Checking Loki..."
  local LOKI_POD=""
  LOKI_POD=$(kubectl get pods -n "${NS}" -l app.kubernetes.io/name=loki -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
  if [ -n "$LOKI_POD" ]; then
    local LOKI_READY=""
    LOKI_READY=$(kubectl get pod -n "${NS}" "${LOKI_POD}" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || echo "")
    if [ "$LOKI_READY" = "True" ]; then
      echo "Loki pod is ready"
    else
      echo "❌ Loki pod not ready"
      ERRORS=$((ERRORS + 1))
    fi
  else
    echo "❌ Loki pod not found"
    ERRORS=$((ERRORS + 1))
  fi
  echo ""

  # Check Tempo is running
  echo "🔍 Checking Tempo..."
  local TEMPO_POD=""
  TEMPO_POD=$(kubectl get pods -n "${NS}" -l app.kubernetes.io/name=tempo -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
  if [ -n "$TEMPO_POD" ]; then
    local TEMPO_READY=""
    TEMPO_READY=$(kubectl get pod -n "${NS}" "${TEMPO_POD}" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || echo "")
    if [ "$TEMPO_READY" = "True" ]; then
      echo "Tempo pod is ready"
    else
      echo "❌ Tempo pod not ready"
      ERRORS=$((ERRORS + 1))
    fi
  else
    echo "❌ Tempo pod not found"
    ERRORS=$((ERRORS + 1))
  fi
  echo ""

  # Check ServiceMonitors exist
  echo "🎯 Checking ServiceMonitors..."
  local SM_COUNT=""
  SM_COUNT=$(kubectl get servicemonitors -n "${NS}" --no-headers 2>/dev/null | wc -l || echo "0")
  echo "  - Found ${SM_COUNT} ServiceMonitors in ${NS}"
  
  local OTEL_SM=""
  OTEL_SM=$(kubectl get servicemonitor -n "${NS}" -l app.kubernetes.io/name=opentelemetry-collector -o name 2>/dev/null || echo "")
  if [ -n "$OTEL_SM" ]; then
    echo "OTEL ServiceMonitor exists"
  else
    echo " OTEL ServiceMonitor not found - Prometheus won't scrape OTEL metrics"
  fi
  echo ""

  # Check Grafana dashboards ConfigMap
  echo "📊 Checking Grafana dashboards..."
  local DASHBOARD_CMS=""
  DASHBOARD_CMS=$(kubectl get configmaps -n "${NS}" -l grafana_dashboard=1 --no-headers 2>/dev/null | wc -l || echo "0")
  echo "  - Found ${DASHBOARD_CMS} dashboard ConfigMaps"
  echo ""

  # Summary
  echo "=========================================="
  if [ "$ERRORS" -gt 0 ]; then
    echo "❌ HEALTH CHECK FAILED: ${ERRORS} errors found"
    echo "=========================================="
    echo ""
    echo "Troubleshooting tips:"
    echo "  - Check pod logs: kubectl logs -n ${NS} <pod-name>"
    echo "  - Describe pods: kubectl describe pod -n ${NS} <pod-name>"
    echo "  - Check events: kubectl get events -n ${NS} --sort-by='.lastTimestamp'"
  else
    echo "ALL HEALTH CHECKS PASSED"
    echo "=========================================="
  fi
  echo ""

  # Print resources for reference
  echo "📦 Pods:"
  kubectl get pods -n "${NS}"
  echo ""
  echo "🔗 Services:"
  kubectl get svc -n "${NS}"
  echo ""
  echo "🌐 Ingress:"
  kubectl get ingress -n "${NS}"
}

get_grafana_url() {
  local NS="$1"
  local HOSTNAME="$2"
  local INGRESS_ADDRESS=""

  INGRESS_ADDRESS=$(kubectl get ingress -n "${NS}" -l app.kubernetes.io/name=grafana -o jsonpath='{.items[0].status.loadBalancer.ingress[0].hostname}' 2>/dev/null || echo "")

  if [ -n "$INGRESS_ADDRESS" ]; then
    echo "https://${HOSTNAME}"
  else
    echo "Ingress not yet ready. Check: kubectl get ingress -n ${NS}"
  fi
}

get_acm_certificate_arn() {
  local DOMAIN="$1"
  local CERT_ARN=""
  local PLATFORM_DIR="${PROJECT_ROOT}/infra/envs/platform"

  if [ -n "${ACM_CERT_ARN:-}" ]; then
    echo "$ACM_CERT_ARN"
    return
  fi

  if command -v tofu >/dev/null 2>&1 && [ -d "$PLATFORM_DIR" ]; then
    CERT_ARN="$(cd "$PLATFORM_DIR" && tofu output -raw public_env_certificate_arn 2>/dev/null || true)"
  fi

  if [ -n "$CERT_ARN" ] && [ "$CERT_ARN" != "None" ] && [ "$CERT_ARN" != "null" ]; then
    echo "$CERT_ARN"
    return
  fi

  # Prefer the wildcard certificate used by public ALB services.
  CERT_ARN=$(aws acm list-certificates \
    --query "CertificateSummaryList[?DomainName=='*.${DOMAIN}.${PUBLIC_ROOT_DOMAIN}'].CertificateArn | [0]" \
    --output text \
    --region "${REGION}" 2>/dev/null)

  if [ -z "$CERT_ARN" ] || [ "$CERT_ARN" = "None" ] || [ "$CERT_ARN" = "null" ]; then
    # Fallback only to an exact observability cert. Do not select unrelated
    # docs/app certs for the same environment domain.
    CERT_ARN=$(aws acm list-certificates \
      --query "CertificateSummaryList[?DomainName=='observability.${DOMAIN}.${PUBLIC_ROOT_DOMAIN}'].CertificateArn | [0]" \
      --output text \
      --region "${REGION}" 2>/dev/null)
  fi

  echo "$CERT_ARN"
}

configure_kubectl "$CLUSTER_NAME" "$REGION"

VALUES_DIR="${PROJECT_ROOT}/infra/modules/addons/values"
PROMETHEUS_VALUES="${VALUES_DIR}/prometheus-stack-values.yaml"
TEMPO_VALUES="${VALUES_DIR}/tempo-values.yaml"
LOKI_VALUES="${VALUES_DIR}/loki-values.yaml"
OTEL_VALUES="${VALUES_DIR}/otel-collector-values.yaml"
PROMTAIL_VALUES="${VALUES_DIR}/promtail-values.yaml"

for f in "$PROMETHEUS_VALUES" "$TEMPO_VALUES" "$LOKI_VALUES" "$OTEL_VALUES" "$PROMTAIL_VALUES"; do
  if [ ! -f "$f" ]; then
    echo "Error: Values file not found: ${f}"
    exit 1
  fi
done

# Get ACM certificate for HTTPS
ACM_CERT_ARN=$(get_acm_certificate_arn "$ENVIRONMENT")
if [ -z "$ACM_CERT_ARN" ] || [ "$ACM_CERT_ARN" = "None" ] || [ "$ACM_CERT_ARN" = "null" ]; then
  echo "Warning: No ACM certificate found for *.${ENVIRONMENT}.${PUBLIC_ROOT_DOMAIN}. HTTPS may not work."
  ACM_CERT_ARN=""
else
  echo "Using ACM certificate: ${ACM_CERT_ARN}"
fi

create_namespace "$NAMESPACE"
create_grafana_secret "$NAMESPACE"
apply_grafana_dashboards "$NAMESPACE"
install_tempo "$NAMESPACE" "$TEMPO_VALUES"
install_loki "$NAMESPACE" "$LOKI_VALUES"
install_promtail "$NAMESPACE" "$PROMTAIL_VALUES"
install_prometheus_stack "$NAMESPACE" "$GRAFANA_HOSTNAME" "$PROMETHEUS_VALUES" "$ACM_CERT_ARN"
install_otel_collector "$NAMESPACE" "$OTEL_VALUES"
verify_installations "$NAMESPACE"
