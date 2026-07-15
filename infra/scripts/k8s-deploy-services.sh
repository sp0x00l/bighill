#! /usr/bin/env bash
set -euo pipefail

ENVIRONMENT="${1:-}"
REGION="${AWS_REGION:-eu-west-1}"

if [ -z "$ENVIRONMENT" ]; then
  echo "Usage: $0 [staging|prod]"
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
NAMESPACE="ml-ops-${ENVIRONMENT}"
CLUSTER_NAME="bighill-${ENVIRONMENT}"
COMMON_SCRIPT="${PROJECT_ROOT}/scripts/common.sh"
INTERNAL_ROOT_DOMAIN="${INTERNAL_ROOT_DOMAIN:-internal.bighill.example}"

# shellcheck disable=SC1090
. "${COMMON_SCRIPT}"

get_target_services() {
  if [ "${DEPLOY_SERVICE_DIRS:-}" = "none" ]; then
    echo ""
    return 0
  fi

  if [ -n "${DEPLOY_SERVICE_DIRS:-}" ]; then
    echo "${DEPLOY_SERVICE_DIRS}" | tr ',' ' '
    return 0
  fi

  get_services_list "${PROJECT_ROOT}"
}

# Get ACM certificate ARN for the environment domain
get_acm_certificate_arn() {
  local DOMAIN="$1"
  local CERT_ARN=""
  local PLATFORM_DIR="${PROJECT_ROOT}/infra/envs/platform"

  if [ -n "${ACM_CERT_ARN:-}" ]; then
    echo "${ACM_CERT_ARN}"
    return
  fi

  if command -v tofu >/dev/null 2>&1; then
    CERT_ARN="$(cd "${PLATFORM_DIR}" && tofu output -raw internal_certificate_arn 2>/dev/null || true)"
  fi

  if [ -n "${CERT_ARN}" ] && [ "${CERT_ARN}" != "None" ] && [ "${CERT_ARN}" != "null" ]; then
    echo "${CERT_ARN}"
    return
  fi

  # Prefer the wildcard certificate for internal service ALBs.
  CERT_ARN=$(aws acm list-certificates \
    --query "CertificateSummaryList[?DomainName=='*.${INTERNAL_ROOT_DOMAIN}'].CertificateArn | [0]" \
    --output text \
    --region "${REGION}" 2>/dev/null || echo "")

  if [ "${CERT_ARN}" = "None" ] || [ "${CERT_ARN}" = "null" ]; then
    CERT_ARN=""
  fi

  echo "${CERT_ARN}"
}

# Get JWT signing key ID from Terraform output or AWS directly
get_jwt_signing_key_id() {
  # First try Terraform output (works locally)
  cd "$PROJECT_ROOT/infra/envs/platform"
  local KEY_ID=""
  KEY_ID=$(tofu output -raw jwt_signing_key_id 2>/dev/null || echo "")
  
  if [ -n "$KEY_ID" ] && [ "$KEY_ID" != "" ]; then
    echo "$KEY_ID"
    return
  fi
  
  # Fallback: query AWS KMS directly by alias
  KEY_ID=$(aws kms describe-key \
    --key-id "alias/bighill-${ENVIRONMENT}-jwt-signing" \
    --query 'KeyMetadata.KeyId' \
    --region "${REGION}" \
    --output text 2>/dev/null || echo "")
  
  echo "$KEY_ID"
}

# Get tenant-service IAM role ARN for IRSA
get_tenant_service_role_arn() {
  # First try Terraform output (works locally)
  cd "$PROJECT_ROOT/infra/envs/platform"
  local ROLE_ARN=""
  ROLE_ARN=$(tofu output -raw tenant_service_role_arn 2>/dev/null || echo "")
  
  if [ -n "$ROLE_ARN" ] && [ "$ROLE_ARN" != "" ]; then
    echo "$ROLE_ARN"
    return
  fi
  
  # Fallback: query AWS IAM directly
  ROLE_ARN=$(aws iam get-role \
    --role-name "bighill-${ENVIRONMENT}-tenant-service" \
    --query 'Role.Arn' \
    --region "${REGION}" \
    --output text 2>/dev/null || echo "")
  
  echo "$ROLE_ARN"
}

get_object_store_role_arn() {
  local SERVICE_ACCOUNT="$1"
  local PLATFORM_DIR="$PROJECT_ROOT/infra/envs/platform"
  local ROLE_ARN=""

  if command -v tofu >/dev/null 2>&1; then
    ROLE_ARN="$(cd "$PLATFORM_DIR" && tofu output -json object_store_service_role_arns 2>/dev/null | jq -r --arg name "$SERVICE_ACCOUNT" '.[$name] // empty' 2>/dev/null || true)"
  fi

  if [ -n "$ROLE_ARN" ] && [ "$ROLE_ARN" != "null" ] && [ "$ROLE_ARN" != "None" ]; then
    echo "$ROLE_ARN"
    return
  fi

  ROLE_ARN=$(aws iam get-role \
    --role-name "bighill-${ENVIRONMENT}-${SERVICE_ACCOUNT}" \
    --query 'Role.Arn' \
    --region "${REGION}" \
    --output text 2>/dev/null || echo "")

  if [ "$ROLE_ARN" = "None" ] || [ "$ROLE_ARN" = "null" ]; then
    ROLE_ARN=""
  fi

  echo "$ROLE_ARN"
}

object_store_extra_args() {
  local SERVICE_ACCOUNT="$1"
  local ROLE_ARN
  ROLE_ARN="$(get_object_store_role_arn "$SERVICE_ACCOUNT")"

  if [ -z "$ROLE_ARN" ]; then
    return 0
  fi

  echo "--set serviceAccount.create=true --set serviceAccount.annotations.eks\\.amazonaws\\.com/role-arn=${ROLE_ARN}"
}

ensure_eks_context() {
  echo "Updating kubeconfig for cluster ${CLUSTER_NAME} in region ${REGION}..."
  
  if [ -n "${AWS_PROFILE:-}" ]; then
    aws eks update-kubeconfig --name "${CLUSTER_NAME}" --region "${REGION}" --profile "${AWS_PROFILE}"
  else
    aws eks update-kubeconfig --name "${CLUSTER_NAME}" --region "${REGION}"
  fi

  if ! kubectl cluster-info >/dev/null 2>&1; then
    echo "Error: Cannot connect to Kubernetes cluster ${CLUSTER_NAME}"
    exit 1
  fi
}

install_service() {
  SERVICE_NAME="$1"
  SERVICE_BUILD_VERSION="$2"
  EXTRA_ARGS="${3:-}"

  if [ -f "$PROJECT_ROOT/$SERVICE_NAME/scripts/config.sh" ]; then
    . "$PROJECT_ROOT/$SERVICE_NAME/scripts/config.sh" "$ENVIRONMENT"
  fi

  K8S_SERVICE_NAME=$(echo "${SERVICE_NAME}" | sed 's/_/-/g')."${SERVICE_BUILD_VERSION}"
  VALUES_BASE="$PROJECT_ROOT/${SERVICE_NAME}/helm/values.yaml"
  VALUES_ENV="$PROJECT_ROOT/${SERVICE_NAME}/helm/${ENVIRONMENT}-values.yaml"

  echo "Deploying ${SERVICE_NAME} (build ${SERVICE_BUILD_VERSION}) to namespace '${NAMESPACE}'"

  if [ ! -f "$VALUES_ENV" ]; then
    echo "Error: Values file not found: $VALUES_ENV"
    exit 1
  fi

  # Use upgrade --install to avoid destroying and recreating ALBs
  # shellcheck disable=SC2086
  helm upgrade --install "${K8S_SERVICE_NAME}" "$PROJECT_ROOT/${SERVICE_NAME}/helm" --debug \
    --namespace "${NAMESPACE}" \
    --create-namespace \
    -f "$VALUES_BASE" \
    -f "$VALUES_ENV" \
    $EXTRA_ARGS
}

preserve_targeted_replica_count_args() {
  local SERVICE_DIR="$1"
  local DEPLOYMENT_NAME
  local CURRENT_REPLICAS

  if [ -z "${DEPLOY_SERVICE_DIRS:-}" ] || [ "${DEPLOY_SERVICE_DIRS:-}" = "none" ]; then
    return 0
  fi

  DEPLOYMENT_NAME="$(service_dir_to_name "${SERVICE_DIR}")"
  CURRENT_REPLICAS="$(kubectl get deployment "${DEPLOYMENT_NAME}" -n "${NAMESPACE}" -o jsonpath='{.spec.replicas}' 2>/dev/null || true)"
  if [ -n "${CURRENT_REPLICAS}" ]; then
    echo "--set replicaCount=${CURRENT_REPLICAS}"
  fi
}

ensure_namespace() {
  kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -
}

apply_service_policies() {
  local SERVICES
  SERVICES="$(get_target_services)"
  if [ -z "${SERVICES}" ]; then
    echo "No services selected for policy application."
    return 0
  fi

  for SERVICE_DIR in ${SERVICES}; do
    local POLICY_SCRIPT="${PROJECT_ROOT}/${SERVICE_DIR}/scripts/policy.sh"
    if [ -f "${POLICY_SCRIPT}" ]; then
      echo "Applying policy for ${SERVICE_DIR}..."
      "${POLICY_SCRIPT}" "${ENVIRONMENT}" "${NAMESPACE}"
    fi
  done
}


# ALB controller can get stuck trying to delete security groups
cleanup_ingresses() {
  echo "Checking for stuck ingresses..."
  
  local SERVICES
  SERVICES="$(get_target_services)"
  if [ -z "${SERVICES}" ]; then
    echo "No services selected for ingress cleanup."
    return 0
  fi

  for SERVICE_DIR in ${SERVICES}; do
    local SERVICE=$(service_dir_to_name "${SERVICE_DIR}")
    local INGRESS_NAME="${SERVICE}-ingress"
    
    # Check if ingress exists
    if ! kubectl get ingress "$INGRESS_NAME" -n "$NAMESPACE" >/dev/null 2>&1; then
      continue
    fi
    
    # Check ALB controller logs for errors with this ingress
    local ALB_ERROR=$(kubectl logs -n kube-system -l app.kubernetes.io/name=aws-load-balancer-controller --tail=50 2>/dev/null | grep -E "\"name\":\"${INGRESS_NAME}\".*error" | tail -1 || true)
    
    if echo "$ALB_ERROR" | grep -q "failed to delete securityGroup"; then
      echo "Found stuck ingress: $INGRESS_NAME - cleaning up..."
      
      # Remove finalizers to allow deletion
      kubectl patch ingress "$INGRESS_NAME" -n "$NAMESPACE" \
        -p '{"metadata":{"finalizers":[]}}' --type=merge 2>/dev/null || true
      
      # Force delete
      kubectl delete ingress "$INGRESS_NAME" -n "$NAMESPACE" \
        --force --grace-period=0 2>/dev/null || true
      
      echo "Cleaned up stuck ingress: $INGRESS_NAME"
      
      # Give ALB controller time to process
      sleep 10
    fi
  done
  
  # Restart ALB controller if we cleaned up any stuck ingresses
  if kubectl logs -n kube-system -l app.kubernetes.io/name=aws-load-balancer-controller --tail=20 2>/dev/null | grep -q "failed to delete securityGroup"; then
    echo "Restarting ALB controller to clear stuck state..."
    kubectl rollout restart deployment -n kube-system -l app.kubernetes.io/name=aws-load-balancer-controller 2>/dev/null || true
    sleep 15
  fi
}

# Wait for infrastructure dependencies (Kafka, Redis) before deploying services
wait_for_infra_ready() {
  echo "Verifying infrastructure is ready..."
  
  # Check Kafka
  local RETRIES=15
  while [ $RETRIES -gt 0 ]; do
    local KAFKA_READY=$(kubectl get statefulset kafka -n "$NAMESPACE" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "0")
    local KAFKA_ENDPOINTS=$(kubectl get endpoints kafka -n "$NAMESPACE" -o jsonpath='{.subsets[0].addresses[0].ip}' 2>/dev/null || echo "")
    
    if [ "$KAFKA_READY" = "1" ] && [ -n "$KAFKA_ENDPOINTS" ]; then
      echo "Kafka is ready."
      break
    fi
    
    RETRIES=$((RETRIES - 1))
    echo "Waiting for Kafka to be ready... ($RETRIES retries left)"
    sleep 5
  done
  
  # Check Redis
  RETRIES=15
  while [ $RETRIES -gt 0 ]; do
    local REDIS_READY=$(kubectl get deployment redis -n "$NAMESPACE" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "0")
    local REDIS_ENDPOINTS=$(kubectl get endpoints redis -n "$NAMESPACE" -o jsonpath='{.subsets[0].addresses[0].ip}' 2>/dev/null || echo "")
    
    if [ "$REDIS_READY" = "1" ] && [ -n "$REDIS_ENDPOINTS" ]; then
      echo "Redis is ready."
      return 0
    fi
    
    RETRIES=$((RETRIES - 1))
    echo "Waiting for Redis to be ready... ($RETRIES retries left)"
    sleep 5
  done
  
  echo "WARNING: Infrastructure may not be fully ready, but continuing..."
}

build_service_extra_args() {
  local KMS_EXTRA_ARGS=""
  if [ -n "$JWT_SIGNING_KEY_ID" ]; then
    echo "Using JWT signing key: ${JWT_SIGNING_KEY_ID}"
    KMS_EXTRA_ARGS="--set env.authKmsKeyId=${JWT_SIGNING_KEY_ID}"
  else
    echo "Warning: JWT signing key not found, services will use local KMS"
  fi

  TENANT_EXTRA_ARGS="${KMS_EXTRA_ARGS}"
  if [ -n "$TENANT_SERVICE_ROLE_ARN" ]; then
    echo "Using tenant-service IAM role: ${TENANT_SERVICE_ROLE_ARN}"
    TENANT_EXTRA_ARGS="${TENANT_EXTRA_ARGS} --set serviceAccount.create=true --set serviceAccount.annotations.eks\\.amazonaws\\.com/role-arn=${TENANT_SERVICE_ROLE_ARN}"
  else
    echo "Warning: Tenant service IAM role not found, IRSA disabled"
  fi

  INGESTION_EXTRA_ARGS="${KMS_EXTRA_ARGS}"
  INGESTION_EXTRA_ARGS="${INGESTION_EXTRA_ARGS} --set env.huggingFaceDownloadMode=kubernetes"
  INGESTION_EXTRA_ARGS="${INGESTION_EXTRA_ARGS} --set env.huggingFaceJobNamespace=${NAMESPACE}"
  INGESTION_EXTRA_ARGS="${INGESTION_EXTRA_ARGS} --set env.huggingFaceJobImage=${INGESTION_SERVICE_HUGGINGFACE_JOB_IMAGE:-training-jobs:0.0.1}"
}

append_service_extra_args() {
  local SERVICE_DIR="$1"
  local EXTRA_ARGS="${2:-}"
  local SERVICE_ACCOUNT
  SERVICE_ACCOUNT="$(service_dir_to_name "${SERVICE_DIR}")"

  case "${SERVICE_DIR}" in
    ingestion_service|data_stream_service|feature_materializer_service|inference_service|model_registry_service|training_service)
      local IRSA_ARGS
      IRSA_ARGS="$(object_store_extra_args "$SERVICE_ACCOUNT")"
      if [ -n "$IRSA_ARGS" ]; then
        EXTRA_ARGS="${EXTRA_ARGS} ${IRSA_ARGS}"
      fi
      ;;
  esac

  if [ "${SERVICE_DIR}" = "training_service" ]; then
    EXTRA_ARGS="${EXTRA_ARGS} --set env.kubeRayNamespace=${NAMESPACE}"
    EXTRA_ARGS="${EXTRA_ARGS} --set rayJobServiceAccount.create=true"
    EXTRA_ARGS="${EXTRA_ARGS} --set rayJobServiceAccount.name=training-jobs"
    local TRAINING_JOB_ROLE_ARN
    TRAINING_JOB_ROLE_ARN="$(get_object_store_role_arn "training-service")"
    if [ -n "$TRAINING_JOB_ROLE_ARN" ]; then
      EXTRA_ARGS="${EXTRA_ARGS} --set rayJobServiceAccount.annotations.eks\\.amazonaws\\.com/role-arn=${TRAINING_JOB_ROLE_ARN}"
    fi
  fi

  if [ -n "$ACM_CERT_ARN" ]; then
    case "${SERVICE_DIR}" in
      data_registry_service|ingestion_service|tenant_service|model_registry_service|training_service|inference_service)
        EXTRA_ARGS="${EXTRA_ARGS} --set ingress.certificateArn=${ACM_CERT_ARN}"
        ;;
    esac
  fi

  echo "$EXTRA_ARGS"
}

deploy_services() {
  local SERVICE_DIRS
  SERVICE_DIRS="$(get_target_services)"
  if [ -z "${SERVICE_DIRS}" ]; then
    echo "No services selected for deployment."
    return 0
  fi
  for SERVICE_DIR in ${SERVICE_DIRS}; do
    source_service_config "${SERVICE_DIR}" "${ENVIRONMENT}" "${PROJECT_ROOT}"
    local SERVICE_PREFIX="$(service_dir_to_env_prefix "${SERVICE_DIR}")"
    local VERSION_VAR="${SERVICE_PREFIX}_BUILD_VERSION"
    local SERVICE_VERSION="${!VERSION_VAR:-0.0.1}"
    local EXTRA_ARGS=""

    if [ "${SERVICE_DIR}" = "tenant_service" ]; then
      EXTRA_ARGS="${TENANT_EXTRA_ARGS}"
    elif [ "${SERVICE_DIR}" = "ingestion_service" ]; then
      EXTRA_ARGS="${INGESTION_EXTRA_ARGS}"
    fi

    EXTRA_ARGS="$(append_service_extra_args "${SERVICE_DIR}" "${EXTRA_ARGS}")"

    local REPLICA_ARGS
    REPLICA_ARGS="$(preserve_targeted_replica_count_args "${SERVICE_DIR}")"
    if [ -n "${REPLICA_ARGS}" ]; then
      echo "Preserving current replica count for $(service_dir_to_name "${SERVICE_DIR}"): ${REPLICA_ARGS#--set replicaCount=}"
      EXTRA_ARGS="${EXTRA_ARGS} ${REPLICA_ARGS}"
    fi

    install_service "${SERVICE_DIR}" "${SERVICE_VERSION}" "${EXTRA_ARGS}"
  done
}

wait_for_rollouts() {
  local SERVICE_DIRS
  SERVICE_DIRS="$(get_target_services)"
  if [ -z "${SERVICE_DIRS}" ]; then
    echo "No services selected for rollout checks."
    return 0
  fi

  for SERVICE_DIR in ${SERVICE_DIRS}; do
    local SERVICE
    SERVICE="$(service_dir_to_name "${SERVICE_DIR}")"
    local DESIRED_REPLICAS
    DESIRED_REPLICAS="$(kubectl get deployment "${SERVICE}" -n "${NAMESPACE}" -o jsonpath='{.spec.replicas}' 2>/dev/null || true)"
    if [ "${DESIRED_REPLICAS}" = "0" ]; then
      echo "Skipping rollout wait for ${SERVICE}: deployment is scaled to 0."
      continue
    fi
    echo "Waiting for rollout: ${SERVICE}"
    kubectl rollout status deployment "${SERVICE}" -n "${NAMESPACE}" --timeout=10m

    # rollout status is authoritative; strict pod image string matching is brittle
    # (e.g., digest/tag normalization or terminating old pods).
    local EXPECTED_IMAGE
    local READY_PODS
    EXPECTED_IMAGE="$(kubectl get deployment "${SERVICE}" -n "${NAMESPACE}" -o jsonpath='{.spec.template.spec.containers[0].image}' 2>/dev/null || true)"
    READY_PODS="$(kubectl get pods -n "${NAMESPACE}" -l "app=${SERVICE}" --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l | tr -d ' ')"
    if [ -n "${EXPECTED_IMAGE}" ]; then
      echo "Rollout ready for ${SERVICE} (deployment image: ${EXPECTED_IMAGE}, running pods: ${READY_PODS})"
    else
      echo "Rollout ready for ${SERVICE} (running pods: ${READY_PODS})"
    fi
  done
}

# Services that are internal-only and do not expose gateway-routed HTTP ingress.
INTERNAL_ONLY_SERVICES="data-stream-service feature-materializer-service model-serving-service tool-service"

service_needs_alb() {
  local SERVICE="$1"
  for INTERNAL in ${INTERNAL_ONLY_SERVICES}; do
    if [ "${SERVICE}" = "${INTERNAL}" ]; then
      return 1
    fi
  done
  return 0
}

# Wait for ALBs to be provisioned (ingress resources create ALBs)
wait_for_albs() {
  echo "Waiting for service ALBs to be provisioned..."
  local RETRIES=60
  while [ $RETRIES -gt 0 ]; do
    local SERVICES
    SERVICES="$(get_target_services)"
    if [ -z "${SERVICES}" ]; then
      echo "No services selected for ALB checks."
      return 0
    fi
    
    # Check if any targeted service actually needs an ALB
    local NEEDS_ALB="false"
    for SERVICE_DIR in ${SERVICES}; do
      local SERVICE=$(service_dir_to_name "${SERVICE_DIR}")
      if service_needs_alb "${SERVICE}"; then
        NEEDS_ALB="true"
        break
      fi
    done
    if [ "${NEEDS_ALB}" = "false" ]; then
      echo "No targeted services require ALBs, skipping ALB check."
      return 0
    fi
    
    for SERVICE_DIR in ${SERVICES}; do
      local SERVICE=$(service_dir_to_name "${SERVICE_DIR}")
      if ! service_needs_alb "${SERVICE}"; then
        continue
      fi
      local INGRESS_HOST=$(kubectl get ingress -n "$NAMESPACE" -l app="${SERVICE}" -o jsonpath='{.items[0].status.loadBalancer.ingress[0].hostname}' 2>/dev/null || echo "")
      if [ -n "$INGRESS_HOST" ]; then
        echo "ALBs are ready. ${SERVICE} ALB: ${INGRESS_HOST}"
        return 0
      fi
    done
    
    # Check for ALB controller errors that might block provisioning
    local ALB_ERRORS=$(kubectl logs -n kube-system -l app.kubernetes.io/name=aws-load-balancer-controller --tail=10 2>/dev/null | grep "error" | wc -l | tr -d ' ')
    ALB_ERRORS=${ALB_ERRORS:-0}
    if [ "$ALB_ERRORS" -gt 5 ] 2>/dev/null; then
      echo "WARNING: ALB controller has errors, check logs"
      kubectl logs -n kube-system -l app.kubernetes.io/name=aws-load-balancer-controller --tail=5 2>/dev/null | grep "error" || true
    fi
    
    RETRIES=$((RETRIES - 1))
    if [ $((RETRIES % 10)) -eq 0 ]; then
      echo "Waiting for ALBs to be provisioned... ($RETRIES retries left)"
    fi
    sleep 5
  done
  
  echo "WARNING: ALBs may not be fully provisioned yet. Check ingress status."
}

# Verify service ALBs are resolvable
verify_service_albs() {
  echo "Verifying service ALBs are accessible..."
  
  local SERVICES
  SERVICES="$(get_target_services)"
  if [ -z "${SERVICES}" ]; then
    echo "No services selected for ALB verification."
    return 0
  fi
  
  # Check if any targeted service actually needs an ALB
  local NEEDS_ALB="false"
  for SERVICE_DIR in ${SERVICES}; do
    local SERVICE=$(service_dir_to_name "${SERVICE_DIR}")
    if service_needs_alb "${SERVICE}"; then
      NEEDS_ALB="true"
      break
    fi
  done
  if [ "${NEEDS_ALB}" = "false" ]; then
    echo "No targeted services require ALB verification, skipping."
    return 0
  fi
  
  local FOUND_ALB="false"
  for SERVICE_DIR in ${SERVICES}; do
    local SERVICE=$(service_dir_to_name "${SERVICE_DIR}")
    if ! service_needs_alb "${SERVICE}"; then
      continue
    fi
    local INGRESS_HOST=$(kubectl get ingress "${SERVICE}-ingress" -n "$NAMESPACE" -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null || echo "")
    if [ -n "$INGRESS_HOST" ]; then
      echo "Service ALB: ${SERVICE} -> ${INGRESS_HOST}"
      FOUND_ALB="true"
    fi
  done

  if [ "${FOUND_ALB}" != "true" ]; then
    echo "WARNING: no service ingresses have ALB hostnames yet"
    return 1
  fi
  
  # Check if the ALB resolves via DNS
  local RETRIES=10
  while [ $RETRIES -gt 0 ]; do
    local RESOLVED="true"
    for SERVICE_DIR in ${SERVICES}; do
      local SERVICE=$(service_dir_to_name "${SERVICE_DIR}")
      if ! service_needs_alb "${SERVICE}"; then
        continue
      fi
      local INGRESS_HOST=$(kubectl get ingress "${SERVICE}-ingress" -n "$NAMESPACE" -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null || echo "")
      if [ -n "$INGRESS_HOST" ]; then
        if ! nslookup "$INGRESS_HOST" >/dev/null 2>&1; then
          RESOLVED="false"
        fi
      fi
    done
    if [ "${RESOLVED}" = "true" ]; then
      echo "Service ALBs resolve successfully"
      return 0
    fi
    
    RETRIES=$((RETRIES - 1))
    echo "Waiting for service ALB DNS... ($RETRIES retries left)"
    sleep 5
  done
  
  echo "WARNING: Service ALBs may not be resolvable yet"
  return 0
}

# Restart external-dns to force DNS record sync
restart_external_dns() {
  echo "Restarting external-dns to force DNS record sync..."
  if kubectl get deployment -n kube-system -l app.kubernetes.io/name=external-dns >/dev/null 2>&1; then
    kubectl rollout restart deployment -n kube-system -l app.kubernetes.io/name=external-dns 2>/dev/null || true
    sleep 5
  elif kubectl get deployment -n "$NAMESPACE" -l app.kubernetes.io/name=external-dns >/dev/null 2>&1; then
    kubectl rollout restart deployment -n "$NAMESPACE" -l app.kubernetes.io/name=external-dns 2>/dev/null || true
    sleep 5
  fi
  echo "External-dns restarted."
}

wait_for_dns_sync() {
  echo "Waiting for external-dns to sync DNS records..."
  local RETRIES=6
  
  # Check if external-dns pod is running
  if ! kubectl get pods -n kube-system -l app.kubernetes.io/name=external-dns --no-headers 2>/dev/null | grep -q Running; then
    echo "WARNING: external-dns pod not running"
    return 0
  fi
  
  # Wait for external-dns to process changes (check logs for sync completion)
  while [ $RETRIES -gt 0 ]; do
    local SYNC_STATUS=$(kubectl logs -n kube-system -l app.kubernetes.io/name=external-dns --tail=5 2>/dev/null | grep -E "(All records are already up to date|Change batch.*was submitted)" | tail -1 || true)
    
    if [ -n "$SYNC_STATUS" ]; then
      echo "External-dns sync status: $SYNC_STATUS"
      echo "DNS records synced."
      return 0
    fi
    
    # Check for errors
    local SYNC_ERROR=$(kubectl logs -n kube-system -l app.kubernetes.io/name=external-dns --tail=5 2>/dev/null | grep -i "error" | tail -1 || true)
    
    if [ -n "$SYNC_ERROR" ]; then
      echo "WARNING: External-dns error: $SYNC_ERROR"
      return 0
    fi
    
    RETRIES=$((RETRIES - 1))
    echo "Waiting for external-dns sync... ($RETRIES retries left)"
    sleep 10
  done
  
  echo "External-dns sync wait completed."
  return 0
}

refresh_lambda_environments() {
  echo "Refreshing Lambda execution environments to pick up DNS changes..."
  
  local STACK_NAME="bighill-${ENVIRONMENT}-api-gateway"
  
  # Get Lambda function names from CloudFormation
  local API_FUNCTION_NAME=""
  local AUTH_FUNCTION_NAME=""
  
  API_FUNCTION_NAME=$(aws cloudformation describe-stack-resources \
    --stack-name "$STACK_NAME" \
    --logical-resource-id BighillApiFunction \
    --query 'StackResources[0].PhysicalResourceId' \
    --output text \
    --region "$REGION" 2>/dev/null || echo "")
  
  AUTH_FUNCTION_NAME=$(aws cloudformation describe-stack-resources \
    --stack-name "$STACK_NAME" \
    --logical-resource-id BighillAuthFunction \
    --query 'StackResources[0].PhysicalResourceId' \
    --output text \
    --region "$REGION" 2>/dev/null || echo "")
  
  # Touch each Lambda to force new execution environment with fresh DNS cache
  if [ -n "$API_FUNCTION_NAME" ] && [ "$API_FUNCTION_NAME" != "None" ]; then
    echo "Refreshing API Lambda: $API_FUNCTION_NAME"
    aws lambda update-function-configuration \
      --function-name "$API_FUNCTION_NAME" \
      --description "Refreshed $(date -u +%Y-%m-%dT%H:%M:%SZ)" \
      --region "$REGION" \
      --output text >/dev/null 2>&1 || echo "WARNING: Failed to refresh API Lambda"
  fi
  
  if [ -n "$AUTH_FUNCTION_NAME" ] && [ "$AUTH_FUNCTION_NAME" != "None" ]; then
    echo "Refreshing Auth Lambda: $AUTH_FUNCTION_NAME"
    aws lambda update-function-configuration \
      --function-name "$AUTH_FUNCTION_NAME" \
      --description "Refreshed $(date -u +%Y-%m-%dT%H:%M:%SZ)" \
      --region "$REGION" \
      --output text >/dev/null 2>&1 || echo "WARNING: Failed to refresh Auth Lambda"
  fi
  
  # Wait for Lambda updates to complete
  echo "Waiting for Lambda updates to complete..."
  if [ -n "$API_FUNCTION_NAME" ] && [ "$API_FUNCTION_NAME" != "None" ]; then
    aws lambda wait function-updated \
      --function-name "$API_FUNCTION_NAME" \
      --region "$REGION" 2>/dev/null || true
  fi
  if [ -n "$AUTH_FUNCTION_NAME" ] && [ "$AUTH_FUNCTION_NAME" != "None" ]; then
    aws lambda wait function-updated \
      --function-name "$AUTH_FUNCTION_NAME" \
      --region "$REGION" 2>/dev/null || true
  fi
  
  echo "Lambda environments refreshed."
}

echo "Environment: ${ENVIRONMENT} (AWS EKS)"
ensure_eks_context

echo ""
echo "Deploying to cluster nodes size"
kubectl get nodes -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.metadata.labels.node\.kubernetes\.io/instance-type}{"\n"}{end}'
echo ""

ensure_namespace
apply_service_policies
cleanup_ingresses
wait_for_infra_ready

JWT_SIGNING_KEY_ID=$(get_jwt_signing_key_id)
TENANT_SERVICE_ROLE_ARN=$(get_tenant_service_role_arn)

echo "Fetching ACM certificate ARN..."
ACM_CERT_ARN=$(get_acm_certificate_arn "$ENVIRONMENT")
if [ -n "$ACM_CERT_ARN" ] && [ "$ACM_CERT_ARN" != "None" ] && [ "$ACM_CERT_ARN" != "null" ]; then
  echo "Using ACM certificate: ${ACM_CERT_ARN}"
else
  echo "Warning: No ACM certificate found for *.${INTERNAL_ROOT_DOMAIN}; service ALBs will stay HTTP-only."
  ACM_CERT_ARN=""
fi

build_service_extra_args
deploy_services
wait_for_rollouts
wait_for_albs
verify_service_albs
echo "Service deployment complete."

restart_external_dns
wait_for_dns_sync
refresh_lambda_environments
echo "All services deployed and DNS synced."
