#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
PLATFORM_DIR="${PROJECT_ROOT}/infra/envs/platform"
PLATFORM_CHART="${PROJECT_ROOT}/infra/helm/platform/helm-chart"

require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Error: required command not found: ${cmd}" >&2
    exit 1
  fi
}

service_name() {
  basename "$(dirname "$1")" | sed 's/_/-/g'
}

validate_shell() {
  echo "Validating shell scripts..."
  bash -n \
    "${SCRIPT_DIR}/k8s-deploy-infra.sh" \
    "${SCRIPT_DIR}/k8s-deploy-services.sh" \
    "${SCRIPT_DIR}/k8s-install-addons.sh" \
    "${SCRIPT_DIR}/k8s-run-migrations.sh" \
    "${SCRIPT_DIR}/k8s-deploy-gateway.sh" \
    "${SCRIPT_DIR}/k8s-create-kafka-topics.sh" \
    "${SCRIPT_DIR}/kafka-common.sh" \
    "${SCRIPT_DIR}/kafka-purge.sh"
}

validate_tofu() {
  echo "Validating Terraform/OpenTofu formatting..."
  tofu fmt -check -recursive "${PROJECT_ROOT}/infra"

  if [ -d "${PLATFORM_DIR}/.terraform" ]; then
    echo "Validating Terraform/OpenTofu configuration..."
    if ! (cd "${PLATFORM_DIR}" && tofu validate) >/tmp/bighill-tofu-validate.out 2>/tmp/bighill-tofu-validate.err; then
      if grep -q "Module not installed" /tmp/bighill-tofu-validate.err; then
        echo "Skipping tofu validate: Terraform modules are not installed. Run 'tofu init' in ${PLATFORM_DIR}."
      else
        cat /tmp/bighill-tofu-validate.out
        cat /tmp/bighill-tofu-validate.err >&2
        exit 1
      fi
    fi
  else
    echo "Skipping tofu validate: ${PLATFORM_DIR}/.terraform is not initialized."
  fi
}

validate_platform_chart() {
  echo "Validating platform Helm chart..."
  helm lint "${PLATFORM_CHART}" --set postgres.credentialsSecret.name=aurora-creds-test
  for env in staging prod; do
    local rendered
    rendered="/tmp/bighill-platform-${env}.yaml"
    helm template bighill-infra "${PLATFORM_CHART}" \
      -f "${PLATFORM_CHART}/values.yaml" \
      -f "${PLATFORM_CHART}/${env}-values.yaml" \
      --set postgres.dbnames='bighill_data_registry_db bighill_ingestion_db bighill_feature_materializer_db bighill_inference_db bighill_model_registry_db bighill_profile_db' \
      --set postgres.host=example.cluster.local \
      --set postgres.credentialsSecret.name=aurora-creds-test \
      >"$rendered"
    if rg -U -n 'name: (POSTGRES_PASSWORD|BIGHILL_DB_ADMIN_PASSWORD|BIGHILL_DB_PASSWORD)\n\s+value:' "$rendered" >/tmp/bighill-platform-plaintext-db-secrets.txt; then
      cat /tmp/bighill-platform-plaintext-db-secrets.txt >&2
      echo "Error: platform chart renders DB password env vars as plaintext values." >&2
      exit 1
    fi
  done
}

validate_service_charts() {
  echo "Validating service Helm charts..."
  local chart svc env values expected_tag
  for chart in "${PROJECT_ROOT}"/*_service/helm; do
    [ -d "$chart" ] || continue
    svc="$(service_name "$chart")"
    helm lint "$chart"
    for env in staging prod; do
      values="${chart}/${env}-values.yaml"
      if [ ! -f "$values" ]; then
        echo "Error: missing ${env} values file: ${values}" >&2
        exit 1
      fi
      expected_tag="${svc}-0.0.1-${env}"
      if ! grep -Eq 'repository:.*bighill/mlops"?$' "$values"; then
        echo "Error: ${values} must use the shared bighill/mlops ECR repository." >&2
        exit 1
      fi
      if ! grep -Eq "tag: \"?${expected_tag}\"?" "$values"; then
        echo "Error: ${values} must use service-encoded image tag ${expected_tag}." >&2
        exit 1
      fi
      helm template "$svc" "$chart" -f "${chart}/values.yaml" -f "$values" >/dev/null
    done
  done
}

validate_no_stale_copied_services() {
  echo "Checking for stale copied-service references..."
  if rg --glob '!validate-deploy.sh' -n 'event_service|event-service|comms_service|comms-service|price-oracle|PRICE_ORACLE|internal\.northern\.bighill|host\.docker\.internal|kafka\.bighill|redis\.bighill|postgres\.ml-ops' \
    "${PROJECT_ROOT}/infra/scripts" \
    "${PROJECT_ROOT}/infra/envs/platform" \
    "${PROJECT_ROOT}/infra/modules" \
    "${PROJECT_ROOT}/api_gateway/template.yml" \
    "${PROJECT_ROOT}"/*_service/helm \
    >/tmp/bighill-infra-stale-refs.txt; then
    cat /tmp/bighill-infra-stale-refs.txt >&2
    echo "Error: stale copied-service references found." >&2
    exit 1
  fi
}

require_cmd bash
require_cmd grep
require_cmd helm
require_cmd rg
require_cmd tofu

validate_shell
validate_tofu
validate_platform_chart
validate_service_charts
validate_no_stale_copied_services

echo "Deployment validation completed successfully."
