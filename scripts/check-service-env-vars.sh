#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

declare -a search_paths=(
  "ingestion_service"
  "data_registry_service"
  "data_stream_service"
  "feature_materializer_service"
  "inference_service"
  "model_registry_service"
  "model_serving_service"
  "tenant_service"
  "tool_execution_service"
  "tool_catalog_service"
  "training_service"
  "scripts"
  ".vscode/launch.json"
  "docker-compose-services.yml"
)

declare -a patterns=(
  "(?<![A-Z0-9_])INGESTION_(?!SERVICE_)"
  "(?<![A-Z0-9_])DATA_REGISTRY_(?!SERVICE_)"
  "(?<![A-Z0-9_])DATA_STREAM_(?!SERVICE_)"
  "(?<![A-Z0-9_])FEATURE_MATERIALIZER_(?!SERVICE_)"
  "(?<![A-Z0-9_])INFERENCE_(?!SERVICE_)"
  "(?<![A-Z0-9_])MODEL_REGISTRY_(?!SERVICE_)"
  "(?<![A-Z0-9_])MODEL_SERVING_(?!SERVICE_)"
  "(?<![A-Z0-9_])PROFILE_(?!SERVICE_)"
  "(?<![A-Z0-9_])TOOL_(?!EXECUTION_SERVICE_|CATALOG_SERVICE_|ERROR)"
  "(?<![A-Z0-9_])TRAINING_(?!SERVICE_|JOB_|JOBS_|OUTPUT_|AXOLOTL_|ARTIFACT_|ADAPTER_|BASE_|DATASET_|MODEL_|RECIPE_|RUN_|SERVING_|EVALUATION_|REPORT_|RAY_)"
)

failed=0
for pattern in "${patterns[@]}"; do
  if rg --hidden -n -P \
    -g '!**/target/**' \
    -g '!**/build/**' \
    -g '!**/test_results/**' \
    -g '!**/*.pb.go' \
    -g '!scripts/check-service-env-vars.sh' \
    "${pattern}" "${search_paths[@]}"; then
    failed=1
  fi
done

if [[ "${failed}" -ne 0 ]]; then
  echo "service-owned environment variables must include the _SERVICE segment" >&2
  exit 1
fi
