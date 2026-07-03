#!/usr/bin/env bash
set -euo pipefail

ENVIRONMENT="${1:-}"
ARCH="${2:-arm64}"

if [ -z "$ENVIRONMENT" ]; then
  echo "Usage: $0 <environment> [arch]"
  echo "  environment: staging|prod"
  echo "  arch: arm64|amd64 (default: arm64)"
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
REGION="${AWS_REGION:-us-east-1}"
PLATFORM_DIR="${PROJECT_ROOT}/infra/envs/platform"

resolve_bucket_name() {
  local OUTPUT_NAME="$1"
  local SUFFIX="$2"
  local BUCKET=""
  local ACCOUNT_ID=""

  if [ -n "${LAMBDA_ARTIFACTS_BUCKET_NAME:-}" ]; then
    echo "$LAMBDA_ARTIFACTS_BUCKET_NAME"
    return
  fi

  if command -v tofu >/dev/null 2>&1; then
    BUCKET="$(cd "$PLATFORM_DIR" && tofu output -raw "$OUTPUT_NAME" 2>/dev/null || true)"
  fi

  if [ -z "$BUCKET" ] && command -v terraform >/dev/null 2>&1; then
    BUCKET="$(cd "$PLATFORM_DIR" && terraform output -raw "$OUTPUT_NAME" 2>/dev/null || true)"
  fi

  if [ -n "$BUCKET" ] && [ "$BUCKET" != "null" ] && [ "$BUCKET" != "None" ]; then
    echo "$BUCKET"
    return
  fi

  ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text --region "$REGION" 2>/dev/null || true)"
  if [ -n "$ACCOUNT_ID" ] && [ "$ACCOUNT_ID" != "None" ]; then
    echo "ml-ops-${ENVIRONMENT}-${ACCOUNT_ID}-${SUFFIX}"
    return
  fi

  echo "Error: could not resolve ${OUTPUT_NAME}. Run 'aws sso login' or set LAMBDA_ARTIFACTS_BUCKET_NAME." >&2
  exit 1
}

S3_BUCKET="$(resolve_bucket_name "lambda_artifacts_bucket_name" "lambda-artifacts")"

echo "Deploying API Gateway for environment '$ENVIRONMENT' (arch: $ARCH)..."

build_gateway() {
  local ROOT="$1"
  local DIST_DIR="$ROOT/api_gateway/build/dist"

  if [ -f "$DIST_DIR/api.zip" ] && [ -f "$DIST_DIR/auth.zip" ]; then
    echo "Using prebuilt API Gateway artifacts from $DIST_DIR"
    return
  fi

  echo "Building API Gateway..."
  cd "$ROOT/api_gateway"
  make build
}

upload_lambda_artifacts() {
  local ROOT="$1"
  local BUCKET="$2"
  local REGION_ARG="$3"

  echo "Uploading Lambda artifacts to S3..."
  cd "$ROOT/api_gateway/build/dist"

  if [ ! -f "api.zip" ] || [ ! -f "auth.zip" ]; then
    echo "ERROR: Lambda zip files not found in $ROOT/api_gateway/build/dist"
    exit 1
  fi

  aws s3 cp api.zip "s3://${BUCKET}/lambda/api.zip" --region "$REGION_ARG"
  aws s3 cp auth.zip "s3://${BUCKET}/lambda/auth.zip" --region "$REGION_ARG"

  echo "Uploaded Lambda artifacts to s3://${BUCKET}/lambda/"
}

apply_terraform() {
  local ROOT="$1"
  local ENV_ARG="$2"
  local STACK_NAME="ml-ops-${ENV_ARG}-api-gateway"

  echo "Applying Terraform for API Gateway..."
  cd "$ROOT/infra/envs/platform"
  tofu init -reconfigure -backend-config="${ENV_ARG}-account.hcl"
  if ! tofu apply -var-file="${ENV_ARG}.tfvars" \
    -target=aws_s3_object.api_lambda_zip \
    -target=aws_s3_object.auth_lambda_zip \
    -target=module.api_gateway \
    -auto-approve; then
    echo "CloudFormation stack events for ${STACK_NAME}:"
    aws cloudformation describe-stack-events \
      --stack-name "$STACK_NAME" \
      --query 'StackEvents[?ResourceStatusReason!=`null`].[Timestamp,LogicalResourceId,ResourceType,ResourceStatus,ResourceStatusReason]' \
      --output table \
      --region "$REGION" || true
    return 1
  fi
}

update_lambda_code() {
  local ENV_ARG="$1"
  local REGION_ARG="$2"
  local BUCKET="$3"

  echo "Updating Lambda function code..."
  local STACK_NAME="ml-ops-${ENV_ARG}-api-gateway"

  local API_FUNCTION_NAME=$(aws cloudformation describe-stack-resources \
    --stack-name "$STACK_NAME" \
    --logical-resource-id BighillApiFunction \
    --query 'StackResources[0].PhysicalResourceId' \
    --output text \
    --region "$REGION_ARG")

  local AUTH_FUNCTION_NAME=$(aws cloudformation describe-stack-resources \
    --stack-name "$STACK_NAME" \
    --logical-resource-id BighillAuthFunction \
    --query 'StackResources[0].PhysicalResourceId' \
    --output text \
    --region "$REGION_ARG")

  if [ -n "$API_FUNCTION_NAME" ] && [ "$API_FUNCTION_NAME" != "None" ]; then
    echo "Updating API Lambda: $API_FUNCTION_NAME"
    aws lambda update-function-code \
      --function-name "$API_FUNCTION_NAME" \
      --s3-bucket "$BUCKET" \
      --s3-key lambda/api.zip \
      --region "$REGION_ARG" \
      --output text > /dev/null
  fi

  if [ -n "$AUTH_FUNCTION_NAME" ] && [ "$AUTH_FUNCTION_NAME" != "None" ]; then
    echo "Updating Auth Lambda: $AUTH_FUNCTION_NAME"
    aws lambda update-function-code \
      --function-name "$AUTH_FUNCTION_NAME" \
      --s3-bucket "$BUCKET" \
      --s3-key lambda/auth.zip \
      --region "$REGION_ARG" \
      --output text > /dev/null
  fi
}

if [ "${SKIP_GATEWAY_BUILD:-}" != "1" ] && [ "${SKIP_GATEWAY_BUILD:-}" != "true" ]; then
  build_gateway "$PROJECT_ROOT"
fi
upload_lambda_artifacts "$PROJECT_ROOT" "$S3_BUCKET" "$REGION"
apply_terraform "$PROJECT_ROOT" "$ENVIRONMENT"
update_lambda_code "$ENVIRONMENT" "$REGION" "$S3_BUCKET"

echo "API Gateway deployment complete."
