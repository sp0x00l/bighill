#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common.sh"
PROJECT_ROOT="$(get_project_root)"

MIGRATIONS_SERVICE_BUILD_VERSION="0.0.1"

usage() {
  echo "Usage: $0 [staging|prod|dev|local-dev|cicd] [amd64|arm64]"
}

ENVIRONMENT="${1:-}"
TARGETARCH="${2:-arm64}"
AWS_REGION="${AWS_REGION:-eu-west-1}"
ECR_REPO="${ECR_REPO:-bighill/mlops}"

if [ -z "$ENVIRONMENT" ]; then
  usage
  exit 1
fi

if [ -z "$TARGETARCH" ]; then
  usage
  exit 1
fi

if ! check_docker; then
  exit 1
fi

"${PROJECT_ROOT}/scripts/docker-db-migrations.sh" "$ENVIRONMENT"

echo "Building migrations:${MIGRATIONS_SERVICE_BUILD_VERSION} from local migration files"
docker buildx build --load --platform "linux/${TARGETARCH}" \
  --build-arg TARGETARCH="${TARGETARCH}" \
  --build-arg BUILD_VERSION_REQUIRED="${MIGRATIONS_SERVICE_BUILD_VERSION}" \
  --no-cache \
  -t "migrations:${MIGRATIONS_SERVICE_BUILD_VERSION}" \
  -f "${PROJECT_ROOT}/migrations.Dockerfile" \
  "${PROJECT_ROOT}"

AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID:-$(aws sts get-caller-identity --query Account --output text)}"
ECR_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPO}"
REMOTE_IMAGE="${ECR_URI}:migrations-${MIGRATIONS_SERVICE_BUILD_VERSION}-${ENVIRONMENT}"

echo "Logging in to ECR: ${ECR_URI}"
aws ecr get-login-password --region "${AWS_REGION}" \
  | docker login --username AWS --password-stdin "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"

if ! aws ecr describe-repositories \
    --repository-names "${ECR_REPO}" \
    --region "${AWS_REGION}" >/dev/null 2>&1; then
  echo "Creating ECR repository: ${ECR_REPO}"
  aws ecr create-repository \
    --repository-name "${ECR_REPO}" \
    --region "${AWS_REGION}" >/dev/null
fi

aws ecr put-image-tag-mutability \
  --repository-name "${ECR_REPO}" \
  --image-tag-mutability MUTABLE \
  --region "${AWS_REGION}"

echo "Pushing migrations:${MIGRATIONS_SERVICE_BUILD_VERSION} -> ${REMOTE_IMAGE}"
docker tag "migrations:${MIGRATIONS_SERVICE_BUILD_VERSION}" "${REMOTE_IMAGE}"
docker push "${REMOTE_IMAGE}"
