#! /usr/bin/env bash

set -euo pipefail

ARCH="${1:-arm64}"
BUILD_TYPE="${2:-Debug}"
LIBC="${3:-musl}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
PDF_EXTRACTOR_LIB_DIR="${PROJECT_ROOT}/pdf_extractor_lib"
OUTPUT_DIR="${PDF_EXTRACTOR_LIB_DIR}/cpp/build/linux/${LIBC}/${ARCH}"

case "$LIBC" in
  musl)
    DOCKERFILE="pdf_extractor_lib/Dockerfile.build.musl"
    ;;
  glibc)
    DOCKERFILE="pdf_extractor_lib/Dockerfile.build.glibc"
    ;;
  *)
    echo "unsupported libc: $LIBC"
    exit 1
    ;;
esac

IMAGE_TAG="pdf-extractor-lib-builder:${LIBC}-${ARCH}"
PLATFORM="linux/${ARCH}"
if [ "${ARCH}" = "amd64" ]; then
  PLATFORM="linux/amd64"
fi

rm -rf "${OUTPUT_DIR}"
mkdir -p "${OUTPUT_DIR}"

cd "${PROJECT_ROOT}"

docker build \
  --platform "${PLATFORM}" \
  --build-arg TARGETARCH="${ARCH}" \
  --build-arg BUILD_TYPE="${BUILD_TYPE}" \
  --target builder \
  -t "${IMAGE_TAG}" \
  -f "${DOCKERFILE}" \
  .

CONTAINER_ID="$(docker create --platform "${PLATFORM}" "${IMAGE_TAG}")"
trap 'docker rm -f "${CONTAINER_ID}" >/dev/null 2>&1 || true' RETURN

docker cp "${CONTAINER_ID}:/build/bin/libgo_pdf_extractor_lib.a" "${OUTPUT_DIR}/libgo_pdf_extractor_lib.a"
docker rm "${CONTAINER_ID}"
CONTAINER_ID=""
trap - RETURN

echo "Built: ${OUTPUT_DIR}/libgo_pdf_extractor_lib.a"
