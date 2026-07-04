#!/bin/bash
set -euo pipefail

DOMAIN="bighill-artifacts"
REPOSITORY="cpp-libs"
REGION="${AWS_REGION:-eu-west-1}"

upload_pdf_extractor_lib() {
    local ARCH="${1:-arm64}"
    local LIBC="${2:-musl}"
    local SCRIPT_DIR
    local PDF_EXTRACTOR_LIB_DIR
    local LIB_ROOT
    local LIB_FILE
    local TEMP_DIR
    local VERSION
    local VERSION_SUFFIX
    local ARTIFACT_FILE

    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    PDF_EXTRACTOR_LIB_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
    LIB_ROOT="${PDF_EXTRACTOR_LIB_DIR}/cpp/build/linux/${LIBC}/${ARCH}"
    LIB_FILE="${LIB_ROOT}/libgo_pdf_extractor_lib.a"
    TEMP_DIR="$(mktemp -d)"
    VERSION="latest"
    VERSION_SUFFIX="${VERSION}-${LIBC}-${ARCH}"
    ARTIFACT_FILE="${TEMP_DIR}/pdf-extractor-lib-${VERSION_SUFFIX}.tar.gz"

    if [ ! -f "${LIB_FILE}" ]; then
        echo "Error: Library not found at ${LIB_FILE}"
        echo "Please run 'make build_linux' first"
        rm -rf "${TEMP_DIR}"
        return 1
    fi

    echo "Packaging pdf_extractor_lib..."
    tar --no-xattrs -czf "${ARTIFACT_FILE}" -C "$(dirname "${LIB_FILE}")" "$(basename "${LIB_FILE}")" 2>/dev/null || \
        tar -czf "${ARTIFACT_FILE}" -C "$(dirname "${LIB_FILE}")" "$(basename "${LIB_FILE}")"

    echo "Uploading to CodeArtifact..."
    aws codeartifact delete-package-versions \
        --domain "${DOMAIN}" \
        --repository "${REPOSITORY}" \
        --format generic \
        --namespace cpp \
        --package pdf-extractor-lib \
        --versions "${VERSION_SUFFIX}" \
        --region "${REGION}" 2>/dev/null || true

    aws codeartifact publish-package-version \
        --domain "${DOMAIN}" \
        --repository "${REPOSITORY}" \
        --format generic \
        --namespace cpp \
        --package pdf-extractor-lib \
        --package-version "${VERSION_SUFFIX}" \
        --asset-content "${ARTIFACT_FILE}" \
        --asset-name "pdf-extractor-lib-${VERSION_SUFFIX}.tar.gz" \
        --asset-sha256 "$(shasum -a 256 "${ARTIFACT_FILE}" | cut -d' ' -f1)" \
        --region "${REGION}"

    echo "Successfully uploaded pdf-extractor-lib-${VERSION_SUFFIX}"

    rm -rf "${TEMP_DIR}"
}

upload_pdf_extractor_lib "$@"
