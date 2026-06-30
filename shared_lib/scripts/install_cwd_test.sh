#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
TMP_DIR="$(mktemp -d)"
LOG_FILE="${TMP_DIR}/go.log"

cleanup() {
    rm -rf "${TMP_DIR}"
}

trap cleanup EXIT

FIXTURE_ROOT="${TMP_DIR}/backend"
mkdir -p \
    "${FIXTURE_ROOT}/shared_lib/scripts" \
    "${FIXTURE_ROOT}/shared_lib" \
    "${FIXTURE_ROOT}/data_contracts/build/protobufs" \
    "${TMP_DIR}/bin"

cp "${PROJECT_ROOT}/shared_lib/scripts/install.sh" "${FIXTURE_ROOT}/shared_lib/scripts/install.sh"
chmod +x "${FIXTURE_ROOT}/shared_lib/scripts/install.sh"

cat > "${TMP_DIR}/bin/go" <<EOF
#!/usr/bin/env bash
set -euo pipefail
printf '%s|%s\n' "\$PWD" "\$*" >> "${LOG_FILE}"
exit 0
EOF
chmod +x "${TMP_DIR}/bin/go"

(
    cd "${FIXTURE_ROOT}"
    PATH="${TMP_DIR}/bin:${PATH}" bash "${FIXTURE_ROOT}/shared_lib/scripts/install.sh"
)

if [ ! -s "${LOG_FILE}" ]; then
    echo "expected install.sh to invoke go"
    exit 1
fi

EXPECTED_DIR="${FIXTURE_ROOT}/shared_lib"
if ! awk -F'|' -v expected="${EXPECTED_DIR}" '$1 != expected { exit 1 }' "${LOG_FILE}"; then
    echo "install.sh invoked go from the wrong directory"
    cat "${LOG_FILE}"
    exit 1
fi

echo "shared_lib install cwd test passed"
