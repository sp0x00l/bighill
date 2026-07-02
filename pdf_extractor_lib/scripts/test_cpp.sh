#!/usr/bin/env bash
set -euo pipefail

run_cpp_unit_tests() {
    echo "Running C++ unit tests..."

    local SCRIPT_DIR
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    local PROJECT_ROOT
    PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
    local CPP_ROOT="${PROJECT_ROOT}/pdf_extractor_lib/cpp"
    local CPP_TEST_ROOT="${CPP_ROOT}/test"
    local CPP_TEST_BUILD_DIR="${CPP_TEST_ROOT}/build"
    local OUTPUT_DIR="${PROJECT_ROOT}/test_results/pdf_extractor_lib"
    local CPP_TEST_BINARY="${CPP_TEST_BUILD_DIR}/pdf_extractor_lib_unit_tests"
    local BUILD_JOBS

    if command -v nproc >/dev/null 2>&1; then
        BUILD_JOBS="$(nproc)"
    elif command -v sysctl >/dev/null 2>&1; then
        BUILD_JOBS="$(sysctl -n hw.ncpu)"
    else
        BUILD_JOBS=4
    fi

    mkdir -p "$OUTPUT_DIR"

    echo "Building C++ unit tests..."
    if [ -f "${CPP_TEST_BUILD_DIR}/CMakeCache.txt" ]; then
        local cached_home
        cached_home="$(sed -n 's/^CMAKE_HOME_DIRECTORY:INTERNAL=//p' "${CPP_TEST_BUILD_DIR}/CMakeCache.txt" | tail -n 1)"
        if [ "${cached_home}" != "${CPP_TEST_ROOT}" ]; then
            rm -rf "${CPP_TEST_BUILD_DIR}"
        fi
    fi
    mkdir -p "${CPP_TEST_BUILD_DIR}"

    cmake -S "${CPP_TEST_ROOT}" -B "${CPP_TEST_BUILD_DIR}" \
        -DCMAKE_BUILD_TYPE=Debug \
        -DCMAKE_CXX_COMPILER=g++ \
        -DCMAKE_C_COMPILER=gcc

    cmake --build "${CPP_TEST_BUILD_DIR}" --target pdf_extractor_lib_unit_tests -j"${BUILD_JOBS}"

    if [ ! -x "${CPP_TEST_BINARY}" ]; then
        echo "Expected test binary not found: ${CPP_TEST_BINARY}"
        exit 1
    fi

    cd "${CPP_TEST_BUILD_DIR}"
    "${CPP_TEST_BINARY}" 2>&1 | tee "${OUTPUT_DIR}/cpp_unit_tests.log"
    local CPP_TEST_RESULT=${PIPESTATUS[0]}

    if [ $CPP_TEST_RESULT -ne 0 ]; then
        echo "C++ unit tests failed"
        exit 1
    fi

    echo "C++ unit tests passed"
}

run_cpp_unit_tests
