#! /usr/bin/env bash

install() {
    local CURRENT_DIR
    CURRENT_DIR="$(pwd)"
    local SCRIPT_DIR
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    local PROJECT_ROOT
    PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

    cd "${PROJECT_ROOT}/shared_lib"
    rm -f go.mod

    go mod init shared_lib
    go mod edit -replace lib/shared_lib=../shared_lib
    go mod edit -replace lib/data_contracts_lib=../data_contracts/build/protobufs

    # Pin OpenTelemetry version that has WithEndpointURL
    go mod edit -require go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp@v1.40.0

    go install -mod=mod github.com/onsi/ginkgo/v2/ginkgo@latest

    go mod tidy
    go mod download

    cd "${CURRENT_DIR}"
}

install
