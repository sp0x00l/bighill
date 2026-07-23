#!/usr/bin/env bash
set -euo pipefail

absolute_path()
{
    local BASE_DIR="$1"
    local PATH_VALUE="$2"
    if [[ "$PATH_VALUE" != /* ]]; then
        PATH_VALUE="$BASE_DIR/$PATH_VALUE"
    fi
    local DIR_VALUE
    DIR_VALUE="$(dirname "$PATH_VALUE")"
    if [ -d "$DIR_VALUE" ]; then
        PATH_VALUE="$(cd "$DIR_VALUE" && pwd)/$(basename "$PATH_VALUE")"
    fi
    echo "$PATH_VALUE"
}

prepare_default_gguf()
{
    local BIGHILL_ROOT="$1"
    local TARGET_PATH="$2"
    if [ -f "$TARGET_PATH" ]; then
        return
    fi

    mkdir -p "$(dirname "$TARGET_PATH")"
    local SOURCE_PATH
    SOURCE_PATH="$(find "$BIGHILL_ROOT/tmp/model_serving_artifacts" "$BIGHILL_ROOT/tmp/local_s3_storage" -type f -name '*.gguf' -print -quit 2>/dev/null || true)"
    if [ -z "$SOURCE_PATH" ]; then
        echo "No local GGUF source is available to create $TARGET_PATH"
        echo "Run the model artifact ingestion fixture first, or pass GGUF=/path/to/chat-model.gguf"
        exit 1
    fi
    if ! ln "$SOURCE_PATH" "$TARGET_PATH" 2>/dev/null; then
        cp "$SOURCE_PATH" "$TARGET_PATH"
    fi
}

test_ollama()
{
    local ENVIRONMENT="${1:-local-dev}"
    local CURRENT_DIR
    CURRENT_DIR="$(pwd)"
    trap "cd '$CURRENT_DIR'" EXIT

    local BIGHILL_ROOT
    BIGHILL_ROOT="$(git rev-parse --show-toplevel)"
    . "$BIGHILL_ROOT/scripts/common.sh"

    local CONFIGURED_OLLAMA_ENDPOINT="${OLLAMA_ENDPOINT:-${MODEL_SERVING_SERVICE_LOCAL_OLLAMA_ENDPOINT:-}}"
    local DEFAULT_GGUF_PATH="$BIGHILL_ROOT/model_serving_service/test/data/ollama-chat.gguf"
    local GGUF_PATH
    if [ -n "${GGUF:-}" ]; then
        GGUF_PATH="$(absolute_path "$CURRENT_DIR" "$GGUF")"
    else
        prepare_default_gguf "$BIGHILL_ROOT" "$DEFAULT_GGUF_PATH"
        GGUF_PATH="$DEFAULT_GGUF_PATH"
    fi
    if [ ! -f "$GGUF_PATH" ]; then
        echo "Configured GGUF path is not a file: $GGUF_PATH"
        exit 1
    fi

    . "$BIGHILL_ROOT/shared_lib/scripts/config.sh" "$ENVIRONMENT"
    cd "$BIGHILL_ROOT/model_serving_service"
    . ./scripts/config.sh "$ENVIRONMENT"
    if [ -n "$CONFIGURED_OLLAMA_ENDPOINT" ]; then
        export MODEL_SERVING_SERVICE_LOCAL_OLLAMA_ENDPOINT="$CONFIGURED_OLLAMA_ENDPOINT"
    fi

    local OLLAMA_ENDPOINT_VALUE="${MODEL_SERVING_SERVICE_LOCAL_OLLAMA_ENDPOINT:-http://localhost:11434}"
    OLLAMA_ENDPOINT_VALUE="${OLLAMA_ENDPOINT_VALUE%/}"
    export MODEL_SERVING_SERVICE_LOCAL_OLLAMA_ENDPOINT="$OLLAMA_ENDPOINT_VALUE"

    if ! curl -fsS --max-time 5 "$OLLAMA_ENDPOINT_VALUE/api/tags" >/dev/null; then
        echo "Ollama is not reachable at $OLLAMA_ENDPOINT_VALUE. Start local infra with: make start-infra"
        exit 1
    fi

    local RESULTS_DIR="$BIGHILL_ROOT/test_results/model_serving_service/ollama"
    rm -rf "$RESULTS_DIR"
    mkdir -p "$RESULTS_DIR"
    stop_service "model_serving_service"

    echo "Running Ollama GGUF integration against $OLLAMA_ENDPOINT_VALUE"
    echo "GGUF artifact: $GGUF_PATH"
    ginkgo -timeout=30m -v --output-dir="$RESULTS_DIR" -procs=1 -race --label-filter='ollama' ./test -- -gguf-path="$GGUF_PATH"

    echo "Ollama GGUF integration complete"
}

test_ollama "${1:-local-dev}"
