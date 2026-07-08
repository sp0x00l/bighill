#! /usr/bin/env sh

BIGHILL_ROOT=$(git rev-parse --show-toplevel)
. $BIGHILL_ROOT/shared_lib/scripts/config.sh $1

if [ "$1" != "local-dev" ] && [ "$1" != "cicd" ] && [ "$1" != "staging" ] && [ "$1" != "prod" ]; then
    echo "Error: Invalid environment provided to model_serving_service config"
    echo "Usage: './config.sh [local-dev|cicd|staging|prod]'"
    exit 1
fi

if [ "$1" = "local-dev" ] || [ "$1" = "cicd" ]; then
    export MODEL_SERVING_SERVICE_BACKEND=local
elif [ "$1" = "staging" ] || [ "$1" = "prod" ]; then
    export MODEL_SERVING_SERVICE_BACKEND=kubernetes
fi

export MODEL_SERVING_SERVICE_NAME=model-serving-service
export MODEL_SERVING_SERVICE_NAMESPACE=default
export MODEL_SERVING_SERVICE_LOCAL_STORE_PATH=$BIGHILL_ROOT/tmp/local_served_models/served_models.json
export MODEL_SERVING_SERVICE_HEALTHCHECK_CPU_THRESHOLD_PERCENT=80
export MODEL_SERVING_SERVICE_HEALTHCHECK_FREE_MEM_THRESHOLD_PERCENT=20
export MODEL_SERVING_SERVICE_HEALTHCHECK_PORT=5061
export MODEL_SERVING_SERVICE_HEALTHCHECK_SERVICE_LATENCY_THRESHOLD_SECONDS=5
export MODEL_SERVING_SERVICE_HEALTHCHECK_CONTROLLER_MAX_SILENCE_SECONDS=30
export MODEL_SERVING_SERVICE_POLL_MS=1000
export MODEL_SERVING_SERVICE_SERVED_MODEL_CRD_GROUP=serving.bighill.io
export MODEL_SERVING_SERVICE_SERVED_MODEL_CRD_VERSION=v1alpha1
export MODEL_SERVING_SERVICE_SERVED_MODEL_CRD_RESOURCE=servedmodels
export MODEL_SERVING_SERVICE_VLLM_IMAGE=vllm/vllm-openai:latest
export MODEL_SERVING_SERVICE_VLLM_IMAGE_PULL_POLICY=IfNotPresent
export MODEL_SERVING_SERVICE_VLLM_SERVICE_ACCOUNT=
export MODEL_SERVING_SERVICE_VLLM_MULTI_TENANT_ENABLED=false
export MODEL_SERVING_SERVICE_VLLM_REPLICAS=1
export MODEL_SERVING_SERVICE_VLLM_PORT=8000
export MODEL_SERVING_SERVICE_VLLM_CPU=1
export MODEL_SERVING_SERVICE_VLLM_MEMORY=4Gi
export MODEL_SERVING_SERVICE_VLLM_GPU_RESOURCE=nvidia.com/gpu
export MODEL_SERVING_SERVICE_VLLM_GPU=1
export MODEL_SERVING_SERVICE_VLLM_REQUEST_TIMEOUT_MS=5000
export MODEL_SERVING_SERVICE_LOCAL_OLLAMA_ENDPOINT=http://localhost:11434
export MODEL_SERVING_SERVICE_LOCAL_ARTIFACT_CACHE_DIR=$BIGHILL_ROOT/tmp/model_serving_artifacts
if [ "$1" = "local-dev" ] || [ "$1" = "cicd" ]; then
    export MODEL_SERVING_SERVICE_GGUF_INSPECTOR_COMMAND="sh $BIGHILL_ROOT/model_serving_service/scripts/gguf-inspector.sh"
else
    export MODEL_SERVING_SERVICE_GGUF_INSPECTOR_COMMAND="python3 -m bighill_model_artifacts.gguf"
fi
export MODEL_SERVING_SERVICE_LOCAL_OLLAMA_CREATE_TIMEOUT_SECONDS=1200

if [ "$1" = "local-dev" ] || [ "$1" = "cicd" ]; then
    export BIGHILL_LOCAL_S3_STORAGE_DIR=$BIGHILL_ROOT/tmp/local_s3_storage
fi

# The following are variables set at build time, intended to be used at runtime.
# IMPORTANT: This IDs the K8s deployment instance and is used in the templates.
export MODEL_SERVING_SERVICE_BUILD_VERSION=0.0.1
