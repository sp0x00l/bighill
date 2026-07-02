#! /usr/bin/env sh

BIGHILL_ROOT=$(git rev-parse --show-toplevel)
. $BIGHILL_ROOT/shared_lib/scripts/config.sh $1

if [ "$1" != "local-dev" ] && [ "$1" != "cicd" ] && [ "$1" != "staging" ] && [ "$1" != "prod" ]; then
    echo "Error: Invalid environment provided to model_serving_service config"
    echo "Usage: './config.sh [local-dev|cicd|staging|prod]'"
    exit 1
fi

export MODEL_SERVING_SERVICE_NAME=model-serving-service
export MODEL_SERVING_NAMESPACE=default
export MODEL_SERVING_HEALTHCHECK_PORT=5061
export MODEL_SERVING_POLL_MS=1000
export MODEL_SERVING_SERVED_MODEL_CRD_GROUP=serving.bighill.io
export MODEL_SERVING_SERVED_MODEL_CRD_VERSION=v1alpha1
export MODEL_SERVING_SERVED_MODEL_CRD_RESOURCE=servedmodels
export MODEL_SERVING_VLLM_IMAGE=vllm/vllm-openai:latest
export MODEL_SERVING_VLLM_IMAGE_PULL_POLICY=IfNotPresent
export MODEL_SERVING_VLLM_SERVICE_ACCOUNT=
export MODEL_SERVING_VLLM_REPLICAS=1
export MODEL_SERVING_VLLM_PORT=8000
export MODEL_SERVING_VLLM_CPU=1
export MODEL_SERVING_VLLM_MEMORY=4Gi
export MODEL_SERVING_VLLM_GPU_RESOURCE=nvidia.com/gpu
export MODEL_SERVING_VLLM_GPU=1

# The following are variables set at build time, intended to be used at runtime.
# IMPORTANT: This IDs the K8s deployment instance and is used in the templates.
export MODEL_SERVING_SERVICE_BUILD_VERSION=0.0.1
