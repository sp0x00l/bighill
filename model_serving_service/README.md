# model_serving_service

## What It Does

`model_serving_service` reconciles desired model-serving intent into running serving workloads. In Kubernetes mode it watches `ServedModel` resources and ensures vLLM workloads are created, adapters are loaded, and serving status is written back. In local mode it uses the shared local served-model store to make CI and local e2e loops runnable without a cluster or GPU.

It is the serving-plane controller for model load status. It does not decide promotion; `model_registry_service` remains the authority for READY/FAILED model state.

## MLOps / Platform Pieces

- Kubernetes custom resources: `serving.bighill.io/v1alpha1 ServedModel`.
- vLLM OpenAI-compatible runtime.
- LoRA adapter loading through vLLM.
- Optional multi-tenant mode for shared base-model runtimes with multiple adapters.
- Local served-model backend for local-dev/CI.
- Health endpoint for process supervision.

## How It Fits

- Reads desired serving specs written by `model_registry_service`.
- Reconciles vLLM Deployment/Service resources in Kubernetes mode.
- Confirms loaded models through the vLLM model API.
- Writes observed load status for registry to consume.

## Local Development

Local and CI default to the local backend so tests do not require Kubernetes or vLLM. Staging/prod should use `MODEL_SERVING_SERVICE_SERVING_BACKEND=kubernetes` with the `ServedModel` CRD installed.
