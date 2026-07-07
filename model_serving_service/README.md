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
- Writes observed load status plus `serving_protocol`, `serving_target`, and `serving_model` for registry and inference to consume.

## Local Development

Local and CI default to the local backend so tests do not require Kubernetes or vLLM. Staging/prod should use `MODEL_SERVING_SERVICE_SERVING_BACKEND=kubernetes` with the `ServedModel` CRD installed.

The serving contract is model-family agnostic. Llama, Mistral, Qwen, DeepSeek, Gemma, and similar open model families are represented by the model record's `base_model` / `serving_model` data. They do not require new protocol enum values when they run on an existing runtime.

Local-dev currently verifies shared/base models by checking that the requested Ollama tag exists through `/api/tags`, then records `serving_protocol=OLLAMA_GENERATE`. Inference dispatches through that recorded protocol and calls the local Ollama `/api/generate` endpoint. Local-dev does not load fine-tuned `HF_PEFT_ADAPTER` artifacts into Ollama. Fine-tuned local serving requires a real HF-PEFT to GGUF adapter conversion step plus `ollama create` with a Modelfile; until that exists, non-base local served models fail closed instead of silently serving the base tag.
