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

Local and CI default to the local backend so tests do not require Kubernetes or vLLM. Staging/prod should use `MODEL_SERVING_SERVICE_BACKEND=kubernetes` with the `ServedModel` CRD installed.

The serving contract is model-family agnostic. Llama, Mistral, Qwen, DeepSeek, Gemma, and similar open model families are represented by the model record's `base_model` / `serving_model` data. They do not require new protocol enum values when they run on an existing runtime.

Local-dev verifies shared/base tags through Ollama `/api/tags` and records `serving_protocol=OLLAMA_GENERATE` for those existing tags. Exact GGUF chat artifacts are different: local serving validates metadata, requires `tokenizer.chat_template`, uploads blobs through Ollama `/api/blobs`, creates a deterministic tag through `/api/create`, and records `serving_protocol=OPENAI_CHAT_COMPLETIONS` only after the created tag exposes a usable chat definition.

The GGUF chat-template path is deliberately fail-closed:

- The first create request lets Ollama infer the Modelfile template from GGUF metadata.
- The created tag is accepted only when `/api/show` returns an Ollama-compatible template and stop parameters.
- Raw Hugging Face/Jinja templates are rejected if they appear in `/api/show`; they are metadata, not an Ollama template payload.
- If Ollama inference does not produce a usable chat definition, local serving deletes that attempted tag and falls back only for recognized chat formats: Llama 3, ChatML/Qwen-style, Mistral/Mixtral Instruct, Gemma, Phi, and Llama 2 chat.
- Stop tokens discovered from GGUF tokenizer metadata are unioned with family defaults before create.

Run the opt-in Ollama GGUF provisioning integration locally with:

```bash
make start-infra
make test-ollama
```

The default fixture path is `model_serving_service/test/data/ollama-chat.gguf`. Use `GGUF=/path/to/chat-model.gguf` for a specific artifact, and `OLLAMA_ENDPOINT=http://host:11434` if Ollama is not on the default local endpoint. The GGUF artifact must be chat-capable and include `tokenizer.chat_template`; missing chat metadata is a test failure, not a skip.

Raw `HF_PEFT_ADAPTER` directories are not local-compatible serving artifacts; they must be represented as validated `GGUF_LORA_ADAPTER` artifacts for local adapter serving.
