# Multi-LoRA Serving

## What It Is

Multi-LoRA serving means the platform runs one large base model once, then loads many small
fine-tuned LoRA adapters onto that shared runtime.

Instead of this:

```text
Fine-tuned model A -> its own vLLM server + GPU
Fine-tuned model B -> its own vLLM server + GPU
Fine-tuned model C -> its own vLLM server + GPU
```

the platform wants this:

```text
Base model runtime -> one vLLM server + GPU
  ├── LoRA adapter A
  ├── LoRA adapter B
  └── LoRA adapter C
```

That is much cheaper because a fine-tuned model is usually:

```text
base model + small adapter
```

The base model is expensive to keep loaded. The adapter is relatively small. So the platform keeps
the base model running and dynamically loads adapters onto it.

This is the serving foundation for the [self-improving loop](self-improving-loop.md): champion and
challenger models can coexist cheaply, and rollback can be fast when the champion stays loaded.

## Ownership

`model_serving_service` is the serving-plane controller. It does not decide whether a model is good,
promoted, or failed from a product/model-registry perspective.

It only answers:

```text
Is this model loaded?
Where is it served?
What protocol should inference use?
```

`model_registry_service` remains the authority for model lifecycle state such as `READY`, `FAILED`,
candidate/champion decisions, and promotion results.

## Kubernetes Resources

The Kubernetes backend uses two CRDs under `serving.bighill.io/v1alpha1`
(`model_serving_service/helm/templates`).

### `ServedModel`

`ServedModel` means:

```text
Please serve this specific model.
```

It carries model identity and serving intent:

- `model_id`
- `base_model`
- `adapter_uri`
- `adapter_rank`
- `runtime_isolation` (`SHARED` or `DEDICATED`)
- `pinned`

Domain type: `model_serving_service/pkg/domain/model/served_model.go`.

### `BaseRuntime`

`BaseRuntime` means:

```text
This is the shared running base-model pool.
```

It tracks:

- `base_model`
- `pool_key`
- `max_loras`
- `max_lora_rank`
- runtime `endpoint`
- runtime `phase`
- `loaded_adapters`

Domain type: `model_serving_service/pkg/domain/model/base_runtime.go`.

So `ServedModel` is the request to serve a model. `BaseRuntime` is the shared runtime that adapters
attach to.

## How An Adapter Gets Served

The reconciliation code lives in
`model_serving_service/pkg/infra/network/k8s/vllm_runtime.go` in `EnsureServedModel`.

When a fine-tuned adapter needs to be served:

1. The controller sees a `ServedModel`.
2. It checks whether the model is a fine-tuned adapter (`FINE_TUNED` with an `adapter_uri`).
3. It finds or creates the correct `BaseRuntime` for the adapter's `base_model` and runtime pool.
4. It ensures a vLLM Deployment/Service exists for that base runtime.
5. It checks that the runtime has capacity for another adapter.
6. It calls vLLM's OpenAI-compatible `load_lora_adapter` endpoint.
7. It records the adapter in the `BaseRuntime` loaded-adapter list.
8. It writes observed serving status back for registry and inference to consume.

The written status includes:

- `serving_target`
- `serving_model`
- `serving_protocol`
- ready replicas
- load status
- failure reason, when loading fails

`inference_service` does not need Kubernetes details. It only needs the recorded serving target,
model name, and protocol.

Deletion does the inverse: `DeleteServedModel` unloads the adapter from vLLM and removes it from the
`BaseRuntime` loaded-adapter list.

## Shared vs Dedicated Runtime

Adapters can land in shared or dedicated runtime pools.

### Shared Runtime

Shared mode is the default:

```text
pool_key = base_model
```

All adapters for the same base model share one runtime, even across tenants.

Example:

```text
llama-3-8b runtime
  ├── org-a adapter
  ├── org-b adapter
  └── org-c adapter
```

This gives maximum density and lowest cost.

### Dedicated Runtime

Dedicated mode is used when:

```text
runtime_isolation = DEDICATED
```

and an org id is present.

In that case:

```text
pool_key = org_id
```

Example:

```text
org-a llama-3-8b runtime
  └── org-a adapters only
```

This costs more, but gives a tenant isolated capacity and avoids noisy-neighbour concerns.

The `BaseRuntime` resource name is derived from `base_model` and `pool_key`, so shared and dedicated
pools do not collide.

## Capacity, Eviction, And Pinning

Each base runtime can only hold a limited number of adapters:

```text
max_loras
```

Before loading an adapter, the service checks compatibility:

- the adapter's `base_model` must match the runtime's base model;
- the adapter rank must be known;
- the adapter rank must be `<= max_lora_rank`.

When the runtime is full and another adapter needs to load, `prepareAdapterCapacity` chooses what to
do:

1. If the adapter is already loaded, nothing changes.
2. If there is room, load the adapter directly.
3. If the runtime is full, evict the least-recently-used adapter that is not pinned.
4. If every loaded adapter is pinned, fail instead of evicting a pinned model.

Evicted adapters are marked `NOT_LOADED` with reason `capacity_evicted`. That is not terminal. The
controller treats capacity-evicted models as needing reconciliation, so they can be loaded again when
they are used later.

Pinning protects an adapter from eviction.

This is useful for rollback. The current champion adapter can stay pinned, so if a challenger is bad,
the system can switch back to the champion immediately.

## Backends

There are two serving backends.

### Kubernetes Backend

```text
MODEL_SERVING_SERVICE_BACKEND=kubernetes
```

This is the real CRD + vLLM path used in staging and production. It requires the `ServedModel` and
`BaseRuntime` CRDs to be installed.

### Local Backend

```text
MODEL_SERVING_SERVICE_BACKEND=local
```

This is used for local-dev and CI service-script runs so tests do not need Kubernetes or GPUs.

The local backend serves through Ollama where possible. The GGUF chat-template path is deliberately
fail-closed; see `model_serving_service/README.md` for the exact acceptance rules.

Important local limitation:

```text
Raw HF_PEFT_ADAPTER directories are not directly local-servable.
```

For local adapter serving, artifacts need to be represented as validated `GGUF_LORA_ADAPTER`
artifacts.

## How Inference Triggers Loading

If `inference_service` needs a model that is not currently loaded, it does not immediately fail.

It:

1. calls the serving load trigger;
2. waits for the model record to show `ServingLoadStatus=LOADED`;
3. fails only if loading fails or times out.

This is how capacity eviction becomes recoverable. If an adapter was evicted because the runtime was
full, the next inference request can trigger it to load again.

Relevant code:

- `inference_service/pkg/infra/modelserving/http_load_trigger.go`
- `ensureServingModelLoaded` in `inference_service/pkg/app/inference_usecase.go`

## Controller

`model_serving_service/pkg/infra/network/k8s/controller.go` watches `ServedModel` and `BaseRuntime`
resources.

It:

- reconciles when resources change;
- reconciles on a poll interval;
- serialises work per resource;
- serialises work per shared runtime via `sharedRuntimeLockKey`;
- avoids concurrent load/unload races;
- requeues capacity-evicted models;
- exposes health status based on last successful reconciliation.

## Plain English Summary

Multi-LoRA serving is a cost-saving serving system.

Instead of running a full GPU server for every fine-tuned model, the platform runs one base model and
loads small LoRA adapters into it as needed.

The system:

- shares base-model runtimes across adapters;
- optionally gives tenants dedicated runtimes;
- evicts least-used adapters when a runtime is full;
- protects pinned adapters for fast rollback;
- lets inference trigger reloads on demand;
- uses Kubernetes/vLLM in staging and production;
- uses local/Ollama validation paths in local-dev and CI.

## Testing

- Unit tests cover `BaseRuntime` storage, vLLM runtime behavior, served-model reconciliation, and
  inference load triggering.
- End-to-end coverage lives in `api_gateway/test/multi_lora_serving_test.go`, which exercises
  multiple adapters over a shared base runtime through the real serving path.
