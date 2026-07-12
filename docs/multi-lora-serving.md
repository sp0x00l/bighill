# Multi-LoRA Serving

## What it is

Multi-LoRA serving lets many fine-tuned adapters share a single base-model runtime.
Instead of standing up a dedicated vLLM deployment (and GPU) per fine-tuned model, the
platform runs one vLLM process per base model and dynamically loads/unloads LoRA
adapters onto it. This makes per-tenant and per-lineage fine-tunes economical, and is the
serving foundation the [self-improving loop](self-improving-loop.md) relies on for cheap
champion/challenger coexistence and fast rollback.

## Ownership

`model_serving_service` is the serving-plane controller. It reconciles desired serving
intent into running workloads and writes back observed load status. It does **not** decide
promotion — `model_registry_service` remains the authority for `READY`/`FAILED` model
state. `model_serving_service` only answers "is this model loaded and reachable, and on
what target/protocol".

## Custom resources

Two Kubernetes CRDs under `serving.bighill.io/v1alpha1`
(`model_serving_service/helm/templates`):

- **`ServedModel`** — one per model the platform wants served. Carries the model
  identity, `base_model`, `adapter_uri`, `adapter_rank`, `runtime_isolation`
  (`SHARED`/`DEDICATED`), and `pinned`. Domain type:
  `model_serving_service/pkg/domain/model/served_model.go`.
- **`BaseRuntime`** — one per base-model runtime pool. Carries `base_model`, `pool_key`,
  `max_loras`, `max_lora_rank`, the runtime `endpoint`/`phase`, and the list of
  `loaded_adapters`. Domain type:
  `model_serving_service/pkg/domain/model/base_runtime.go`.

## How an adapter gets served

Reconciliation lives in `model_serving_service/pkg/infra/network/k8s/vllm_runtime.go`
(`EnsureServedModel`):

1. **Classify.** `IsAdapter()` is true for a `FINE_TUNED` model with an `adapter_uri`.
   `sharedAdapter()` is true for an adapter when the runtime is not forced dedicated.
2. **Resolve the base runtime.** `resolveBaseRuntime` → `FindOrCreate` on the
   `BaseRuntime` for `(base_model, pool_key)`. A base model without an adapter runs as its
   own workload; adapters attach to the shared base runtime.
3. **Upsert workload + service.** One vLLM Deployment/Service per base runtime, sized with
   `max_loras` / `max_lora_rank`.
4. **Ensure capacity, then load.** `ensureServingModel` → `prepareAdapterCapacity` →
   `loadLoraAdapter` calls vLLM's OpenAI-compatible `load_lora_adapter` endpoint;
   `recordAdapterLoaded` records it on the `BaseRuntime`.
5. **Write status back.** Observed `serving_target`, `serving_model`, `serving_protocol`,
   ready replicas, and load status are written for registry/inference to consume.

Deletion (`DeleteServedModel`) unloads the adapter from vLLM and removes it from the base
runtime's loaded-adapter list.

## Runtime pools: shared vs dedicated

`RuntimePoolKey` (`model_serving_service/pkg/infra/network/k8s/names.go`) decides which
runtime an adapter lands on:

- **Shared (default)** — `pool_key = base_model`. All adapters for the same base model,
  across tenants, share one runtime pool. Maximum density.
- **Dedicated** — when `runtime_isolation = DEDICATED` and an org id is present,
  `pool_key = org_id`. Gives a tenant an isolated base runtime (noisy-neighbour / data
  isolation), at the cost of density.

The base runtime resource name is derived from `base_model` + `pool_key`
(`BaseRuntimeResourceName`), so shared and dedicated pools never collide.

## Capacity, eviction, and pinning

A base runtime can hold at most `max_loras` adapters. When a new adapter needs to load and
the runtime is full (`prepareAdapterCapacity`):

- If the adapter is already loaded → no-op.
- If there is room → load directly.
- If full → pick an **LRU victim** (`lruEvictionCandidate`): the least-recently-used
  adapter that is **not pinned**. Unload it from vLLM, remove it from the base runtime,
  and mark the evicted `ServedModel` as `NOT_LOADED` with reason
  `capacity_evicted` (`NotLoadedReasonCapacityEvicted`) so the controller can re-load it
  on demand later.
- If **all** loaded adapters are pinned → fail with "base runtime at capacity with all
  adapters pinned" rather than evicting a pinned model.

**Pinning** (`ServedModel.Pinned`, propagated to `BaseRuntimeLoadedAdapter.Pinned`) keeps
an adapter resident. This is what makes instant rollback cheap: the champion adapter can be
pinned so it is never evicted, so cutting back to it after a bad promotion is immediate.

**Compatibility checks** before load: the adapter's `base_model` must match the runtime's
base model, and `adapter_rank` must be known and `≤ max_lora_rank`
(`validateAdapterCompat`, `validateAdapterRankKnown`).

Capacity-evicted models are treated as needing reconciliation
(`statusNeedsReconcile` in `controller.go`), so eviction is recoverable, not terminal.

## Backends

- **Kubernetes** (`MODEL_SERVING_SERVICE_BACKEND=kubernetes`) — the CRD/vLLM path above.
  Staging/prod. Requires the `ServedModel` and `BaseRuntime` CRDs installed.
- **Local** (`model_serving_service/pkg/infra/network/localserving`) — default for
  local-dev and CI so tests need no cluster or GPU. Validates artifacts and serves through
  Ollama; the GGUF chat-template path is deliberately fail-closed (see
  `model_serving_service/README.md` for the exact acceptance rules). Raw
  `HF_PEFT_ADAPTER` directories are not local-servable; they must be represented as
  validated `GGUF_LORA_ADAPTER` artifacts for local adapter serving.

## How serving is triggered from inference

When `inference_service` needs a model that is not yet loaded, it triggers a load rather
than failing (`inference_service/pkg/infra/modelserving/http_load_trigger.go`,
`ensureServingModelLoaded` in `inference_usecase.go`): it calls the serving load trigger,
then polls the model record until `ServingLoadStatus` becomes `Loaded` (or times out /
fails). This is how a capacity-evicted adapter is transparently re-loaded on next use.

## Controller

`model_serving_service/pkg/infra/network/k8s/controller.go` watches both `ServedModel` and
`BaseRuntime` resources, reconciles on change and on a poll interval, serialises work per
resource (and per shared runtime via `sharedRuntimeLockKey`) to avoid concurrent
load/unload races, and requeues capacity-evicted models. It exposes a health endpoint with
last-successful-reconcile tracking for supervision.

## Testing

- Unit: `model_serving_service/pkg/infra/network/k8s/base_runtime_store_test.go`,
  `vllm_runtime` tests, `served_model_reconciler_test.go`,
  `inference_service/pkg/infra/modelserving/http_load_trigger_test.go`.
- End-to-end: `api_gateway/test/multi_lora_serving_test.go` — exercises multiple adapters
  over a shared base runtime through the real serving path.
