# Agentic Rails

## Status

Current architecture and roadmap. Companion decisions:
[ADR 0003 — Effective-Base Identity](adr/0003-effective-base-identity.md),
[ADR 0004 — Agent Authoring & Extensibility](adr/0004-agent-authoring-and-extensibility.md),
[ADR 0005 — Tool Execution Boundary Contract](adr/0005-tool-execution-service-boundary-contract.md),
[ADR 0006 — Docker-Image Capability Rails](adr/0006-docker-image-capability-rails.md).

## 1. Why This Exists

Enterprises want agents that work on their data and their workflows.

The common choices are incomplete:

- Frontier API agents are powerful, but data and behavior flow through a vendor boundary.
- DIY SDK agents are flexible, but they are hard to govern, audit, reproduce, and improve.

BigHill’s answer is to own the loop: run agents in the tenant boundary, record what happened, train
from the useful traces, evaluate the result, promote only when it improves, then serve the improved
agent on the next run.

The practical promise is simple: agents on your data, in your boundary, that get measurably better
and remain auditable.

“Better” must mean real numbers, not a vibe:

- holdout task success rate
- grounded-answer rate and citation accuracy
- tool success rate
- human acceptance rate
- escalation reduction
- cost / latency at equal quality

BigHill may host customer code as governed capabilities, including Docker images. It does not let
customer code become the agent runtime itself.

## 2. Architecture

The design has five rules:

1. **The agent runtime is closed to code injection.** Capabilities enter only through typed ports; untrusted code never runs in the core loop.
2. **The agent artifact is declarative and content-addressed.** The deployable spec is a validated, immutable manifest. Power lives in the capabilities it references, not embedded code.
3. **Every capability is governed:** identity + version + schema + isolation + policy + tracing + audit + platform-managed credentials.
4. **Remove-or-wire honesty:** nothing ships without a producer and a consumer; no fake defaults; perishable provenance captured at the source.
5. **Isolation matched to risk:** read-only reads in-process; world-acting tools sandboxed (`tool_execution_service`); side-effecting work durable + approved (Temporal); untrusted code in a rented or hosted sandbox.

That gives this shape:

- **Control plane (declarative):** content-addressed agent specs; a capability catalog (tools/MCP/memory/policy/sub-agents — versioned, tenant-granted, credential-bound); content-addressed effective-base (served-artifact) identity.
- **Data plane (runtime):** a small agent loop calling typed ports — `GenerationAdapter` (model_serving/vLLM/Multi-LoRA), `ToolInvoker` (local RAG + `tool_execution_service`/MCP boundary), `MemoryPort` (memory_service), `PolicyPort` (policy_service), `DurableActivityPort` (Temporal), `EventPublisher` (socket_service).
- **Data substrate:** multi-source ingestion → materialization (embeddings + graph) → **versioned snapshots** → retrieval (RAG + GraphRAG) as grounding.
- **The flywheel:** run → provenance-attributed trajectory → label/eval → trajectory-shaped training data → tenant adapter over the effective base → promotion gate on holdout → champion → serving binding → next run.

Every run is pinned to a provenance tuple:

```text
agent_spec_hash
toolset_hash
effective_base_id
data_snapshot_set
policy_set_hash
credential_binding_ids
schema_hashes
runtime/host versions
```

The tuple grows only when the producer exists. If there is no policy service, there is no
`policy_set_hash`. If there is no credential-binding producer, there is no credential-binding tuple
field. Empty columns are not extensibility; they are false contracts.

`http_get` is the reference implementation for a hardened world-acting tool. MCP and future SQL,
browser, and container tools inherit the same boundary objects: egress, timeout, response cap,
credentials, schema validation, audit, and events.

## 3. Current State

**Built foundation:**
- managed interactive agent loop
- content-addressed agent specs
- trajectories (with model + tool provenance)
- `tool_execution_service` boundary with hardened `http_get`, MCP, durable audit, and shared boundary policy
- `tool_catalog_service` control plane for dynamic tool manifests, tenant grants, and credential bindings
- socket_service + shared_lib/userevents
- Temporal
- service-owned Postgres state
- RLS multi-tenancy

**Still hardening:**
- systematic user-visible failure events (coverage, see §4.3).
- Multi-LoRA production serving path.
- GraphRAG quality evaluation for model-serving extraction.

**Still missing from the flywheel middle:**
- `agent_registry_service` — version/champion state and the **champion→serving edge**
- golden tasks — customer-specific holdout/eval set
- eval runner — measure lift before promotion
- trajectory labeler — human/model/evaluator labels
- **trajectory-shaped dataset builder** — the proprietary transform (agent training data has tool calls, tool results, intermediate reasoning, failures, and policy decisions; this is a *new* builder, not an extension of the single-turn RAG/DPO path)
- adapter training for agent traces (reuse training_service/Ray/Axolotl where possible)
- promotion gate — holdout, no-regression, rollback
- serving compatibility — adapter binds only if base/spec/toolset match

**Still missing extensibility slices:**
- hosted container runtime (ADR 0006)
- capability SDK/CLI

## 4. Near-Term Work

**4.1 Keep the tool boundary shared.** `http_get` and MCP use the same egress, timeout, response cap,
schema, credential, and durable audit path. Future tool kinds must inherit this boundary rather than
reimplementing it.

**4.2 Keep GraphRAG extraction honest.** Graph extraction uses model-serving with a real embedded
prompt artifact, schema validation, and prompt-content provenance. The local co-occurrence
extractor remains an explicitly named dev-only implementation; it must not be presented as
model-extracted GraphRAG.

**4.3 Surface failures to users.** Every user-relevant async failure needs a user event through
socket_service: model-serving failed, chat template unusable, materialization failed, embedding
failed, tool denied/timeout/egress-blocked, agent run failed, policy blocked, approval required, and
approval rejected. This is product behavior, not just observability.

**4.4 Grow provenance only with real producers.** Pin what exists now: `agent_spec_hash`,
`toolset_hash`, `effective_base_id`, `data_snapshot_set`, and known model/runtime ids. Add
`policy_set_hash`, `credential_binding_ids`, schema hashes, and catalog ids only with the slices that
produce them.

## 5. Sequencing

1. **Fix honesty gaps:** GraphRAG extractor truth; Phase A/B closeout (dedup, stale-run reaper, idempotency, rate-limit, timeout envelope).
2. **Pin perishable provenance now:** `effective_base_id`, `data_snapshot_set`, and other fields with real producers.
3. **Generalize `tool_execution_service` hardening** into a shared boundary layer; add durable audit; confirm failure-event coverage. Implemented for `http_get` and MCP.
4. **Add one config-pinned MCP tool** for the design partner, then graduate dynamic tools into `tool_catalog_service`: catalog publishes capability/grant/credential events, and execution projects them locally. Implemented for tools; hosted containers, SDK/CLI, memory, policy, and approval remain later slices.
5. **Prove one real flywheel turn:** trajectory → label/eval → train → promote → champion → serve → **measured holdout lift** on the design-partner dataset.
6. **Then** build hosted containers / SDK / memory / policy / approval as demand-triggered slices.
7. **Then** scale SaaS data-source breadth.

The design-partner proof must not wait for every extension. It needs one real, hardened integration.
The catalog/control plane helps scale integrations; the flywheel proof is the main milestone.

## 6. Milestone

> One real design-partner dataset produces **measured lift** through the full loop: trajectory → label/eval → train → promote → champion → serve → holdout improvement.

Before that milestone is credible, the platform must have:
- honest GraphRAG/materialization state
- a complete (producer-backed) provenance tuple
- the generalized tool boundary + durable audit
- systematic user-visible failure events
- the champion→serving edge

That is the real “rails figured out” gate. Everything after it compounds a loop that has proven it
can produce lift.
