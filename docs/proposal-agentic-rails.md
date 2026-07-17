# Proposal: A Governed, Self-Improving Agent Platform

## Status

Draft for review. Synthesis of the agent-rails direction. Companion decisions:
[ADR 0003 — Effective-Base Identity](adr/0003-effective-base-identity.md),
[ADR 0004 — Agent Authoring & Extensibility](adr/0004-agent-authoring-and-extensibility.md),
[ADR 0005 — Tool-Service Boundary Contract](adr/0005-tool-service-boundary-contract.md),
[ADR 0006 — Docker-Image Capability Rails](adr/0006-docker-image-capability-rails.md).

## 1. The Reason — the business case

**The problem.** Enterprises want agents that work on *their* data and their workflows, and the two available options both fail them. Frontier-API agents are powerful but generic, and using them means routing proprietary data through a vendor with weak attribution and no control over serving or training. DIY SDK agents are flexible but un-governed, un-reproducible, and — decisively — they do not improve. Everyone is prompt-engineering, and prompt-engineering plateaus.

**The thesis.** The durable advantage is a **data-and-behavior flywheel**: agents specialized on the customer's own data *and* their own run history, getting measurably better each cycle, with the improvement staying inside the tenant boundary. Every run produces a trajectory; trajectories train tenant-specific adapters; the specialized model serves the next run. More usage → better agent → more usage. Switching cost compounds because the specialization is the customer's accumulated behavior — it is not portable to a competitor.

**Why now.** Multi-LoRA serving makes per-tenant specialization economically viable (many cheap adapters over a shared base) — that is new. The market is bifurcating into generic-but-vendor-controlled frontier agents and flexible-but-ungoverned SDK agents. The open lane is a **governed, self-improving, private** agent platform for enterprises that need audit, data residency, and compounding specialization at once.

**The moat.** Three reinforcing assets an SDK or a frontier API cannot offer:
- a **proprietary trajectory→specialization pipeline** — the transform from noisy runs into safe, useful, tenant-specific training data is hard and owned;
- a **compounding per-tenant data asset** (their trajectories + data) that raises switching cost every run;
- **reproducibility and attribution** — behavior traceable to an exact spec/tool/model/data/policy tuple — which is both the trust story regulated buyers require and the substrate that makes training honest.

**The wedge.** Land in one data-sensitive vertical with design partners on the immediate value of retrieval-grounded agents over their own data; expand into specialization, which is the lock-in. The promise: *agents on your data, in your boundary, that get measurably better and that you can audit and reproduce.*

**"Measurably better" — name the metrics.** The flywheel claim is only credible if it moves numbers a buyer recognizes:
- holdout task success rate
- grounded-answer rate and citation accuracy
- tool success rate
- human acceptance rate
- escalation reduction
- cost / latency at equal quality

**Framing on frontier APIs (defensible form).** Frontier APIs can be powerful, but enterprises often cannot accept vendor-controlled data flow, weak attribution, or limited serving/training control. BigHill's answer is ownership of the loop, not a claim that frontier models are weak.

**What we are not.** Not an embeddable SDK, not a generic framework, not a container-hosting company. A governed, self-improving agent **platform**. One clarification so extensibility does not sound artificially limited: *BigHill may host customer code as governed capabilities, including Docker images, but not as the agent runtime itself.*

## 2. The Architecture — reasoned out

The business case dictates the architecture. To own the flywheel you must own serving, training, and the trajectory record. To sell to regulated buyers you must be governed and reproducible. To let developers extend without breaking either, custom logic must be a governed capability, not part of the runtime. That forces five invariants:

1. **The agent runtime is closed to code injection.** Capabilities enter only through typed ports; untrusted code never runs in the core loop.
2. **The agent artifact is declarative and content-addressed.** Reproducibility, policy-before-runtime, and attribution require the deployable spec to be a validated, immutable manifest. Power lives in the capabilities it references, not embedded code.
3. **Every capability is governed:** identity + version + schema + isolation + policy + tracing + audit + platform-managed credentials.
4. **Remove-or-wire honesty:** nothing ships without a producer and a consumer; no fake defaults; perishable provenance captured at the source.
5. **Isolation matched to risk:** read-only reads in-process; world-acting tools sandboxed (`tool_service`); side-effecting work durable + approved (Temporal); untrusted code in a rented or hosted sandbox.

**The shape that follows:**
- **Control plane (declarative):** content-addressed agent specs; a capability catalog (tools/MCP/memory/policy/sub-agents — versioned, tenant-granted, credential-bound); content-addressed effective-base (served-artifact) identity.
- **Data plane (runtime):** a small agent loop calling typed ports — `GenerationAdapter` (model_serving/vLLM/Multi-LoRA), `ToolInvoker` (local RAG + `tool_service`/MCP boundary), `MemoryPort` (memory_service), `PolicyPort` (policy_service), `DurableActivityPort` (Temporal), `EventPublisher` (socket_service).
- **Data substrate:** multi-source ingestion → materialization (embeddings + graph) → **versioned snapshots** → retrieval (RAG + GraphRAG) as grounding.
- **The flywheel:** run → provenance-attributed trajectory → label/eval → trajectory-shaped training data → tenant adapter over the effective base → promotion gate on holdout → champion → serving binding → next run.

**The spine.** Every run must be attributable to a **provenance tuple**: `{agent_spec_hash, toolset_hash, effective_base_id, data_snapshot_set, policy_set_hash, credential_binding_ids, schema_hashes, runtime/host versions}`. That tuple makes runs reproducible, behavior attributable, training honest, and promotion trustworthy. It is the load-bearing element — but it **grows only as producers exist** (see §3): capture the perishable, producible parts now; add the rest with the slice that produces them. Adding empty tuple columns ahead of their producers repeats the forward-schema mistake.

**Shared boundary policy objects.** `http_get` is the *reference implementation* of a hardened world-acting tool, not the boundary itself. MCP, SQL, browser, and container tools must not each reinvent partial safety rules; they inherit common policy objects — egress policy, timeout policy, response cap, credential policy, audit policy, event policy, schema policy (see ADR 0005). That gives reusable rails without pretending the whole control plane exists today.

**Why not the SDK model.** An embeddable SDK forfeits governance, tenant isolation, reproducibility, and — fatally — the ability to attribute and improve. The ports/platform model gives customers real extensibility (custom tools via MCP or hosted containers, behind the sandbox) without surrendering the loop, and it is the only shape in which the flywheel is possible.

## 3. Built / Partial / Missing (honest accounting)

**Built foundation** (hard to retrofit, mostly right):
- managed interactive agent loop
- content-addressed agent specs
- trajectories (with model + tool provenance)
- `tool_service` boundary skeleton (hardened `http_get` executor)
- socket_service + shared_lib/userevents
- Temporal
- service-owned Postgres state
- RLS multi-tenancy

**Partial — must harden:**
- `tool_service` egress hardening: **built for `http_get`** (exact-host allowlist, blocked private/loopback/link-local/metadata + CGNAT/NAT64, redirects disabled, dial-pin defeating DNS rebinding, response byte cap, timeout, proxy disabled). Gap: it is per-executor, not a shared boundary contract every tool kind inherits.
- `tool_service` boundary audit: **exists but log-based** (`LogInvocationAuditRepository`) — must become durable/queryable.
- systematic user-visible failure events (coverage, see §4.3).
- effective-base / run provenance wiring (tuple incomplete on the run).
- Multi-LoRA production serving path.
- GraphRAG extraction honesty (currently a regex stub with fabricated `prompt_version`).

**Missing moat (the flywheel middle):**
- `agent_registry_service` — version/champion state and the **champion→serving edge**
- golden tasks — customer-specific holdout/eval set
- eval runner — measure lift before promotion
- trajectory labeler — human/model/evaluator labels
- **trajectory-shaped dataset builder** — the proprietary transform (agent training data has tool calls, tool results, intermediate reasoning, failures, and policy decisions; this is a *new* builder, not an extension of the single-turn RAG/DPO path)
- adapter training for agent traces (reuse training_service/Ray/Axolotl where possible)
- promotion gate — holdout, no-regression, rollback
- serving compatibility — adapter binds only if base/spec/toolset match

**Missing extensibility control plane:**
- tool_catalog_service
- MCP bridge in tool_service
- capability manifests + versioning
- tenant grants
- credential bindings
- hosted container runtime (ADR 0006)
- capability SDK/CLI

## 4. Corrected P0s (blockers, not polish)

**4.1 Generalize the tool boundary — do not rebuild it.** `http_get` hardening is substantially done. The P0 is to lift it into a **shared boundary contract** (ADR 0005) that every future tool kind (MCP, SQL, browser, container) inherits, and to replace the log-based audit with a **durable/queryable** store. Rebuilding the egress pipeline would waste effort; the work is to generalize, enforce, and persist what exists.

**4.2 GraphRAG extraction must be honest.** Either wire real model-serving extraction with real prompt/model provenance, or relabel it as heuristic extraction and gate it separately from model extraction. A green `READY` state must not hide fabricated provenance.

**4.3 Failure conditions must be surfaced systematically.** Every user-relevant async failure needs a user event through socket_service — model-serving failed, chat template unusable, materialization failed, embedding failed, tool denied/timeout/egress-blocked, agent run failed, policy blocked, approval required/rejected. This is product, not observability.

**4.4 Complete the provenance tuple now — incrementally.** Pin now (real producers, perishable): `agent_spec_hash`, `toolset_hash`, `effective_base_id`, `data_snapshot_set`, known model/runtime ids. Add later, with the slice that produces them: `policy_set_hash`, `credential_binding_ids`, MCP `schema_hashes`, catalog ids. Do not add fake tuple fields ahead of producers.

## 5. Sequencing

1. **Fix honesty gaps:** GraphRAG extractor truth; Phase A/B closeout (dedup, stale-run reaper, idempotency, rate-limit, timeout envelope).
2. **Pin perishable provenance now:** `effective_base_id`, `data_snapshot_set`, and other fields with real producers.
3. **Generalize `tool_service` hardening** into a shared boundary layer; add durable audit; confirm failure-event coverage.
4. **Add one config-pinned MCP tool** for the design partner: config-pinned MCP server → hardened/audited `tool_service` boundary → schema validation → failure events. One tenant, one integration, one Secrets-Manager-backed secret. **No marketplace, no generic grants, no credential-binding system yet.**
5. **Prove one real flywheel turn:** trajectory → label/eval → train → promote → champion → serve → **measured holdout lift** on the design-partner dataset.
6. **Then** build catalog / grants / credentials / hosted containers / memory / policy / approval.
7. **Then** scale SaaS data-source breadth.

The design-partner proof must not wait for the full governed capability platform — it needs one real, hardened integration. The catalog/control-plane is a scaling mechanism; the flywheel proof is the company-defining milestone.

## 6. Funding milestone — "rails figured out"

> One real design-partner dataset produces **measured lift** through the full loop: trajectory → label/eval → train → promote → champion → serve → holdout improvement.

Before that milestone is credible, the platform must have:
- honest GraphRAG/materialization state
- a complete (producer-backed) provenance tuple
- the generalized tool boundary + durable audit
- systematic user-visible failure events
- the champion→serving edge

That is the real "rails figured out" gate. Everything after it — catalog, containers, memory, policy, and SaaS breadth — compounds a loop that has been proven to produce lift.
