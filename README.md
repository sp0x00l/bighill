# BigHill

BigHill is a self-hosted platform for governed RAG, tool-using agents, evaluation, fine-tuning, and serving. It is built as production-style infrastructure: service-owned Postgres databases, Kafka events, Temporal workflows, Ray/Axolotl training jobs, vLLM/Multi-LoRA serving, and a guarded tool execution boundary.

The short version: upload company data, materialize it into retrieval indexes, publish RAG or agent endpoints, let agents use approved tools, capture every trajectory, evaluate against holdout tasks, train adapters, promote only when they beat the champion, and serve the improved model.

## Why This Exists

Most LLM systems are easy to prototype and hard to operate. The hard parts are tenancy, audit, reproducibility, evaluation, safe tool access, retraining, promotion, and rollback. BigHill is an end-to-end implementation of those rails.

It is useful for real business cases such as:

- Customer support agents grounded in a company's documents and SaaS systems.
- Internal knowledge assistants with auditable retrieval and tool use.
- Regulated workflows where every prompt, tool call, dataset snapshot, model version, and decision must be traceable.
- Design-partner loops where production trajectories become labeled training data and better tenant-specific adapters.

It is also a portfolio project: the repo shows distributed Go services, ML platform architecture, workflow orchestration, event-driven consistency, Kubernetes serving, and practical agent safety controls in one codebase.

## What Works

- **Data ingestion and materialization:** datasets, uploads, snapshots, chunking, embeddings, pgvector retrieval, and model-extracted graph retrieval.
- **RAG inference:** multi-dataset endpoint retrieval, reranking/merge strategy, generation, request audit, and feedback capture.
- **Agent runtime:** content-addressed agent specs, async Temporal-backed agent runs, budgets/deadlines, tool calls, trajectory recording, and WebSocket/user-event progress.
- **Tool execution:** `tool_execution_service` runs tools behind tenant grants, schema validation, hardened egress, response caps, timeouts, credentials, and durable audit.
- **Tool catalog:** `tool_catalog_service` owns capability manifests, tenant grants, credential bindings, and projects execution-ready state into `tool_execution_service`.
- **Model lifecycle:** training workflows, model registry, promotion reports, serving reconciliation, effective-base identity, and Multi-LoRA adapter serving.
- **Agent flywheel rails:** `agent_registry_service` owns spec championing, golden tasks, eval reports, trajectory labels, trajectory-shaped datasets, adapter training dispatch, promotion gates, and champion adapter binding.

## Architecture

```text
data sources
  -> data_registry_service
  -> ingestion_service
  -> feature_materializer_service
  -> inference_service
  -> agent trajectories
  -> agent_registry_service
  -> training_service
  -> model_registry_service
  -> model_serving_service
  -> inference_service
```

Control-plane services decide what should exist. Data-plane services execute locally from projected state:

- `model_registry_service` decides model state; `model_serving_service` serves it.
- `agent_registry_service` decides champion specs/adapters; `inference_service` reads local endpoint bindings at run time.
- `tool_catalog_service` owns manifests/grants/credentials; `tool_execution_service` executes from local projections.

That split is deliberate. A registry outage must not break an already-bound inference or tool invocation path.

## Agent Rails

Agents are declarative. An agent spec names its lineage, model, tools, and budgets. The spec is validated, canonicalized, content-addressed, and stored as an immutable artifact.

Agent runs are asynchronous and durable through Temporal:

1. `POST /generations` for an agent endpoint returns `202` with an `agent_run_id`.
2. The workflow prepares the run, pins the spec/model/toolset/data snapshot tuple, generates a step, records it, invokes tools as separate activities, and completes or fails.
3. WebSockets stream progress, but `GET /agent-runs/{id}` is the source of truth.

The trajectory stores the provenance that matters for evaluation and training:

- `agent_spec_hash`
- `toolset_hash`
- `effective_base_id`
- `data_snapshot_hash`
- step generation results
- tool arguments/results/error types

No field is treated as real unless a current producer writes it and a current reader uses it.

## Tool Governance

`tool_execution_service` is the execution boundary. It currently supports `http_get` and MCP tools. All executor kinds inherit the same controls:

- tenant grant lookup
- JSON Schema argument validation
- exact egress host allowlist
- private/loopback/link-local/metadata/CGNAT/NAT64 blocking
- dial-time re-resolution to prevent DNS rebinding
- redirect blocking where required
- timeout and response-size caps
- credential injection from configured bindings
- durable audit rows

`tool_catalog_service` is the control plane. It publishes real capability manifests, grants them to tenants, binds credentials, and emits events into the execution service. The execution service does not call the catalog on the hot path.

Coming next: selected LangChain and LlamaIndex components can be wrapped as governed MCP/tool-execution capabilities. The frameworks provide useful components; BigHill provides tenancy, provenance, policy, and audit around them.

## Flywheel

The agent improvement loop is designed to be measurable, not aspirational:

1. Production runs create trajectories.
2. Trajectories are labeled by humans, rules, or model evaluators.
3. Golden tasks protect a promotion holdout from leaking into training.
4. A trajectory-shaped dataset is built from eligible labels.
5. Training produces a real LoRA adapter.
6. Eval runs the candidate adapter and champion against the same holdout.
7. Promotion updates the champion only when the candidate beats the gate.
8. Inference serves the next run with the promoted adapter.

Unit and service tests can inject deterministic trainers or mock MCP servers as test doubles. Normal run profiles, including local-dev, use the real dependent services so local behavior stays compatible with staging and production.

## Repository Map

| Path | Purpose |
| --- | --- |
| `api_gateway/` | Edge routing, auth integration, API e2e tests |
| `data_contracts/` | Protobuf and JSON Schema contracts |
| `data_registry_service/` | Dataset/source metadata and materialization state |
| `ingestion_service/` | Upload sessions and raw artifact landing |
| `feature_materializer_service/` | Snapshots, chunking, embeddings, graph materialization/search |
| `data_stream_service/` | Arrow Flight and DataFusion query boundary |
| `inference_service/` | RAG, agent runtime, trajectories, feedback, preference datasets |
| `tool_execution_service/` | Governed tool execution boundary |
| `tool_catalog_service/` | Tool manifests, tenant grants, credential bindings |
| `agent_registry_service/` | Agent champion/eval/training control plane |
| `training_service/` | Temporal/Ray/Axolotl training workflows |
| `model_registry_service/` | Model records, promotion gates, serving intent |
| `model_serving_service/` | Local/Kubernetes model serving reconciliation |
| `socket_service/` | User-visible WebSocket event delivery |
| `shared_lib/` | Common DB, outbox, messaging, auth, lifecycle, tracing |
| `docs/adr/` | Architecture decisions |

## Quick Start

```bash
make install
make start-infra
make build-all
make test
```

Useful narrower targets:

```bash
make start-test
cd api_gateway && make test
make test-api-data-sources
make k8s-validate
```

The datasource API tests run against the datasource compose stack. `make test` includes the service suites and API workflow tests; `make test-api-data-sources` focuses on external datasource coverage.

## Main Technologies

- Go for services, handlers, control-plane logic, and infrastructure adapters.
- Python for GPU/batch jobs and model artifact utilities.
- Postgres, pgvector, RLS, and transactional outbox for service-owned state.
- Kafka for cross-service events.
- Temporal for durable materialization, training, and agent workflows.
- Ray/KubeRay and Axolotl-style recipes for SFT/DPO/LoRA training.
- vLLM and Ollama-compatible local paths for generation.
- Kubernetes/Helm/Terraform for deployment.
- Ginkgo/Gomega for service and integration tests.

## Design Docs

- [ADR 0001: Open Lakehouse Query Stack](docs/adr/0001-open-lakehouse-query-stack.md)
- [ADR 0002: Temporal and Event Delivery Boundaries](docs/adr/0002-temporal-and-event-delivery-boundaries.md)
- [ADR 0003: Effective-Base Identity](docs/adr/0003-effective-base-identity.md)
- [ADR 0004: Agent Authoring and Extensibility](docs/adr/0004-agent-authoring-and-extensibility.md)
- [ADR 0005: Tool Execution Boundary](docs/adr/0005-tool-execution-service-boundary-contract.md)
- [ADR 0006: Docker Image Capability Rails](docs/adr/0006-docker-image-capability-rails.md)
- [ADR 0008: Agent Registry Flywheel Control Plane](docs/adr/0008-agent-registry-flywheel-control-plane.md)
- [Agent Tool Plane](docs/agent-tool-plane.md)

## Status

BigHill is a serious work-in-progress, not a polished SaaS product. The repo is meant to demonstrate the platform rails needed for production LLM systems: RAG, agent tools, evaluation, training, serving, provenance, and governed extensibility. The next push is breadth: more SaaS connectors and more governed framework components exposed through MCP/tool execution.
