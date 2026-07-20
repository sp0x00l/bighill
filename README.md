# BigHill

BigHill is a self-hosted platform for building, running, evaluating, improving, and serving governed AI agents.

The repo implements the full operating loop:

- ingest tenant data and build versioned retrieval and graph snapshots;
- publish RAG endpoints or declarative agent specs;
- run agents through Temporal with budgets, tool calls, progress events, and durable state;
- govern tools through a catalog, tenant grants, schemas, credentials, egress policy, and audit;
- record trajectories with the exact spec, tools, data snapshots, base model, and adapter used;
- label runs, evaluate against golden tasks, train LoRA adapters, and promote only passing candidates;
- serve promoted models through vLLM/Multi-LoRA in production-style infrastructure.

The goal is concrete: a company should be able to see what an agent did, why it was allowed to do it, which artifacts produced the behavior, whether it met the business standard, and what changed before a new version was promoted.

## What It Delivers

- **Governed RAG and agents:** publish retrieval endpoints or declarative agent specs backed by company data.
- **Safe tool use:** agents call approved tools through schema validation, tenant grants, credential injection, egress policy, timeouts, response caps, and durable audit.
- **Observable runs:** every agent run records steps, tool calls, failures, data snapshots, model identity, adapter identity, and user-visible status events.
- **Business-specific evaluation:** golden tasks, labels, holdouts, eval reports, and promotion gates define what "good" means for a tenant.
- **Continuous improvement:** production trajectories become training data; new adapters are promoted only when they beat the champion.
- **Efficient serving:** vLLM/Multi-LoRA serves many tenant or workflow adapters over shared base models.
- **Grounded retrieval:** ingestion, snapshots, embeddings, pgvector retrieval, reranking, and graph retrieval support auditable data use.

## The Loop

```text
company data
  -> snapshots and retrieval indexes
  -> RAG endpoints and agents
  -> trajectories, labels, and evals
  -> adapter training
  -> promotion gates
  -> champion model/spec
  -> next run
```

The run record that matters is:

```text
agent_spec_hash + toolset_hash + effective_base_id + data_snapshot_set + adapter_id
```

That tuple makes behavior attributable. It lets the platform answer which spec, tools, data, base model, and adapter produced a result.

## Core Services

| Area | Services |
| --- | --- |
| Edge/API | `api_gateway`, `tenant_service` |
| Data | `data_registry_service`, `ingestion_service`, `feature_materializer_service`, `data_stream_service` |
| Inference/agents | `inference_service`, `socket_service` |
| Tools | `tool_catalog_service`, `tool_execution_service` |
| Flywheel | `agent_registry_service`, `training_service`, `model_registry_service`, `model_serving_service` |
| Shared contracts | `data_contracts`, `shared_lib` |

Registries decide what should exist. Runtime services execute from projected local state:

- `model_registry_service` decides model intent; `model_serving_service` serves it.
- `agent_registry_service` decides champion specs and adapters; `inference_service` executes runs.
- `tool_catalog_service` owns manifests, grants, and credentials; `tool_execution_service` invokes tools.

This keeps hot paths running even when a control-plane service is unavailable.

## Runtime Environments

- **Local development:** runs the service stack locally with local infrastructure. Ollama is used for local model-serving and GGUF/chat-template validation paths where appropriate.
- **CI:** exercises service contracts and local-compatible model paths without requiring production GPU infrastructure.
- **Staging/production:** Kubernetes, Helm, Terraform/OpenTofu, AWS infrastructure, and vLLM/Multi-LoRA serving. Ollama is not the production serving layer.

## Tech Stack

- **Languages:** Go for services/control plane, Python for training and artifact tooling, Rust/DataFusion for query execution, C++/Poppler for PDF extraction, SQL and shell for state and orchestration.
- **APIs:** HTTP/REST, gRPC/Protobuf, Arrow Flight, WebSockets, AWS Lambda/API Gateway, OpenAPI/SAM/CloudFormation.
- **State and events:** PostgreSQL/Aurora, pgvector, transactional outbox, Kafka, Redis, Temporal, SQS, KMS, JWT/OAuth/Argon2id.
- **ML runtime:** vLLM/Multi-LoRA for production serving, Ollama for local/CI validation, LoRA/QLoRA, Ray/KubeRay, Axolotl-style recipes, Hugging Face, TEI-compatible embeddings/reranking, Ragas, JSON Schema, tiktoken-go.
- **Model-runtime alternatives:** SGLang or TensorRT-LLM can replace the serving runtime; NVIDIA NeMo can replace parts of the training recipe stack; SLURM can replace KubeRay in HPC-style environments; MLflow can complement or replace experiment/evidence tracking.
- **Data/lakehouse:** Arrow, Arrow IPC, Parquet, Iceberg, Polaris, Nessie, OpenDAL, S3/MinIO, Postgres, MySQL, MongoDB, ClickHouse, Oracle, PDF, HTML, Markdown, text, JSON, CSV.
- **Infrastructure:** Docker, Docker Compose, Kubernetes, Helm, Terraform/OpenTofu, EKS, ECR, S3, IAM/IRSA, VPC networking, Secrets Manager, NVIDIA device plugin, AWS Load Balancer Controller, ExternalDNS, CodeArtifact.
- **Observability/testing:** OpenTelemetry/OTLP, Prometheus, Grafana, Loki, Promtail, Tempo, Logrus, Ginkgo/Gomega, testify, validator, pgx, rueidis, confluent-kafka-go.

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
make stop-services
```

## Key Docs

- [Multi-LoRA serving](docs/multi-lora-serving.md)
- [Self-improving loop](docs/self-improving-loop.md)
- [Agent tool plane](docs/agent-tool-plane.md)
- [Agentic rails](docs/agentic-rails.md)
- [Agent extension architecture](docs/agent-extension-architecture.md)
- [ADR 0003: Effective-base identity](docs/adr/0003-effective-base-identity.md)
- [ADR 0004: Agent authoring and extensibility](docs/adr/0004-agent-authoring-and-extensibility.md)
- [ADR 0005: Tool execution boundary](docs/adr/0005-tool-execution-service-boundary-contract.md)
- [ADR 0006: Docker image capability rails](docs/adr/0006-docker-image-capability-rails.md)
- [ADR 0008: Agent registry flywheel control plane](docs/adr/0008-agent-registry-flywheel-control-plane.md)

## Status

BigHill is a serious work-in-progress. The core rails are in place: governed data use, agent execution, tool control, provenance, evaluation, training, promotion, and serving. The next product push is breadth: more SaaS connectors, more governed framework components through MCP/tool execution, and more real design-partner flywheel turns.
