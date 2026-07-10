<img align="right" width="200" src="logo.png" alt="BigHill logo" />

# BigHill

**A self-hosted platform for building RAG and fine-tuned LLM systems.**

BigHill is a set of Go microservices tied together with Temporal workflows, Kafka events, per-service
Postgres databases, pgvector for retrieval, Ray/KubeRay for training jobs, and Kubernetes-backed vLLM
serving. Python is confined to the GPU batch jobs. It owns the whole lifecycle — data, models,
inference, feedback, and retraining.

---

## Why BigHill?

An LLM project usually begins as a notebook or a single Python app, then stalls the moment it has to
be real — more than one tenant, an audit trail, a way to retrain on feedback, a gate that stops a
worse model from shipping, and serving that survives a restart. Retrofitting those is the expensive
part. BigHill gives you the spine on day one:

- **The whole lifecycle in one system** — ingestion, retrieval, training, evaluation, promotion,
  serving, and a feedback → DPO retrain loop wired end to end.
- **Experimentation beyond notebooks** — POST a training run (dataset + source model + profiles) and
  get back an evaluated, gated model instead of hand-writing training, eval, and serving glue.
  Content-addressed snapshots skip repeated work, and every run is reproducible and comparable against
  its champion.
- **Correct by construction** — service-owned Postgres, a transactional outbox so an event never
  outruns its state, and a promotion gate that won't serve a model that regresses against its
  champion.
- **Multi-tenant and auditable from the start** — every dataset, embedding, prompt, response, and
  model version is attributable to a tenant.
- **Laptop to Kubernetes on one contract** — the same provider contract across local-dev, CI,
  staging, and prod.

It's a strong starting point when what you're building *is* a RAG or fine-tuning platform. It's the
wrong one for a quick chatbot or for non-LLM ML — see [When to use it](#when-to-use-it).

---

## The shape of it

Data flows through the platform roughly like this:

```
data ─▶ registry ─▶ ingestion ─▶ feature materialization ─▶ embeddings
     ─▶ training / evaluation ─▶ model registry ─▶ serving ─▶ inference
     ─▶ feedback ─▶ preference datasets ─▶ DPO / retrain
```

The design follows the **FTI split — Feature, Training, Inference** — as an event-driven platform:
each service owns its own database, events cross between services, and long-running work runs as
durable Temporal workflows. Kubernetes, Ray, and vLLM handle the ML runtime; the heavy Python/GPU work
stays in batch jobs behind clean boundaries.

> **Maturity:** the core lifecycle is implemented and covered by end-to-end tests — ingestion,
> materialization, training, champion/challenger promotion, serving, RAG inference, and the
> feedback-driven DPO retrain loop all run. The areas still maturing are **multi-LoRA serving**,
> **deeper evaluation** (Ragas/DeepEval, golden sets, drift), and the **full lakehouse path**
> (Iceberg / Polaris / Nessie). See [Where it's headed](#where-its-headed).

---

## What it does

- Registers **datasets and their sources**.
- Ingests **dataset files and model artifacts** through presigned upload sessions; data files include
  PDF, HTML, Markdown, text, JSON, CSV, and Parquet, with format detection and validation.
- Extracts and chunks documents, including PDF extraction via `pdf_extractor_lib`.
- Builds **feature and embedding snapshots**, content-addressed so re-runs don't duplicate work.
- Stores and searches vectors with **pgvector** (vectors and metadata live together in Postgres).
- Serves **RAG inference** across **one or more datasets per endpoint**, with a configurable merge
  strategy (`reranker` or `score_normalized`): retrieval, reranking, query rewriting, prompt packing,
  generation, and a full audit trail of every request.
- Captures **feedback** and turns it into **preference datasets** for alignment.
- Runs **SFT and DPO training** on Ray/KubeRay using Axolotl-style recipes.
- **Evaluates models and gates promotion** through the model registry (absolute floors +
  no-regression-vs-champion + evidence).
- **Reconciles serving** through a Kubernetes serving layer to vLLM.
- Uses **Kafka events** and a **Postgres outbox** to keep services consistent without coupling them.
- Ships with **local dev, Docker Compose, Helm, VS Code wiring, and end-to-end tests**.

---

## How it's put together

Two short design docs cover the load-bearing choices — read these first:

- **[ADR-0001 — Open Lakehouse Query Stack](docs/adr/0001-open-lakehouse-query-stack.md):** Go owns the
  APIs, metadata, orchestration, events, and observability. The data side uses a vendor-neutral
  registry and an Arrow/DataFusion query boundary, so Python never becomes the control plane.
- **[ADR-0002 — Temporal and Event Delivery](docs/adr/0002-temporal-and-event-delivery-boundaries.md):**
  services that own data publish events through a **Postgres outbox** in the same transaction as the
  write, so an event never exists without the state behind it. Training workflows publish from
  Temporal activities. Consumers are built to handle duplicates.

The recurring discipline: **Postgres for each service's state, Kafka for events between services,
Temporal for durable workflows, and Kubernetes/Ray/vLLM for the ML runtime.** Every service uses the
same hexagonal layering — `pkg/domain` (the model), `pkg/app` (the logic and its interfaces),
`pkg/infra` (the adapters: DB, messaging, network) — and ships its own Helm chart.

---

## Runtime environments

Services read the runtime mode from `ENVIRONMENT`. The repo-standard values are `local-dev`, `cicd`,
`staging`, and `prod` (trimmed and uppercased internally before comparison). Unset or unknown values
are **not** treated as dev and fail closed. `env.IsDevEnv()` is true only for `local-dev` and `cicd`,
so staging and prod share the same fail-closed behavior.

| Environment | `IsDevEnv()` | Runtime provider contract | Storage policy | Failure behavior |
|-------------|--------------|---------------------------|----------------|------------------|
| `local-dev` | true | Local-compatible generation, TEI-compatible embeddings/reranking | `local-dev-bucket` allowed | Missing provider names/endpoints/models fail startup |
| `cicd` | true | Same contracts as local, backed by test-owned protocol-compatible services | Test-local buckets allowed | Same fail-closed startup policy |
| `staging` | false | Production-parity generation, embeddings, reranking | `local-dev-bucket` rejected | Same fail-closed startup policy |
| `prod` | false | Production generation, embeddings, reranking | `local-dev-bucket` rejected | Same fail-closed startup policy |

The provider contract is identical across environments: model records carry `serving_protocol`,
`serving_target`, and `serving_model`, and `inference_service` dispatches to the matching generation
adapter from that recorded state. What changes by environment is how that serving state is produced:

| Orchestrated path | `local-dev` / `cicd` service scripts | `staging` / `prod` |
|-------------------|--------------------------------------|--------------------|
| Model serving reconciliation | Local served-model store; no Kubernetes/vLLM required | Kubernetes `ServedModel` CRD, vLLM Deployment/Service reconciliation |
| Model registry serving backend | Local served-model backend | Kubernetes `ServedModel` backend |
| Training execution | Direct Ray Jobs API | KubeRay job creation from Temporal activities |
| Hugging Face model downloads | Local command execution | Kubernetes Job execution |

Two operational limits are worth calling out:

- `INFERENCE_SERVICE_GENERATION_MAX_OUTPUT_TOKENS` is the hard output cap. Local/CI use `24` to keep
  CPU-bound Ollama e2e calls inside the generation timeout; staging/prod use `256`. Raise it
  deliberately only for workloads that need longer answers.
- `INFERENCE_SERVICE_HTTP_WRITE_TIMEOUT_SECONDS` **must exceed**
  `INFERENCE_SERVICE_GENERATION_REQUEST_TIMEOUT_SECONDS` — otherwise the gateway sees an EOF even
  though generation finishes in the inference handler.

The API e2e suite is split for local capacity. `make test-api` runs the core gateway/service workflows
without loading the external datasource fixtures. Run `make test-api-data-sources` when you explicitly
want the datasource stack and the `external-datasource` API specs.

Serving detail (the local Ollama backend, GGUF onboarding, and the model-family / chat-template
boundary) lives in [`model_serving_service/README.md`](model_serving_service/README.md). One trap is
load-bearing enough to surface here: **model-family agnostic does not mean template-agnostic.** A base
(non-Instruct) model, or a GGUF served without its chat template, returns HTTP 200 with a fluent but
wrong answer. BigHill enforces the chat template as a **provisioning precondition** (not an output
assertion) so these fail closed; when onboarding GGUF for chat inference, use the **Instruct** build.

---

## End-to-end flow

This is the main path from a user logging in to a self-improving model serving inference. Solid
arrows (`──▶`) are **synchronous** calls (HTTP through the gateway, gRPC, Temporal activities);
dashed arrows (`⤍`) are **asynchronous events** delivered over Kafka via each service's Postgres
outbox. Event names are the real message types from `data_contracts` / `shared_lib/messaging`.

```mermaid
sequenceDiagram
    autonumber
    actor U as User / Client
    participant GW as api_gateway
    participant PR as tenant_service
    participant IN as ingestion_service
    participant DR as data_registry
    participant FM as feature_materializer
    participant TR as training_service
    participant RAY as Ray / training_service/training_jobs
    participant MR as model_registry
    participant SV as model_serving
    participant IF as inference_service

    Note over U,PR: 1 · Authenticate + select org
    U->>GW: POST /public/v1/profiles/password/verify
    GW->>PR: verify credentials
    PR-->>U: access token {userId, orgId, roles, permissions}
    U->>GW: GET /v1/private/orgs/current
    GW->>PR: read active org + membership

    Note over U,PR: 2 · Org RBAC
    U->>GW: POST /v1/private/orgs/{org_id}/members
    GW->>PR: add researcher / consumer membership
    PR-->>U: member role + status

    Note over U,FM: 3 · Ingest & materialize data
    U->>GW: POST /v1/private/data/registry
    GW->>DR: create dataset / source
    U->>GW: POST /v1/private/data/uploads/{dataset_id}
    GW->>IN: open presigned upload session
    U->>GW: upload files (PDF / CSV / Parquet / …)
    IN-->>FM: raw_snapshot_ready
    FM->>FM: extract · chunk · embed → pgvector
    FM-->>DR: feature_snapshot_ready / embedding_snapshot_ready
    DR-->>TR: dataset_updated (FEATURE_MATERIALIZED)

    Note over U,MR: 4 · Onboard / select a base model
    U->>GW: POST /v1/private/models/uploads or /v1/private/models/onboard/huggingface
    GW->>IN: model artifact upload
    IN-->>MR: model_artifact_ingested
    U->>GW: GET /v1/private/models
    GW->>MR: list trainable models

    Note over U,RAY: 5 · Trigger training (researcher permission)
    U->>GW: POST /v1/private/training-runs {dataset_id, source_model_id, profiles}
    GW->>TR: start training run
    TR->>DR: resolve materialized dataset
    TR->>MR: resolve trainable source model
    TR-->>U: 202 {training_run_id, status_url}

    Note over TR,RAY: 6 · Train → evaluate (Temporal workflow)
    TR->>RAY: prepare data · train (Axolotl) · evaluate
    RAY-->>TR: artifact + evaluation report
    TR-->>MR: model_training_completed

    Note over MR,RAY: 7 · Promotion gate
    MR->>MR: register CANDIDATE
    MR-->>TR: promotion_requested (candidate + champion)
    TR->>RAY: promotion_report (Deepchecks / Evidently)
    RAY-->>TR: report
    TR-->>MR: promotion_report_ready
    MR->>MR: gate — floors · champion/challenger · evidence
    alt candidate wins
        MR->>MR: status EVALUATED + serving intent
    else candidate loses
        MR->>MR: status FAILED
    end

    Note over MR,SV: 8 · Serve
    MR-->>SV: model_updated (serving intent)
    SV->>SV: reconcile vLLM deployment
    SV-->>MR: serving status → READY / LOADED (champion)
    MR-->>IF: model_updated (ready model)

    Note over U,RAY: 9 · Consumer inference + feedback
    U->>GW: GET /v1/private/inference/endpoints
    GW->>IF: list safe endpoint projection
    U->>GW: POST /v1/private/inference/endpoints/{endpoint_id}/generations
    GW->>IF: generate from endpoint (trusted X-User-ID / X-Org-ID)
    IF->>FM: vector search across endpoint datasets (pgvector)
    IF->>SV: generate (vLLM)
    IF-->>U: answer + full audit trail
    U->>GW: POST /v1/private/inference/feedback
    GW->>IF: feedback (rating / preference)
    IF-->>TR: preference_dataset_ready
    TR->>RAY: DPO retrain → re-enter step 6
```

**Walking the flow:**

1. **Authenticate + select org.** Everything enters through `api_gateway`, which delegates auth to
   `tenant_service`. Tokens carry the active `orgId`, role, and derived permissions. The gateway
   injects trusted `X-User-ID` / `X-Org-ID` headers and rejects spoofed inbound identity headers.
2. **Org RBAC.** Organization membership is managed through `GET /v1/private/orgs/current`,
   `GET /v1/private/orgs/{org_id}/members`, `POST /v1/private/orgs/{org_id}/members`,
   `PUT /v1/private/orgs/{org_id}/members/{user_id}`, and
   `DELETE /v1/private/orgs/{org_id}/members/{user_id}`. `ml_researcher` can upload data/models
   and start training; `consumer` can only list/invoke inference endpoints and submit feedback.
3. **Ingest & materialize.** A dataset and its source are registered in `data_registry`;
   `ingestion_service` hands back a presigned upload session and lands the raw files.
   `feature_materializer` picks up `raw_snapshot_ready`, extracts and chunks documents, embeds them,
   and writes vectors to **pgvector** — content-addressed, so re-runs don't duplicate work. When the
   snapshot is materialized, `data_registry` publishes `dataset_updated`.
4. **Onboard / select a base model.** Base models are uploaded through `ingestion_service` and
   registered in `model_registry` (`model_artifact_ingested`); trainable models are listed via
   `GET /v1/private/models` (org-scoped: shared bases plus your own).
5. **Trigger training.** Training is **user intent, not a hidden default** — a researcher POSTs the
   dataset, the source model, and named profiles to `POST /v1/private/training-runs`.
   `training_service` resolves the materialized dataset and the trainable source model, validates
   them, and returns a `training_run_id` immediately. The base model is carried as *data*, never as
   service config.
6. **Train → evaluate.** A durable Temporal workflow prepares the data, runs the GPU training job on
   Ray (`training_service/training_jobs`, Axolotl-style recipes), evaluates the result, and emits
   `model_training_completed`.
7. **Promotion gate.** `model_registry` records the new model as a **CANDIDATE** — it is *not* served
   yet — and asks `training_service` for a promotion report (Deepchecks / Evidently). The gate then
   compares the candidate against the current **champion** for that lineage (absolute floors +
   no-regression + evidence), promoting to `EVALUATED` or rejecting to `FAILED`. This is the guard
   that keeps the retrain loop safe.
8. **Serve.** On promotion, `model_registry` records serving intent; `model_serving` reconciles a
   vLLM deployment and reports back until the model is `READY / LOADED` — at which point it becomes
   the new champion. Inference learns the ready model via `model_updated`.
9. **Consumer inference + feedback loop.** Consumers list safe endpoint projections with
   `GET /v1/private/inference/endpoints` and invoke with
   `POST /v1/private/inference/endpoints/{endpoint_id}/generations`. Request bodies carry query
   controls only; `user_id`, `org_id`, `model_id`, and the endpoint's **dataset set** and **merge
   strategy** are resolved from the trusted token/header context and the published endpoint —
   retrieval fans out across every ready dataset and merges the results. Endpoints are published and
   configured explicitly with `POST /v1/private/inference/endpoints`,
   `PUT /v1/private/inference/endpoints/{endpoint_id}/datasets`, and
   `PUT /v1/private/inference/endpoints/{endpoint_id}/merge-strategy`. Captured feedback becomes a
   preference dataset; `preference_dataset_ready` starts a **DPO retrain** (the source model resolved
   from lineage, not config), which re-enters step 6 and closes the loop.

---

## What's in the repo

| Path | What it does |
|------|--------------|
| `data_registry_service/` | Dataset and source metadata |
| `ingestion_service/` | Presigned upload sessions, file validation, raw data landing, model artifact landing |
| `pdf_extractor_lib/` | PDF text/structure extraction |
| `feature_materializer_service/` | Snapshots, chunking, embeddings, pgvector search |
| `data_stream_service/` | Arrow Flight query gateway + DataFusion executor (`internal/`) |
| `training_service/` | Temporal training workflows (SFT/DPO), Ray/KubeRay dispatch |
| `training_service/training_jobs/` | Python GPU jobs (Axolotl train, evaluation) run by Ray |
| `model_registry_service/` | Model records, promotion gating, serving intent + status, outbox |
| `model_serving_service/` | K8s operator that reconciles serving to vLLM; `localserving` for dev |
| `inference_service/` | RAG inference, retrieval/rerank/query-rewrite, generation, auditing, feedback |
| `tenant_service/` | Auth (OAuth / password) and user profiles |
| `api_gateway/` | Edge (Lambda auth/api) and end-to-end API tests |
| `data_contracts/` | Protobuf event and service contracts |
| `shared_lib/` | Shared plumbing: messaging, outbox, DB, metrics/tracing, auth, object storage, K8s client |
| `infra/`, `database/`, `scripts/` | Infra manifests, DB, tooling |
| `docs/adr/` | Design docs |

---

## Getting started

You'll need Go, Docker, and — for the ML runtime — access to Kubernetes / Ray / GPUs. Most things run
from the root `Makefile`:

```bash
make install-dev      # install dev dependencies
make start-infra      # start local infra (Kafka, Postgres, object storage, …)
make start-servers    # start the Go services
make test             # run the tests
make stop             # tear it all down
```

- **Local dev:** Makefile targets + Docker Compose, with VS Code launch configs.
- **Kubernetes:** each service has a Helm chart under `<service>/helm/`.
- **Query engine:** `make build-query-engine` / `make test-query-engine` build the DataFusion executor
  behind the data-stream query boundary.

---

## When to use it

BigHill fits when you want to run an LLM platform yourself and care about repeatable
data-to-model pipelines, auditability (datasets, embeddings, prompts, responses, feedback, model
versions), independently scaling services, event boundaries instead of shared databases, multi-tenant
model and adapter lifecycles, and RAG + fine-tuning + feedback-driven improvement in one system, with
a path from a laptop to Kubernetes.

It's **not** the right tool for a quick chatbot — LangChain, LlamaIndex, Haystack, or a managed RAG
service will get you there faster. BigHill is heavier on purpose: it's meant to sit *under* many
systems rather than be one. Compared to managed platforms (SageMaker / Vertex / Azure ML) it trades
convenience for control and self-hosting; compared to application frameworks it owns far more of the
lifecycle (ingestion, registry, workflows, serving state, promotion, feedback, training).

---

## Where it's headed

The core serve → feedback → preference data → DPO → eval → promote → serve loop is wired end to end.
The next work is depth and reliability, not first light:

- Make the **DPO / feedback loop boringly reliable** and proven, including held-out train/eval splits
  so a new model is only promoted when it actually beats the one it came from.
- **Better evaluation:** Ragas / DeepEval, pairwise preference eval, golden sets, drift checks.
- **Mature multi-LoRA serving** so one base model can serve many tenant adapters cheaply.
- **Better RAG:** structure-aware chunking, hybrid BM25 + vector search, self-querying, HyDE, query
  expansion.
- **Push the lakehouse path:** Iceberg, Polaris / Nessie, DataFusion, Arrow Flight.
- **Product surfaces:** lineage UI, feedback review, eval dashboards, deployment status, tenant controls.
- **Harden the Kubernetes controller**, leaning more on standard controller-runtime / KServe / KubeRay
  patterns.

---

## Bottom line

BigHill is a self-hosted platform for RAG, fine-tuning, evaluation, serving, and feedback-driven
improvement, built on service-owned state, a Postgres outbox, Temporal, Kafka, and explicit contracts.
It takes more to run than a lightweight framework and is less polished than the big commercial
platforms, but it owns the full lifecycle of data, models, inference, feedback, and retraining in one
system that's meant to be extended.
