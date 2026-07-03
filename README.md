# BigHill

**A self-hosted platform for building RAG and fine-tuned LLM systems.**

BigHill isn't just an app — it's the platform underneath one. It's a set of Go microservices tied
together with Temporal workflows, Kafka events, per-service Postgres databases, pgvector for
retrieval, Ray/KubeRay for training jobs, and vLLM-style serving. Python shows up only where it
belongs: the GPU batch jobs.

The point isn't "build a chatbot." The point is **own the whole lifecycle** — data, models,
inference, feedback, and retraining.

---

## The shape of it

Data flows through the platform roughly like this:

```
data ─▶ registry ─▶ ingestion ─▶ feature materialization ─▶ embeddings
     ─▶ training / evaluation ─▶ model registry ─▶ serving ─▶ inference
     ─▶ feedback ─▶ preference datasets ─▶ DPO / retrain
```

The design follows the **FTI idea — Feature, Training, Inference** — but built as a real platform
instead of a single Python app: event-driven, each service owns its own database, and long-running
work runs as durable workflows. Kubernetes, Ray, and vLLM handle the ML runtime.

The key idea: **each service owns its state, events cross between services, and workflows coordinate
the slow steps.** That's a cleaner way to run things than most LLM repos manage.

> **Heads up:** this is an emerging platform. The infrastructure is solid and well-reviewed, but some
> pieces (multi-LoRA serving by default, the self-improving DPO loop, the full lakehouse path) are a
> direction we're building toward, not finished work. See [Where it's headed](#where-its-headed).

---

## What it does

- Registers **datasets and where they come from**.
- Ingests **PDF, HTML, Markdown, text, JSON, CSV, and Parquet**, with format detection and validation.
- Extracts and chunks documents, including proper PDF extraction via `pdf_extractor_lib`.
- Builds **feature and embedding snapshots**, keyed by content so re-runs don't duplicate work
  (content-addressed idempotency).
- Stores and searches vectors with **pgvector** (vectors and metadata live together in Postgres).
- Serves **RAG inference**: retrieval, reranking, query rewriting, prompt packing, generation, and a
  full audit trail of every request.
- Captures **feedback** and turns it into **preference datasets** for alignment.
- Runs **SFT and DPO training** on Ray/KubeRay using Axolotl-style recipes.
- **Evaluates models and gates promotion** through the model registry.
- **Reconciles serving** through a Kubernetes serving layer, heading toward vLLM / multi-LoRA.
- Uses **Kafka events** and a **Postgres outbox** to keep services consistent without coupling them.
- Comes with **local dev, Docker Compose, Helm, VS Code wiring, and end-to-end tests**.

---

## How it's put together

Two short design docs cover the load-bearing choices — read these first:

- **[ADR-0001 — Open Lakehouse Query Stack](docs/adr/0001-open-lakehouse-query-stack.md):** Go owns the
  APIs, metadata, orchestration, events, and observability. The data side uses a vendor-neutral
  registry and an Arrow/DataFusion query boundary, so **Python never becomes the control plane**.
- **[ADR-0002 — Temporal and Event Delivery](docs/adr/0002-temporal-and-event-delivery-boundaries.md):**
  services that own data publish events through a **Postgres outbox** in the same transaction as the
  write, so an event never exists without the state behind it. Training workflows publish from
  Temporal activities. Consumers are built to handle duplicates.

The recurring discipline: **Postgres for each service's state, Kafka for events between services,
Temporal for durable workflows, and Kubernetes/Ray/vLLM for the ML runtime.** The heavy Python/GPU
work stays in batch jobs behind clean boundaries.

---

## What's in the repo

| Path | What it does |
|------|--------------|
| `data_registry_service/` | Dataset and source metadata |
| `data_ingestion_service/` | File upload, format detection/validation, raw data landing |
| `pdf_extractor_lib/` | PDF text/structure extraction |
| `feature_materializer_service/` | Snapshots, chunking, embeddings, pgvector search |
| `data_stream_service/` | Arrow Flight query gateway + DataFusion executor (`internal/`) |
| `training_service/` | Temporal training workflows (SFT/DPO), Ray/KubeRay dispatch |
| `training_jobs/` | Python GPU jobs (Axolotl train, evaluation) run by Ray |
| `model_registry_service/` | Model records, promotion gating, serving intent + status, outbox |
| `model_serving_service/` | K8s operator that reconciles serving to vLLM; `localserving` for dev |
| `inference_service/` | RAG inference, retrieval/rerank/query-rewrite, generation, auditing, feedback |
| `profile_service/` | Auth (OAuth / password) and user profiles |
| `api_gateway/` | Edge (Lambda auth/api) and end-to-end API tests |
| `data_contracts/` | Protobuf event and service contracts |
| `shared_lib/` | Shared plumbing: messaging, outbox, DB, metrics/tracing, auth, object storage, K8s client |
| `infra/`, `database/`, `scripts/` | Infra manifests, DB, tooling |
| `docs/adr/` | Design docs |

Every service uses the same hexagonal layering (ports and adapters) — `pkg/domain` (the model),
`pkg/app` (the logic and its interfaces), `pkg/infra` (the adapters: DB, messaging, network) — and
ships its own Helm chart.

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

## Who it's for

Use BigHill if you want to **own an LLM platform** instead of gluing a prototype together. It's a good
fit when you care about:

- Repeatable **data-to-model pipelines**.
- **Auditability** — datasets, embeddings, prompts, responses, feedback, and model versions.
- **Services that scale independently**.
- **Events instead of shared databases** between components.
- **Self-hosting** instead of SaaS lock-in.
- **Multi-tenant** model and adapter lifecycles.
- **RAG + fine-tuning + feedback-driven improvement** in one system.
- A path **from your laptop to Kubernetes**.

It's **not the right tool** if you just want a quick chatbot — LangChain, LlamaIndex, Haystack, or a
managed RAG service will get you there far faster. BigHill is heavier on purpose: it's meant to sit
*under* many systems.

---

## How it compares

**LangChain / LlamaIndex / Haystack** are application frameworks. BigHill is a platform — it owns
ingestion, registry, workflows, serving state, model promotion, feedback, and training. More serious
operationally, and heavier.

**ZenML / Kubeflow / Metaflow / Dagster / Airflow** focus on orchestration or ML pipelines. BigHill
uses Temporal + Kafka/outbox wrapped in real services. Cleaner event boundaries than most pipeline
tools; less mature on UI and ecosystem.

**SageMaker / Vertex AI / Azure ML** are managed. BigHill is the self-hosted version — more control,
less lock-in, and you carry more of the ops.

**MLflow** is mostly tracking, registry, and artifacts. BigHill has registry concepts too, plus the
whole event/workflow/data/serving/inference platform around them. MLflow is more mature for experiment
tracking; this is broader.

**Qdrant / Pinecone / Weaviate** are vector databases. BigHill uses pgvector, which keeps vectors and
metadata in one Postgres for easy consistency. It won't beat a dedicated vector DB on every feature or
on scale, but it's pragmatic and easy to reason about.

**KServe / Seldon / BentoML / Ray Serve** are serving platforms. BigHill is growing its own serving
reconciliation around vLLM / multi-LoRA. The direction is right; the mature platforms have more
battle-tested autoscaling, routing, and rollout. It should probably learn from (or adopt) KServe's
`ServingRuntime` / `ServedModel` split over time.

**Databricks / Snowflake / lakehouse platforms** — BigHill has a lakehouse *direction* (Arrow /
DataFusion / Iceberg) but isn't a full lakehouse yet, and it's aimed at the LLM lifecycle rather than
general analytics.

---

## Where it's headed

The main next step is to **close the loop and make it self-improving:**

```
serve ─▶ capture feedback ─▶ export preference data ─▶ DPO ─▶ eval ─▶ promote ─▶ serve
```

After that:

- Make the **DPO / feedback loop boringly reliable**, proven end to end — including a held-out
  train/eval split so a new model only gets promoted if it actually beats the one it came from.
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

BigHill is an **emerging self-hosted platform** for RAG, fine-tuning, evaluation, serving, and
feedback-driven improvement.

It's more serious than most LLM app repos — service-owned state, an outbox, Temporal, Kafka, explicit
contracts, clean boundaries. Next to the big commercial platforms it's less polished and more work to
run. Next to lightweight frameworks it's a far more complete system.

The value isn't "build a chatbot." It's **owning the full lifecycle of data, models, inference,
feedback, and retraining.**
