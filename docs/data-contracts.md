# Data Contracts Across Services

This document traces every data contract between the services in the platform —
asynchronous events (Postgres outbox → Kafka) and synchronous calls (gRPC / HTTP) —
surfaces the holes and misalignments found, and provides sequence diagrams grouped by
flow.

Related: [The Self-Improving Loop](self-improving-loop.md), [Multi-LoRA Serving](multi-lora-serving.md).

## Contract inventory

### Asynchronous events

Type registry: `shared_lib/messaging/message.go`. Delivery is via the transactional
outbox (`shared_lib/messaging`) onto Kafka topics.

| Event | Proto | Publisher | Subscriber(s) | Topic |
| --- | --- | --- | --- | --- |
| `user_created` / `user_updated` / `user_deleted` | profile.proto | tenant_service | all services (`shared_lib/tenant`) | `tenant` |
| `dataset_created` / `dataset_updated` / `dataset_deleted` | data_registry.proto | data_registry | ingestion, feature_materializer, inference¹ | `data_registry` |
| `dataset_file_uploaded` | ingestion.proto | ingestion | feature_materializer | `ingestion` |
| `raw_snapshot_ready` / `feature_snapshot_ready` / `embedding_snapshot_ready` | feature_materializer.proto | feature_materializer | data_registry | `feature_materializer` |
| `model_artifact_ingested` | ingestion.proto | ingestion | model_registry | `ingestion` |
| `promotion_requested` | model_registry.proto | model_registry | training | `model_registry` |
| `model_training_completed` / `model_training_failed` | training.proto | training | model_registry | `training` |
| `promotion_report_ready` | training.proto | training | model_registry | `training` |
| `model_updated` | model_registry.proto | model_registry | inference | `model_registry` |

¹ inference only consumes `dataset_updated`.

Every event type has exactly one publisher and at least one live subscriber, and
publisher/subscriber topics are aligned.

### Synchronous calls

| Caller → Callee | Contract | Kind |
| --- | --- | --- |
| api_gateway → inference | endpoint generation, feedback, preference datasets | REST/JSON |
| inference → feature_materializer | `SearchEmbeddings` | gRPC (feature_materializer.proto) |
| inference → model_serving | `POST /v1/private/served-models/{id}/load` | **raw HTTP, no proto** |
| data_stream → data_registry | `ReadSourceConnector`, `ReadDatasetTable` | gRPC (data_registry.proto) |
| training → model_registry | model resolver (reads model DTO incl. `lineage_name`) | REST/JSON |
| training → inference | preference dataset resolver | REST/JSON |


## Sequence diagrams

### Flow A — Tenant identity projection

```mermaid
sequenceDiagram
    participant TEN as tenant_service
    participant BUS as Kafka (topic tenant)
    participant ALL as All services (shared_lib/tenant)
    TEN->>BUS: user_created / user_updated / user_deleted (UserXEvent)
    BUS->>ALL: fan-out
    ALL->>ALL: upsert/delete local profile projection
    Note over ALL: gates FK readiness for org-scoped writes
```

### Flow B — Data ingestion & materialization

```mermaid
sequenceDiagram
    participant DR as data_registry
    participant ING as ingestion
    participant FM as feature_materializer
    DR->>ING: dataset_created / updated / deleted (data_registry.proto)
    DR->>FM: dataset_created / updated (data_registry.proto)
    ING->>FM: dataset_file_uploaded (ingestion.proto)
    FM->>FM: materialize (Temporal + Ray)
    FM->>DR: raw_snapshot_ready (feature_materializer.proto)
    FM->>DR: feature_snapshot_ready (feature_materializer.proto)
    FM->>DR: embedding_snapshot_ready (feature_materializer.proto)
    Note over DR: dataset marked RAG-ready
```

### Flow C — Model artifact upload path

```mermaid
sequenceDiagram
    participant ING as ingestion
    participant MR as model_registry
    participant MS as model_serving
    participant INF as inference
    ING->>MR: model_artifact_ingested (ingestion.proto)
    MR->>MR: create model (EVALUATED/READY), lineage_name = name
    MR->>MS: ensureServedModel (ServedModel CRD)
    MR->>INF: model_updated (model_registry.proto)
```

### Flow D — RAG inference runtime (synchronous)

```mermaid
sequenceDiagram
    participant GW as api_gateway
    participant INF as inference
    participant FM as feature_materializer
    participant MS as model_serving
    GW->>INF: Generate(GenerateRequest)
    alt model not loaded
        INF->>MS: POST /v1/private/served-models/{id}/load  (raw HTTP — no proto)
        INF->>INF: poll model until ServingLoadStatus = Loaded
    end
    INF->>FM: SearchEmbeddings(SearchEmbeddingsRequest)
    FM-->>INF: SearchEmbeddingsResponse (contexts)
    INF-->>GW: GenerateResponse (answer, contexts, model_id)
    GW->>INF: RecordFeedback(RecordFeedbackRequest)
    INF-->>GW: RecordFeedbackResponse
```

### Flow E — Training ← registry / inference

```mermaid
sequenceDiagram
    participant INF as inference
    participant MR as model_registry
    participant TR as training
    par preference-driven (DPO)
        TR->>INF: GET /v1/inference/preference-datasets/{id}
    and promotion-driven
        MR->>TR: promotion_requested (model_registry.proto, topic model_registry)
    end
    TR->>MR: (REST) resolve model DTO incl. lineage_name
    TR->>TR: DPO run (Temporal + Ray) + Deepchecks/Evidently eval
    TR->>MR: model_training_completed (training.proto, topic training)
    TR->>MR: promotion_report_ready (training.proto, topic training)
    TR-->>MR: model_training_failed (on failure)
```

### Flow F — Promotion gate & serving swap

```mermaid
sequenceDiagram
    participant TR as training
    participant MR as model_registry
    participant MS as model_serving
    participant INF as inference
    TR->>MR: model_training_completed (candidate, lineage_name)
    MR->>MR: createCandidateModel + ReadChampion (WHERE lineage_name)
    TR->>MR: promotion_report_ready (deepchecks / evidently)
    MR->>MR: EvaluatePromotion (floors, non-regression, comparable eval set)
    alt promoted
        MR->>MS: ensureServedModel (ServedModel CRD, champion pinned)
        MR->>INF: model_updated (status/serving fields, lineage_name)
    else rejected
        MR->>MR: record PROMOTION_REJECTED (champion unchanged)
    end
```

### Flow G — The self-improving loop (composite)

```mermaid
sequenceDiagram
    participant U as User/Gateway
    participant INF as inference
    participant TR as training
    participant MR as model_registry
    participant MS as model_serving
    U->>INF: endpoint generation → feedback (REST)
    U->>INF: POST /preference-datasets (freeze/reuse lineage eval set)
    U->>TR: POST /training-runs/dpo
    TR->>INF: resolve preference dataset
    TR->>TR: DPO train + eval on frozen eval_dataset_uri
    TR->>MR: model_training_completed + promotion_report_ready (training.proto)
    MR->>MR: gate champion(lineage_name) vs challenger
    MR->>MS: promote → serve (ServedModel)
    MR->>INF: model_updated (model_registry.proto)
    Note over INF,MS: not yet built - auto-trigger, canary, online rollback, loop orchestrator
```
