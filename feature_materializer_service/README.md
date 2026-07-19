# feature_materializer_service

## What It Does

`feature_materializer_service` turns acquired datasets into retrieval-ready assets. It is the materialization layer between ingestion/data registry and inference: uploaded or connector-backed data becomes raw snapshots, feature snapshots, embedding snapshots, graph snapshots, searchable vector records, and graph-search records.

The core pipeline is:

1. Accept a materialization request from a data-registry fact.
2. Build a raw snapshot from either an uploaded artifact or a Data Stream connector query.
3. Normalize source content into Parquet.
4. Build a feature snapshot with stable schema and extraction metadata.
5. Chunk source text and generate embeddings.
6. Store embedding records in Postgres/pgvector for inference retrieval.
7. Optionally extract entities/relations through a model-serving graph extractor and store a dataset-scoped graph.
8. Publish snapshot-ready facts back to the registry through the outbox.

This keeps training and inference off live source systems. Downstream services read immutable snapshots and vectors, not mutable user uploads or external databases.

## Feature Set

- Raw snapshot materialization for uploaded files from `ingestion_service`.
- Connector-backed raw snapshots through `data_stream_service` over Arrow Flight.
- CSV, JSON, Parquet, plain text, Markdown, HTML, and PDF normalization to Parquet.
- PDF text extraction through `pdf_extractor_lib` via the service's `PDFDocumentExtractor`.
- HTML section extraction with heading metadata.
- Basic text cleaning before snapshot and embedding generation.
- Token-window chunking for general text.
- Structure-aware token-window chunking that preserves heading context where available.
- HTTP embedding providers for TEI-compatible services and Ollama-style providers.
- Embedding storage and similarity search through Postgres/pgvector.
- Model-serving graph extraction against the versioned `graph_extraction_v1` contract.
- Graph node/edge storage and local multi-hop graph search for GraphRAG.
- Optional Polaris/Iceberg registration for raw and feature snapshots.
- Temporal workflows for durable multi-step materialization.
- Kafka facts for raw, feature, embedding, and graph snapshot state changes.
- Subscriber health checks and per-stream Kafka consumer groups.

## MLOps / Platform Pieces

- Temporal workflows and activities for durable materialization.
- Postgres for snapshot metadata and vector storage.
- pgvector-style similarity search for retrieval.
- Kafka for materialization facts.
- Postgres transactional outbox for fact publication.
- Arrow Flight for connector-backed dataset reads from `data_stream_service`.
- Apache Arrow/Parquet as the service's snapshot interchange format.
- Embedding providers over HTTP, including TEI-compatible embedding endpoints.
- Graph extraction over an OpenAI-compatible model-serving endpoint.
- Configurable extractors, cleaners, token-window chunking, and structure-aware chunking.
- PDF extraction via `pdf_extractor_lib`.
- Optional Polaris/Iceberg table writes for lakehouse-backed materializations.

## Snapshot Flow

Raw snapshots are the first immutable representation of a dataset. Uploaded artifacts are read from object storage, while connector-backed datasets are read by `data_stream_service` and streamed back over Flight. Both paths converge on the same Parquet artifact shape.

Feature snapshots are derived from raw snapshots. They preserve extraction metadata such as source format, extractor name/version, and schema details so that retrieval behavior can be reproduced.

Embedding snapshots are derived from feature snapshots and an embedding strategy. The strategy records extractor, cleaner, chunker, embedding provider, model, dimensions, chunk size, and overlap. This makes the embedding output traceable and idempotent.

Graph snapshots are derived from embedding chunks and a graph extraction strategy. The production extractor calls a configured OpenAI-compatible model-serving endpoint with the embedded `graph_extraction_prompt_v1` prompt and validates the result against `graph_extraction_v1`. Prompt content is included in the provenance hash, so changing the prompt changes the graph snapshot identity even if the label is accidentally reused. Extraction failures mark the graph snapshot `FAILED`; they are not stored as an empty `READY` graph.

## How It Fits

- Consumes dataset-file and materialization facts from `data_registry_service`.
- Reads uploaded artifacts from object storage.
- Reads connector-backed datasets from `data_stream_service`.
- Produces raw, feature, and embedding snapshots.
- Produces graph snapshots when graph materialization is enabled for the dataset.
- Publishes snapshot-ready facts back to `data_registry_service`.
- Exposes embedding and graph search over gRPC for `inference_service`.

## Local Development

Local and CI use the same runtime embedding and graph-extraction contracts as staging and production. Configure `FEATURE_MATERIALIZER_SERVICE_EMBEDDING_PROVIDER`, `FEATURE_MATERIALIZER_SERVICE_EMBEDDING_URL`, and `FEATURE_MATERIALIZER_SERVICE_EMBEDDING_MODEL` for a TEI or Ollama-compatible embedding endpoint. When graph materialization is enabled, configure `FEATURE_MATERIALIZER_SERVICE_GRAPH_EXTRACTOR=model_serving`, `FEATURE_MATERIALIZER_SERVICE_GRAPH_EXTRACTION_ENDPOINT`, `FEATURE_MATERIALIZER_SERVICE_GRAPH_EXTRACTION_MODEL`, `FEATURE_MATERIALIZER_SERVICE_GRAPH_EXTRACTION_PROMPT_VERSION`, and `FEATURE_MATERIALIZER_SERVICE_GRAPH_EXTRACTION_SCHEMA_VERSION`. The service fails startup if enabled graph extraction is missing those values.

The local file/object-store path is still snapshot based: tests write a dataset artifact, materialization reads it, and the service emits the same snapshot events used in deployed environments.
