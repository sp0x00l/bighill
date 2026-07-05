# feature_materializer_service

## What It Does

`feature_materializer_service` turns acquired datasets into retrieval-ready assets. It is the materialization layer between ingestion/data registry and inference: uploaded or connector-backed data becomes raw snapshots, feature snapshots, embedding snapshots, and searchable vector records.

The core pipeline is:

1. Accept a materialization request from a data-registry fact.
2. Build a raw snapshot from either an uploaded artifact or a Data Stream connector query.
3. Normalize source content into Parquet.
4. Build a feature snapshot with stable schema and extraction metadata.
5. Chunk source text and generate embeddings.
6. Store embedding records in Postgres/pgvector for inference retrieval.
7. Publish snapshot-ready facts back to the registry through the outbox.

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
- Optional Polaris/Iceberg registration for raw and feature snapshots.
- Temporal workflows for durable multi-step materialization.
- Kafka facts for raw, feature, and embedding snapshot state changes.
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
- Configurable extractors, cleaners, token-window chunking, and structure-aware chunking.
- PDF extraction via `pdf_extractor_lib`.
- Optional Polaris/Iceberg table writes for lakehouse-backed materializations.

## Snapshot Flow

Raw snapshots are the first immutable representation of a dataset. Uploaded artifacts are read from object storage, while connector-backed datasets are read by `data_stream_service` and streamed back over Flight. Both paths converge on the same Parquet artifact shape.

Feature snapshots are derived from raw snapshots. They preserve extraction metadata such as source format, extractor name/version, and schema details so that retrieval behavior can be reproduced.

Embedding snapshots are derived from feature snapshots and an embedding strategy. The strategy records extractor, cleaner, chunker, embedding provider, model, dimensions, chunk size, and overlap. This makes the embedding output traceable and idempotent.

## How It Fits

- Consumes dataset-file and materialization facts from `data_registry_service`.
- Reads uploaded artifacts from object storage.
- Reads connector-backed datasets from `data_stream_service`.
- Produces raw, feature, and embedding snapshots.
- Publishes snapshot-ready facts back to `data_registry_service`.
- Exposes embedding search over gRPC for `inference_service`.

## Local Development

Local and CI use deterministic embedding defaults so tests do not require a GPU embedding server. Staging/prod can point `FEATURE_MATERIALIZER_SERVICE_EMBEDDING_URL` at TEI or another compatible embedding endpoint.

The local file/object-store path is still snapshot based: tests write a dataset artifact, materialization reads it, and the service emits the same snapshot events used in deployed environments.
