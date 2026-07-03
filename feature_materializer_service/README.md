# feature_materializer_service

## What It Does

`feature_materializer_service` turns raw uploaded data into feature snapshots and embedding snapshots. It runs the materialization workflow, extracts and cleans document content, chunks it, generates embeddings, and stores searchable vectors.

This is the feature pipeline: raw artifacts in object storage become versioned retrieval assets for inference.

## MLOps / Platform Pieces

- Temporal workflows and activities for durable materialization.
- Postgres for snapshot metadata and vector storage.
- pgvector-style similarity search for retrieval.
- Kafka for materialization facts.
- Postgres transactional outbox for fact publication.
- Embedding providers over HTTP, including TEI-compatible embedding endpoints.
- Configurable extractors, cleaners, token-window chunking, and structure-aware chunking.
- PDF extraction via the native `pdf_extractor_lib` path where configured.

## How It Fits

- Consumes file-uploaded facts from `data_ingestion_service`.
- Produces feature and embedding snapshots.
- Publishes materialization facts back to `data_registry_service`.
- Exposes retrieval/search over gRPC for `inference_service`.

## Local Development

Local and CI use deterministic embedding defaults so tests do not require a GPU embedding server. Staging/prod can point `FEATURE_MATERIALIZER_SERVICE_EMBEDDING_URL` at TEI or another compatible embedding endpoint.
