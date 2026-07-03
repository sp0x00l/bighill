# data_ingestion_service

## What It Does

`data_ingestion_service` owns raw data upload and ingestion. It validates uploaded files, stores accepted objects in the configured bucket, records upload metadata, and publishes ingestion facts for downstream materialization.

It is the boundary between user-provided files and the feature pipeline. Dataset ownership and lifecycle state remain in `data_registry_service`; this service only accepts uploads for datasets that registry has declared valid.

## MLOps / Platform Pieces

- Postgres for ingestion metadata.
- S3-compatible object storage for raw uploads, with local-dev storage support.
- Kafka for ingestion lifecycle facts.
- Postgres transactional outbox for atomic state-change publication.
- File-format validators for text, Markdown, HTML, JSON, CSV, Parquet, and PDF-oriented workflows.

## How It Fits

- Consumes dataset lifecycle facts from `data_registry_service`.
- Validates upload eligibility before accepting bytes.
- Writes raw artifacts to object storage.
- Publishes file-uploaded facts consumed by `feature_materializer_service`.

## Local Development

Configuration comes from `scripts/config.sh` and the service-specific env vars prefixed with `DATA_INGESTION_SERVICE_`. The service is run as part of the root local-dev/test scripts.
