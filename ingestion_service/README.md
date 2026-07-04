# ingestion_service

## What It Does

`ingestion_service` owns object ingestion for user-supplied files. It validates and promotes dataset files, accepts model artifact uploads through the same presigned-session flow, stores accepted objects in the configured bucket, and records upload metadata.

It is the byte-moving boundary for the platform. Dataset ownership and lifecycle state remain in `data_registry_service`; model ownership and promotion state remain in `model_registry_service`. This service handles staging, validation, promotion, and audit metadata for uploaded objects.

## MLOps / Platform Pieces

- Postgres for ingestion and upload-session metadata.
- S3-compatible object storage for raw data files and model artifacts, with local-dev storage support.
- Presigned object-store uploads so large files bypass the API gateway payload limit.
- Kafka for ingestion lifecycle facts.
- Postgres transactional outbox for atomic state-change publication.
- File-format validators for text, Markdown, HTML, JSON, CSV, Parquet, and PDF-oriented workflows.

## How It Fits

- Consumes dataset lifecycle facts from `data_registry_service`.
- Validates dataset upload eligibility before accepting data-file bytes.
- Writes raw dataset files and uploaded model artifacts to object storage.
- Publishes file-uploaded facts consumed by `feature_materializer_service`.
- Keeps model artifacts on separate object-store rails (`models/artifacts/...`) so model registry/promotion can evolve without sharing dataset state.

## Local Development

Configuration comes from `scripts/config.sh` and the service-specific env vars prefixed with `INGESTION_SERVICE_`. The service is run as part of the root local-dev/test scripts.
