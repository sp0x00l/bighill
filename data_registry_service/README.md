# data_registry_service

## What It Does

`data_registry_service` is the system of record for datasets, external sources, and dataset processing state. It tracks dataset lifecycle, source metadata, versions, and materialization progress.

It owns the data catalog boundary for the platform: other services publish facts about work they completed, but registry decides the durable dataset state.

## MLOps / Platform Pieces

- Postgres for dataset/source registry state.
- Kafka for dataset lifecycle facts.
- Postgres transactional outbox for atomic fact publication.
- gRPC APIs used by query and materialization services.
- Monotonic processing-state updates in the database for concurrent materialization facts.
- Polaris REST catalog integration for lakehouse source/table identity when the Iceberg path is enabled.

## How It Fits

- Publishes dataset-created/updated facts.
- Consumes materialization facts from `feature_materializer_service`.
- Provides dataset/source metadata to `data_stream_service`.
- Drives training trigger input by publishing dataset updates.

For lakehouse-backed datasets, `data_registry_service` records the dataset and source connector metadata and registers the connector/table identity with Polaris. The actual bytes are still produced by `feature_materializer_service` and queried by `data_stream_service` through DataFusion/Arrow Flight; registry remains the metadata and lifecycle authority.

## Local Development

Configuration comes from `scripts/config.sh` and `DATA_REGISTRY_SERVICE_` env vars. HTTP, gRPC, health, Kafka, DB, and outbox settings are all controlled there and mirrored in Helm/compose/launch configuration.
