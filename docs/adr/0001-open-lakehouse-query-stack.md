# ADR 0001: Open Lakehouse Query Stack

## Status

Accepted.

## Context

The ML platform needs one data shape that works for ingestion, feature pipelines, training,
inference/RAG, and observability.

Go should stay the platform control plane. Python should be used for ML jobs, not for service
ownership, orchestration, or deployment logic.

## Decision

Use an open lakehouse stack:

- Go services own APIs, auth, metadata, orchestration, Kafka events, object storage, observability,
  and deployment wiring.
- `data_registry_service` stores metadata without tying the platform to one catalog vendor.
- ingestion writes raw data and emits events so later services can materialize tables.
- query access goes through an Arrow Flight-compatible query-engine boundary.
- DataFusion is the first query engine for ML features and RAG reads.
- Iceberg with a REST-compatible catalog, such as Polaris or Nessie, is the target table/catalog
  layer.
- Trino can be added later for broad SQL, BI, and analytics if needed.

## Consequences

Registry connectors keep a neutral catalog id. `data_stream_service` points at a query engine, not a
vendor-specific data source.

The first implementation can use a local catalog client stub while the DataFusion gateway is added
behind the query-engine boundary. This lets existing services keep moving without locking the
architecture to the first local implementation.
