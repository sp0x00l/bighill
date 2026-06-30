# ADR 0001: Open Lakehouse Query Stack

## Status

Accepted

## Context

The ML platform needs a data architecture that supports data collection, feature pipelines, training data access, inference/RAG workflows, and observability without making Python the platform control plane. 

## Decision

Use an open lakehouse shape:

- Go services own APIs, auth, metadata, orchestration, Kafka events, object storage integration, observability, and deployments.
- Data registry stores catalog-neutral metadata.
- Data ingestion writes raw data and emits events for later table materialization.
- Data stream/query access uses a generic Arrow Flight-compatible query engine boundary.
- DataFusion is the preferred first query engine implementation for ML-native feature and RAG access.
- Iceberg plus a REST-compatible catalog, such as Polaris or Nessie, is the target table/catalog layer.
- Trino can be added later for broad SQL, BI, and general analytics if needed.

## Consequences

Registry connectors keep a neutral catalog identifier, and stream service configuration refers to a query engine instead of a vendor-specific data source. The first implementation uses a local catalog client stub so existing services can continue to build while the DataFusion gateway is introduced behind the query-engine boundary.
