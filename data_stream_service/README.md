# data_stream_service

## What It Does

`data_stream_service` exposes registered data through an Arrow Flight interface. It is the query/read boundary for data consumers that need columnar transport instead of REST payloads.

The service does not own dataset state. It asks `data_registry_service` for catalog/source metadata, then delegates query execution to its internal query engine adapter.

## MLOps / Platform Pieces

- Apache Arrow Flight for high-throughput columnar RPC.
- gRPC health and service endpoints.
- Internal DataFusion query-engine adapter for local/lakehouse-style query execution.
- Registry-backed query mode for resolving dataset/source metadata.

## How It Fits

- Serves Arrow Flight clients.
- Reads source metadata from `data_registry_service`.
- Keeps query execution infrastructure internal to the data streaming service.
- Provides the foundation for lakehouse-style reads without putting query-engine details into domain code.

## Local Development

Configuration is controlled by `DATA_STREAM_SERVICE_` env vars. The internal DataFusion binary path and query mode are configured through the standard local-dev config.
