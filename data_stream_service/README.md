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

## DataFusion IPC Boundary

When `data_stream_service` runs the Rust DataFusion query engine, stdout is reserved for data only. The subprocess writes a small framed payload:

1. `BHIPC001` magic header
2. little-endian uint64 expected row count
3. Arrow IPC stream
4. `BHIPCEND` magic footer

The Go gateway rejects missing headers, missing footers, row-count mismatches, and any extra stdout bytes. Logs and diagnostics must go to stderr. Flight `DoGet` streams the subprocess stdout through the Arrow decoder instead of buffering the whole result in memory.
