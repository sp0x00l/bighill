# Query Engine

This directory contains the Rust/DataFusion execution component for the data stream query gateway.

Current shape:

- `data_stream_service` owns the public Arrow Flight API, auth, validation, observability, and service lifecycle.
- `datafusion_query_engine` is the first local DataFusion executor. It queries Parquet files from a local path and emits Arrow IPC on stdout.
- The Go gateway can run this executable with `DATA_STREAM_SERVICE_QUERY_ENGINE_MODE=datafusion`, then stream the returned Arrow records over Flight.
- Local-dev defaults to `DATA_STREAM_SERVICE_QUERY_ENGINE_MODE=registry` so the stream service resolves registered datasources through the data registry. Use `DATA_STREAM_SERVICE_QUERY_ENGINE_MODE=datafusion` when you want the Rust query engine path.

Local runner:

```sh
cd query_engine/datafusion_query_engine
DATAFUSION_PARQUET_PATH=../../tmp/local_s3_storage cargo run -- --sql "SELECT * FROM dataset LIMIT 10" > result.arrow
```

Build and test:

```sh
make build-query-engine
make test-query-engine
```

The runner expects the configured path to contain Parquet files registered as the `dataset` table.
