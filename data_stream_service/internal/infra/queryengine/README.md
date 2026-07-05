# Query Engine

This directory contains the Rust/DataFusion execution component for the data stream query gateway. It is internal infrastructure owned by `data_stream_service`.

Current shape:

- `data_stream_service` owns the public Arrow Flight API, auth, validation, observability, and service lifecycle.
- `datafusion_query_engine` is the first local DataFusion executor. It queries Parquet files from a local path and emits a framed Arrow IPC stream on stdout.
- The Go gateway can run this executable with `DATA_STREAM_SERVICE_QUERY_ENGINE_MODE=datafusion`, then stream the returned Arrow records over Flight.
- Local-dev defaults to `DATA_STREAM_SERVICE_QUERY_ENGINE_MODE=registry` so the stream service resolves registered datasources through the data registry. Use `DATA_STREAM_SERVICE_QUERY_ENGINE_MODE=datafusion` when you want the Rust query engine path.

Local runner:

```sh
cd data_stream_service/internal/infra/queryengine/datafusion_query_engine
DATAFUSION_PARQUET_PATH=../../../../../tmp/local_s3_storage cargo run -- --sql "SELECT * FROM dataset LIMIT 10" > result.bhipc
```

Build and test:

```sh
make build-query-engine
make test-query-engine
```

The runner expects the configured path to contain Parquet files registered as the `dataset` table.

Stdout is a data-only channel. Query logs and diagnostics must go to stderr because
the Go gateway reads stdout as:

1. `BHIPC001` magic header
2. little-endian uint64 expected row count
3. Arrow IPC stream
4. `BHIPCEND` magic footer

Any extra stdout bytes before the header or after the footer are treated as a
corrupt query result.
