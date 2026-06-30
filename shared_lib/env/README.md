# Environment access policy

Service-owned environment variables should be read once during startup, in
`main.loadConfig()` or a dedicated `pkg/infra/config` package. Values should then
be passed into constructors as typed config.

The closed list of shared infrastructure exceptions is:

- `shared_lib/env`: implements env lookup helpers.
- `shared_lib/db`: reads DB logging and shared DB timeout envs.
- `shared_lib/logs`: reads `LOG_LEVEL`.
- `shared_lib/trace` and `shared_lib/metrics`: read standard OTEL exporter envs.
- `shared_lib/key_management`: reads `AUTH_KMS_KEY_ID`.

`KAFKA_BROKER`, `REDIS_ADDRESS`, and downstream service location variables are
not shared-library exceptions. Services should read those in startup config and
pass the resulting values to messaging, Redis, or client constructors.

New service-specific env reads should not be added to shared libraries, app
packages, repositories, clients, or handlers.
