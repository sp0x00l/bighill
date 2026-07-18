# Tool Service

`tool_service` runs tools that an agent model is allowed to call.

The service is intentionally narrow. It does not run the agent loop, store agent state, choose when a
tool should be called, or mutate model/training/dataset state. `inference_service` owns the loop.
`tool_service` owns the safety boundary for tools that act outside inference.

For the broader design, see [docs/agent-tool-plane.md](../docs/agent-tool-plane.md).

## Why This Service Exists

Some tools are safe to run inside `inference_service`. `search_knowledge` is one example: it reads the
same tenant vector data that normal RAG retrieval already reads.

Other tools act on the outside world. `http_get` can reach a URL. Future SQL or code tools would also
act outside inference. Those tools need a separate process with its own allowlist, argument checks,
timeouts, response caps, and audit. That is this service.

`tool_service` owns its own audit database. Inference still records the agent trajectory, but the
tool boundary keeps an independent record of every invocation, denial, and execution failure.

The rule is:

```text
agent loop asks for a world-acting tool
  -> inference_service calls tool_service over gRPC
  -> tool_service validates tenant + arguments + egress policy
  -> tool_service executes the tool or fails closed
```

## Current Tools

### `http_get`

Fetches content from an allowlisted HTTP or HTTPS URL.

Arguments:

```json
{
  "url": "https://example.com/data.json"
}
```

Controls:

- the caller org must be in `TOOL_SERVICE_ALLOWED_ORG_IDS`
- the URL host must be in `TOOL_SERVICE_HTTP_TOOL_ALLOWED_HOSTS`
- arguments must match the tool's JSON Schema
- loopback, private, link-local, CGNAT, NAT64, multicast, unspecified, and metadata hosts are blocked
- redirects are not followed
- environment proxies are disabled
- request timeout and max response bytes are enforced

Each invocation is audited with `invocation_id`, org/user, tool name, implementation version,
executor kind, status, error classification, latency, egress host, trace id, argument hash, and a
redacted argument preview. Raw arguments are not stored in the boundary audit table.

### Pinned MCP Tools

`tool_service` can also expose one operator-pinned HTTP MCP server. This is deliberately not an MCP
catalog or governance plane. There is no self-service publish flow, tenant grant lifecycle, dynamic
credential binding, resources, prompts, sampling, stdio transport, or hosted runtime here. Those
belong to the future `tool_catalog_service` extension.

The pinned MCP slice works like this:

- startup reads `TOOL_SERVICE_PINNED_MCP_*` config
- the service calls the MCP server's real `tools/list`
- only declared tools returned by the server are registered
- each registered tool uses the server-provided input schema
- `implementation_version` is `mcp:{server_host}:{schema_hash}`
- the configured org allowlist decides who can see or invoke the tool
- invocation calls the MCP server's real `tools/call`

If `tools/list` fails, a declared tool is absent, or a tool has no input schema, no placeholder tool
is registered. The tool is unavailable rather than fabricated.

Pinned MCP tools inherit the same boundary controls as `http_get`: JSON Schema validation, egress
allowlist, dial-time SSRF blocking, timeout, response cap, classified failure results, and durable
boundary audit. The credential config is an opaque reference only. The actual secret value must be
provided by the runtime secret environment under that referenced name; it is not stored in normal
tool-service config. The Helm chart can mount a Kubernetes secret into an environment variable named
by `TOOL_SERVICE_PINNED_MCP_CREDENTIAL_REF`.

## Contracts

The service has two contract layers.

### gRPC Service Contract

Defined in [data_contracts/protobufs/tools.proto](../data_contracts/protobufs/tools.proto).

`ListAvailableTools`

- request: `org_id`, `user_id`
- response: tool definitions visible to that actor
- used so inference can present only authorized tools to the model

`Invoke`

- request: `tool_name`, `arguments_json`, `org_id`, `user_id`, `trace_id`, `invocation_id`
- response: `result_json`, `is_error`, `error_code`, `error_message`, `implementation_version`, `latency_ms`, `error_type`
- used by inference when the model emits a tool call

The gRPC boundary validates UUIDs, required fields, and JSON shape before app code receives commands.

### JSON Schema Contracts

Schemas live in [data_contracts/schemas](../data_contracts/schemas).

`agent_spec.schema.json`

- user-authored YAML/JSON contract for an agent spec
- requires `schema_version`, `agent_lineage`, `model_binding`, `tools`, and `budgets`
- `model_binding` requires `model_id`
- `tools` must have non-empty names; the schema does not own the tool catalog
- publish-time policy resolves those tool names against the configured local/remote tool registries
- `budgets` requires `max_steps`, `token`, and `wall_ms`
- the DTO adapter also enforces platform policy, such as tool-bound agents needing at least two steps

Trajectory persistence is service-owned database state, not a standalone cross-language schema in V1.
It records the current interactive run tuple: `agent_spec_hash`, resolved `toolset_hash`, trajectory
schema version, decoding params, status, stop reason, presented tool schemas, generation result, and
tool invocation arguments/results. Future eval/training schemas are added with their own runtime
slices rather than shipped as unused contracts.

The schema files are the contract source. Go validates at the service boundary; generated readers in
other languages may consume them, but the control-plane authority stays in Go.

## Runtime Configuration

Configured by [scripts/config.sh](scripts/config.sh).

Important variables:

- `TOOL_SERVICE_GRPC_PORT`: gRPC port, local default `7084`
- `TOOL_SERVICE_DB_NAME`: audit database name
- `TOOL_SERVICE_DB_USER`: audit database user
- `TOOL_SERVICE_DB_PASSWORD`: audit database password
- `TOOL_SERVICE_DB_MAX_CONNECTIONS`: audit database connection pool limit
- `PGHOST`, `PGPORT`, `PGSSLMODE`: shared Postgres connection settings
- `TOOL_SERVICE_HEALTHCHECK_PORT`: health port, local default `5065`
- `TOOL_SERVICE_HEALTHCHECK_DB_LATENCY_THRESHOLD_SECONDS`: health check DB latency threshold
- `TOOL_SERVICE_HTTP_TOOL_ALLOWED_HOSTS`: comma-separated host allowlist for `http_get`
- `TOOL_SERVICE_ALLOWED_ORG_IDS`: comma-separated org allowlist
- `TOOL_SERVICE_HTTP_TOOL_TIMEOUT_MS`: per-request timeout
- `TOOL_SERVICE_HTTP_TOOL_MAX_RESPONSE_BYTES`: response size cap
- `TOOL_SERVICE_PINNED_MCP_SERVER_ENDPOINT`: optional HTTP MCP JSON-RPC endpoint
- `TOOL_SERVICE_PINNED_MCP_SERVER_TRANSPORT`: must be `http` for this slice
- `TOOL_SERVICE_PINNED_MCP_TOOL_NAMES`: comma-separated declared MCP tools to expose
- `TOOL_SERVICE_PINNED_MCP_CREDENTIAL_REF`: opaque runtime secret reference for MCP auth
- `TOOL_SERVICE_PINNED_MCP_ALLOWED_ORG_IDS`: comma-separated org allowlist for pinned MCP tools

Local dev allows only `localhost,127.0.0.1` and the fixed test org id. Staging/prod default to empty
allowlists, which means deny by default until an operator configures them.

## Error Policy

The service uses domain errors and maps them at the gRPC edge:

- invalid request or invalid arguments -> `InvalidArgument`
- unknown tool -> `NotFound`
- not allowlisted or blocked by policy -> `PermissionDenied`
- execution unavailable -> `Unavailable`
- unexpected failures -> `Internal`

Audit write failures are logged but do not change the tool invocation result.

## Security Rules

The service fails closed:

- unknown tool names are rejected
- duplicate tool names are rejected at registry construction
- disabled tools are not listed or invokable
- empty allowlists deny access
- org/user IDs must be valid
- tool arguments must be valid JSON and match the tool DTO
- egress hosts must be allowlisted
- internal network targets are blocked
- redirects are not followed
- no environment proxy is used

This is not prompt-injection prevention by wording. It is capability containment: even if a model is
tricked into asking for something dangerous, the tool can only do what the tenant policy permits.

## Local Commands

```sh
cd tool_service
./scripts/test.sh local-dev
./scripts/run.sh local-dev
```

The root compose/test scripts start the service when the agent tool flow is enabled.

## Package Shape

```text
pkg/domain/model      tool definitions, command/result/audit models, enums
pkg/app               usecase, ports, audit behavior
pkg/infra/repo/static static allowlist-backed registry
pkg/infra/repo/db     durable boundary audit repository
pkg/infra/executor    tool executors and argument DTO adapters, including pinned MCP
pkg/infra/network/grpc gRPC DTO adapter and server
pkg/infra/policy      boundary policy resolver
pkg/infra/credential  runtime credential resolution
```

The app layer depends on ports. DTO validation happens at infra boundaries.
