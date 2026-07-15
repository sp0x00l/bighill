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
- loopback, private, link-local, CGNAT, NAT64, multicast, unspecified, and metadata hosts are blocked
- redirects are not followed
- environment proxies are disabled
- request timeout and max response bytes are enforced

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
- requires `schema_version`, `agent_lineage`, `runtime_mode`, `model_binding`, `tools`, and `budgets`
- `model_binding` requires both `model_id` and `effective_base_id`
- `tools` must have non-empty names; the schema does not own the tool catalog
- publish-time policy resolves those tool names against the configured local/remote tool registries
- `budgets` requires `max_steps` and `token`
- the DTO adapter also enforces platform policy, such as tool-bound agents needing at least two steps

`golden_task.schema.json`

- describes eval/training seed tasks for future agent promotion
- each task belongs to a split: `seed_train`, `dev_eval`, or `promotion_holdout`
- carries hashes and fingerprints used to prevent train/eval leakage
- `group_key` is a hint; `content_fingerprint` is the stronger anti-leak signal

`trajectory.schema.json`

- describes the durable run record used for audit and future training
- requires a run tuple: `agent_spec_hash`, `effective_base_id`, `toolset_hash`, schema version, status, stop reason, and training eligibility
- each step stores presented tool schemas, the generation result, finish reason, and tool invocations
- tool invocations include tool name, implementation version, arguments, result, and error type

The schema files are the contract source. Go validates at the service boundary; Python jobs may use
generated Pydantic models as readers, not as the control-plane authority.

## Runtime Configuration

Configured by [scripts/config.sh](scripts/config.sh).

Important variables:

- `TOOL_SERVICE_GRPC_PORT`: gRPC port, local default `7084`
- `TOOL_SERVICE_HEALTHCHECK_PORT`: health port, local default `5065`
- `TOOL_SERVICE_HTTP_TOOL_ALLOWED_HOSTS`: comma-separated host allowlist for `http_get`
- `TOOL_SERVICE_ALLOWED_ORG_IDS`: comma-separated org allowlist
- `TOOL_SERVICE_HTTP_TOOL_TIMEOUT_MS`: per-request timeout
- `TOOL_SERVICE_HTTP_TOOL_MAX_RESPONSE_BYTES`: response size cap

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
pkg/infra/executor    tool executors and argument DTO adapters
pkg/infra/network/grpc gRPC DTO adapter and server
pkg/infra/audit       boundary audit implementation
```

The app layer depends on ports. DTO validation happens at infra boundaries.
