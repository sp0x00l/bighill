# Tool Catalog Service

`tool_catalog_service` is the tool control plane.

It owns what dynamic tools exist, which tenants may use them, and which runtime credential reference
is bound for a tenant. It does not execute tools. `tool_execution_service` executes tools from its
own local projection, so a catalog outage does not sit on the hot path for an agent run.

## Responsibilities

- Publish tool capability versions with real input schemas.
- Grant a capability version to an org.
- Bind an org credential reference to a capability.
- Emit catalog events through the Postgres outbox.

The service is intentionally narrow. It does not own MCP transport, HTTP egress, response caps, tool
invocation audit, or agent trajectories. Those belong to `tool_execution_service` and
`inference_service`.

## Runtime Flow

```text
operator publishes capability
  -> tool_catalog_service stores tool_capability_versions
  -> outbox emits tool_capability_updated
  -> tool_execution_service projects it locally

operator grants org + binds credential ref
  -> outbox emits grant/binding events
  -> tool_execution_service resolves available tools from its own DB
```

At invocation time, inference calls `tool_execution_service` directly. `tool_execution_service` never
calls the catalog synchronously to run a tool.

## Schema Design

The database has only the state Phase 6 uses today:

- `tool_capability_versions`: content-addressed tool manifests. Fields include `capability_id`,
  `version`, `tool_name`, `kind`, `parameters_json`, egress hosts, limits, credential requirement,
  implementation version, and content hash.
- `tenant_capability_grants`: tenant authorization rows. Missing grant means deny.
- `tool_credential_bindings`: tenant credential references. The value is an opaque secret ref, not
  the secret itself.
- `outbox_messages`: transactional event delivery.

Only `ACTIVE` capability versions exist because no deprecated/quarantined transition has shipped yet.
Grant status includes `REVOKED` because execution reads that state and hides revoked tools.

The manifest hash is computed over the canonical manifest using the shared serializer. Tool schemas
come from real publish input; the DTO adapter canonicalizes and validates the JSON at the boundary.
The app layer assumes commands are already valid.

## Events

Events are defined in [data_contracts/protobufs/tool_catalog.proto](../data_contracts/protobufs/tool_catalog.proto):

- `tool_capability_updated`
- `tool_grant_updated`
- `tool_credential_binding_updated`

`tool_execution_service` consumes these events and stores local projection rows. This is the control
plane to data plane split: catalog owns source-of-truth metadata, execution owns safe invocation.

## API

- `POST /v1/tool-catalog/capabilities`
- `GET /v1/tool-catalog/capabilities/{capabilityVersionId}`
- `POST /v1/tool-catalog/grants`
- `POST /v1/tool-catalog/credential-bindings`

User and org IDs come from trusted headers. Request DTO validation happens in the REST adapter.

## Local Commands

```sh
cd tool_catalog_service
./scripts/test.sh local-dev
./scripts/build.sh local-dev
```
