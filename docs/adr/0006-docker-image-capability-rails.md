# ADR 0006: Docker-Image Capability Rails

## Status

Proposed, deferred.

Do not build this until after the first flywheel proof. It depends on
[ADR 0005](0005-tool-service-boundary-contract.md) and the tool catalog.

## Context

Developers will want to bring real custom logic, not only configuration. This is the flexibility
people get from in-process SDKs.

BigHill's rule is that untrusted developer code never runs inside `inference_service` and never
becomes the agent runtime. The question is how to allow customer code as a governed capability that
the runtime can call.

Docker images are the natural target for hosted custom capabilities. They are acceptable only if they
sit behind the same boundary as other world-acting tools. The failure mode to avoid is: "upload an
image that becomes the agent."

## Decision

Allow Docker images as hosted capability runtimes, not as the agent runtime.

The path is:

```
OCI image -> capability manifest -> catalog -> deployment controller -> tool_service invocation
```

It is not:

```
OCI image -> replaces agent loop
```

A hosted container is registered in the catalog with a manifest:

- capability kind
- pinned image digest
- protocol, such as MCP-HTTP
- input/output schema hashes
- egress policy
- required credentials
- resource and timeout limits

A deployment controller deploys the pinned digest to Kubernetes. `tool_service` invokes it through
the shared boundary contract from ADR 0005. The agent spec references it by stable capability id and
version.

Required controls are enforced by the platform, not by the image:

- image pinned by digest, not tag
- SBOM and vulnerability scan
- image signature and source verification
- non-root container
- read-only root filesystem
- no privileged mode
- CPU, memory, and concurrency limits
- NetworkPolicy egress allowlist
- platform-injected credentials, never secrets in the spec
- no direct DB, Kafka, or Redis access unless explicitly granted
- input/output schema validation at `tool_service`
- timeout and response-size caps
- durable boundary audit per call
- OpenTelemetry trace propagation
- user-visible failure events through `socket_service`

## Consequences

- Customers get code-level extensibility without BigHill giving up the core loop, tenant isolation,
  or the run records the flywheel depends on.
- BigHill takes on a hosted-runtime surface: deployment controller, image scanning, sandboxing, and
  operational isolation.
- This is deferred until after the flywheel proof. Side-effecting container tools wait further, until
  approval and idempotency exist.
- Until then, external capabilities are config-pinned MCP servers behind ADR 0005. No hosted images.
- Hosting untrusted multi-tenant containers is a serious security and operations commitment. A rented
  sandbox runtime is an acceptable substitute until scale justifies owning the hosted-image
  controller.
