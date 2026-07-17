# ADR 0005: Tool-Service Boundary Contract

## Status

Proposed.

P0 for the agent rails path. This generalizes existing code; it does not replace it.

## Context

`tool_service` is the isolation boundary for tools that act outside the platform.

The current `http_get` executor is already substantially hardened. It has exact-host allowlists,
blocked private/loopback/link-local/multicast/metadata addresses, CGNAT and NAT64 blocks, redirects
disabled, dial-time resolution to the validated IP, response byte caps, request timeout, and proxies
disabled. `tool_usecase` records an audit event on success, denial, and failure.

Two problems block expansion to MCP, SQL, browser, and container tools:

- the hardening lives inside the `http_get` executor, so each new tool kind could reimplement only
  part of it
- the boundary audit is log-based, not durable and queryable

## Decision

Treat `http_get` as the reference implementation, not as the whole boundary.

Extract a shared boundary contract that every executor uses. The contract is made of policy objects
resolved per tool and tenant:

- **egress policy:** scheme and host allowlist, blocked address ranges, dial-pin, redirect handling
- **timeout policy:** per-call deadline
- **response cap:** max bytes and max runtime
- **credential policy:** platform-injected secrets, never secrets from the spec
- **schema policy:** input/output JSON Schema validation
- **audit policy:** durable boundary record per invocation
- **event policy:** user-visible failure/status events

The boundary fails closed. Unknown tools, invalid arguments, missing tenant grants, and blocked egress
deny before execution.

Every executor gets its safety behavior by using these policies, not by re-coding them.

Replace the log-based audit with a durable, queryable audit store owned by `tool_service`. Write it
on success, denial, and failure. This audit is independent of the caller's trajectory.

## Consequences

- Adding a new tool kind means implementing execution and declaring which policies apply.
- New executors inherit egress checks, timeouts, caps, credential handling, schema validation, audit,
  and events.
- `http_get` is refactored to use the shared policy objects. Its current behavior is the baseline.
- `tool_service` gains a durable audit table. The boundary keeps its own record and does not trust
  the caller to audit honestly.
- MCP and container tools use the same contract instead of creating weaker parallel rules.

## Acceptance Criteria

Every executor must:

- block localhost, RFC1918, link-local, IPv6 local, multicast, cloud metadata addresses, and internal
  DNS names where egress applies
- defeat DNS rebinding with dial-pin or an equivalent boundary control
- block redirects, or re-validate redirects against the allowlist
- cap response bytes and runtime
- fail closed on unknown tool, invalid args, missing grant, or blocked egress
- write a durable boundary audit even on failure
- publish user-visible failure events through `socket_service`
