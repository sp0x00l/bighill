# Slice 1 — Component Capability via External HTTP MCP Server

**Type:** implementation spec (contract + acceptance), not an ADR.
**Rides:** the Phase-4 pinned-MCP boundary in `tool_execution_service`.
**Strategic boundary:** *use LangChain and LlamaIndex as component ecosystems, not as the BigHill runtime.*

## Scope

- **One** externally-hosted HTTP MCP server.
- **One Tier-1 component only.** Tier 1 = the component **does not call an LLM internally** (pure tool/retriever). A LlamaIndex `QueryEngine`, or any chain that calls an LLM, is **Tier 2 → out of scope**. The operator asserts Tier-1; this slice does not auto-detect inner LLMs, but registration is rejected if the declared component is known to be Tier 2.
- **Tools over HTTP MCP / JSON-RPC only.** Resources, prompts, sampling, stdio, sessions, and richer transports are out of scope.
- **Config-pinned** (no catalog / grants / credential-binding system, no self-hosted wrapper/container, no re-discovery).

## Deployment & trust boundary

The MCP server is **externally hosted**; `tool_execution_service` governs **the call** (egress, timeout, response cap, credential, audit, breaker) — it does **not** sandbox the server's internals. Self-hosted wrappers (container + NetworkPolicy around the wrapper itself) require the container runtime and are **not** this slice.

## Configuration (env, extends the pinned-MCP pattern)

```
TOOL_EXECUTION_SERVICE_PINNED_MCP_SERVER_ENDPOINT      = https://...
TOOL_EXECUTION_SERVICE_PINNED_MCP_SERVER_TRANSPORT     = http
TOOL_EXECUTION_SERVICE_PINNED_MCP_TOOL_NAMES           = <declared-tool allowlist>
TOOL_EXECUTION_SERVICE_PINNED_MCP_CREDENTIAL_REF       = <runtime secret ref>
TOOL_EXECUTION_SERVICE_PINNED_MCP_ALLOWED_ORG_IDS      = <org uuids>
TOOL_EXECUTION_SERVICE_MCP_BREAKER_FAILURE_THRESHOLD   = <n>
TOOL_EXECUTION_SERVICE_MCP_BREAKER_COOLDOWN_MS         = <ms>
```

## Discovery contract (startup, `tools/list`)

- Call `tools/list` on the endpoint. For each name in the **declared-tool allowlist**:
  - must appear in `tools/list`; if any declared tool is **absent → whole discovery fails**, register nothing.
  - capture the **real `inputSchema` → `ParametersJSON`**; **missing/empty → fail closed** (never register a placeholder schema).
  - `ImplementationVersion = mcp:{endpoint_host}:{schema_hash}`; `ExecutorKind=MCP`, `EgressHosts=[endpoint_host]`, `AllowedOrgIDs`, `Enabled=true`.
- Tools **not** in the allowlist are ignored (allowlist authoritative — extra server tools never leak).
- Discovery failure ⇒ MCP tools unavailable (logged); `http_get` still serves. Boot-time only.
- **Schema test artifact:** the server exposes a **sample valid request/response pair** per declared tool. Slice 1 validates it in acceptance against a mock; the **SDK contract-test harness that runs the pair is Slice 2**.

## Invocation contract (per call, through the Phase-3 boundary)

1. Resolve tool + **tenant grant** (`AllowedOrgIDs`) — unknown tool / not granted → **deny**.
2. **Schema** — validate args against `ParametersJSON` → invalid → **reject pre-call**.
3. **Credential** — resolve the **runtime secret ref** and inject into the boundary call (e.g. `Authorization: Bearer`). *The runtime secret is injected by the platform into the boundary call; the customer's agent spec never contains the secret value.* Unresolvable → **deny**.
4. **Egress** — endpoint host allowlisted + the **hardened dial-pin client** (blocked ranges, re-resolution) → internal/rebinding target → **egress-blocked**.
5. **Timeout** + **response cap**.
6. `tools/call` → map result → `ToolInvocationResult`; classify errors → `ToolErrorType`.

## Circuit breaker (in `tool_execution_service`, not the wrapper)

- Key `{executor_kind}:{tool_name}:{egress_host}`. The wrapper may add its own resilience, but **BigHill's guaranteed breaker is at the governed boundary.**
- Failures ≥ threshold → **open**; while open, calls **fail fast** with `tool_circuit_open` (**transient**) + an audit row. Half-open probe after cooldown → close on success.

## Audit fields (durable)

`invocation_id, org_id, user_id, tool_name, tool_impl_version (mcp:host:schema_hash), executor_kind=mcp, status (COMPLETED|FAILED|DENIED), error_code, error_type, latency_ms, egress_host, trace_id, args_hash, args_preview, breaker_state`; plus, where the capability advertises them: `framework_version, wrapper_image_digest, schema_hash`.

## Trace propagation & observability

- Propagate **trace_id** (W3C `traceparent`) into the MCP request header.
- Record **framework/package version, wrapper image digest, schema hash, latency, error class**, and **token/cost + model IDs where available**.
- **Framework trace export where available** (no hard LangSmith dependency). If exported, it must be a **structured span set** — spans, tool calls, token/cost, model IDs, errors, timings, and **inputs/outputs allowed by policy**. **Do not require chain-of-thought / hidden reasoning.**

## User-visible failure mapping (`agent.tool.result`)

| Condition | error_type | severity |
|---|---|---|
| grant / schema / egress / credential denial | `policy_denied` | warning |
| timeout | `transient` | warning |
| circuit open | `transient` (`tool_circuit_open`) | warning |
| MCP tool error | `permanent`/`transient` per code | warning |

## Out of scope (stated)

catalog / grants / credential-binding system · self-hosted wrapper / container · Tier 2/3 · resources/prompts/sampling/stdio/sessions · re-discovery · the SDK contract-test harness (Slice 2).

## Acceptance tests

Against a **mock external HTTP MCP server** exposing `tools/list`, `tools/call`, and a sample pair. Cases 1–9 are boundary-level (`tool_execution_service`); case 10 is end-to-end (`api_gateway/test`).

1. **Discovery — real schema:** declared tool present → registered with the **real `inputSchema`** + real `impl_version`.
2. **Discovery — fail closed:** a declared tool **absent** → discovery fails, nothing registered; a tool with **missing schema** → not registered.
3. **Allowlist authoritative:** server exposes an extra tool → **not** registered.
4. **Happy path:** agent binds the tool → args validated → **credential injected** → real result → **trajectory records** the invocation → **durable audit row** (`executor_kind=mcp`).
5. **Fail-closed set:** unallowlisted org → denied (audit `DENIED`); invalid args → rejected pre-call; endpoint resolving to internal/blocked → egress-blocked; unresolvable credential → denied.
6. **Breaker:** N consecutive failures → **open** → next call **fails fast** `tool_circuit_open` (transient) + audit; half-open after cooldown → close on success.
7. **Credential isolation:** the agent spec/DTO contains **no** secret value; the secret is injected only at the boundary.
8. **Observability:** `trace_id` propagated into the MCP header; audit records `impl_version (schema_hash)`, `egress_host`, `latency`, `error_class`; token/cost recorded where the wrapper provides it.
9. **Failure mapping:** denied / timeout / egress-blocked / circuit-open surface as `agent.tool.result` with the correct `error_type`/severity.
10. **e2e (`api_gateway/test`):** agent spec binds the component-capability tool → run → mock server called → boundary enforced → trajectory + audit + events assert.
