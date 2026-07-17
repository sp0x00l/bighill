# ADR 0004: Agent Authoring and Extensibility

## Status

Accepted.

## Context

Agent SDKs give developers a broad authoring surface. They usually support programmatic agents, YAML
configuration, custom tools, MCP servers, memory, guardrails, sub-agents, approval flows, streaming,
tracing, token usage, CLIs, and remote agents.

BigHill needs those same capability categories. A serious agent platform cannot stop at "publish a
YAML spec and use platform-built tools." Developers need to bring logic, integrations, memory
policies, specialized agents, approval workflows, and operational controls.

The constraint is not "no custom code." The constraint is where that code runs and how the platform
controls it.

BigHill owns model serving, trajectories, promotion, and the self-improving loop. Those features only
work if the platform can say exactly what produced a run: the spec, model, tools, data, policies, and
runtime versions. If arbitrary developer code can change the agent loop inside `inference_service`,
that record is no longer trustworthy.

## Position

BigHill should support custom tools, MCP, memory, policy, sub-agents, durable activities, streaming,
tracing, token accounting, CLIs, and SDK-assisted authoring.

It should not do that by letting developers replace or patch the agent runtime.

The rule is:

```text
The agent loop is closed to code injection.
Capabilities enter only through typed ports whose hosts enforce policy.
Untrusted code never runs in inference_service, but it runs.
```

The agent spec stays declarative. The power comes from the capabilities it references.

## Decision

Use a governed capability model.

1. **Keep the agent artifact declarative.** `AgentSpec` is the content-addressed artifact that binds
   prompts, model selection, tools, memory policy, sub-agent references, budgets, stop conditions,
   guardrails, and runtime policy. It references capabilities by stable id. It does not embed code or
   secrets.

2. **Keep the loop closed.** `inference_service` owns the generate -> act -> observe loop. Customer
   code cannot patch the loop, replace the planner, bypass trajectory writes, or execute in-process.
   New behavior enters through explicit ports.

3. **Use ports, hosts, and governed units.** A port is what the agent loop can call. A host is where
   an implementation runs. A governed unit is the registered capability behind that host.

| Capability | Port | Host / isolation boundary |
|------------|------|---------------------------|
| Built-in retrieval tool, such as `search_knowledge` | `ToolInvoker` | `inference_service`, only for data the platform already handles safely |
| Custom tool / MCP server | `ToolInvoker` | `tool_service` sandbox |
| Sub-agent | `ToolInvoker` as agent-as-tool | `tool_service` / agent host / future remote-agent adapter |
| Durable or side-effecting activity | `DurableActivity` | Temporal worker, with approval/idempotency policy |
| Memory | `MemoryPort` | `memory_service` |
| Guardrail / policy | `PolicyPort` | `policy_service` |
| Future untrusted runtime | `ToolInvoker` or capability-specific port | WASM/container host |

4. **Give every governed unit the same minimum contract.** A unit must have:

- stable identity and version
- typed input and output schemas
- capability kind and host type
- isolation outside `inference_service`, unless the capability is explicitly safe local platform data
- tenant grant and limits, checked at publish time and call time
- fail-closed policy behavior
- platform-managed credentials, never secrets in specs or code bundles
- tracing and boundary audit
- lifecycle state and owner
- tests for schema, timeout, error, and audit behavior

5. **Make runs attributable, not bit-exact replayable.** Custom code, external APIs, and side
   effects are not deterministic. The platform cannot promise to recreate an old run byte for byte.
   It can and must record what produced the behavior.

The target run record is:

```text
agent_spec_hash
+ resolved capability IDs
+ capability schema hashes
+ implementation versions
+ host/runtime identities
+ credential binding IDs
+ policy IDs
+ model/effective_base_id
```

Only fields with real producers should be stored. Today that means pinning the parts that already
exist and are hard to recover later, such as `agent_spec_hash`, `effective_base_id`, toolset hash,
and data snapshot set. Policy ids, credential binding ids, catalog ids, and schema hashes land with
the services that produce them.

This resolved tuple is what audit, eval, training attribution, and promotion compare against. This
model strengthens the self-improving loop because each tool, MCP server, activity, memory policy, and
sub-agent used by a run can be identified and compared.

6. **Pick isolation by risk.**

- Safe platform reads, such as vector search over tenant-owned retrieval data, can run locally.
- External or world-acting tools run in `tool_service`.
- Long-running or side-effecting work runs through Temporal with approval and idempotency policy.
- Arbitrary untrusted code runs in a stronger host, such as WASM or containers, when that host exists.

The host follows the capability's risk, not whether the capability was written as code or config.

7. **Use MCP as the first external-tool protocol.** MCP is a good fit for custom tools and external
   tool servers. It should sit behind `tool_service`, and later behind `tool_catalog_service`. It is
   not the whole SDK. It does not cover memory, policy, durable activities, streaming, trajectories,
   training attribution, or every future runtime.

8. **Make the SDK an authoring tool, not a second runtime.** SDKs and CLIs should let developers
   write real Go/Python/WASM/containerized logic, test it locally, generate schemas, package it,
   publish it, attach credentials, and bind it to agents. The SDK may also validate and publish
   agent specs. It must not become an unmanaged agent runtime.

9. **Treat sub-agents as governed capabilities.** A sub-agent is an `AgentSpec` reference or
   remote-agent adapter with its own model, tools, memory, budget, policy, and trajectory. Parent
   agents call it as a bounded handoff with recursion limits, timeouts, and parent/child run linkage.

10. **Keep streaming, tracing, and token accounting in the platform.** Intermediate model events,
    tool calls, sub-agent events, errors, usage, and cost flow through the platform event/socket path
    and are persisted in trajectories or observability stores. They are not hidden inside developer
    code.

## Capability Mapping

| Capability surface | BigHill shape |
|-------------------------|------------------|
| Programmatic agent construction | Spec SDK/CLI plus managed runtime. Custom logic enters as governed capabilities. |
| YAML agent/task configuration | `AgentSpec` is the publishable artifact. Task/workflow specs arrive as vertical slices. |
| Custom tools | Tool SDK plus `tool_service`; later WASM/container hosts for arbitrary code. |
| MCP servers | First external-tool protocol behind `tool_service`; later governed by catalog and tenant policy. |
| Memory backends | `MemoryPort` and `memory_service`; spec binds memory policy by ID |
| Guardrails | `PolicyPort` and `policy_service`; local guardrails only while they are real and auditable. |
| Sub-agents / agent-as-tool | Sub-agent references or remote-agent adapters through `ToolInvoker`. |
| Execution plans / approval | Durable activities through Temporal, with approval and idempotency policy. |
| Streaming / intermediate messages | `socket_service` plus trajectory event streams. |
| Tracing | OpenTelemetry plus trajectory/boundary audit |
| Token usage | Per-run, per-step, and per-model accounting in trajectory/usage records |
| CLI operations | Spec/capability CLI for validate, diff, test, publish, bind, and inspect |

## Non-Goals

- Do not run untrusted customer code inside `inference_service`.
- Do not make arbitrary agent-loop code upload the primary authoring model.
- Do not add memory, sub-agent, MCP, policy, durable-activity, or WASM schema fields before their
  host/runtime, validation, storage, policy, audit, and tests exist.
- Do not require optional extension services on the hot path. Extension decisions must be projected
  into core-owned binding state, or into resolved capability/catalog projections, before a run starts.
- Do not make BigHill primarily an embeddable SDK. SDKs are authoring and packaging tools; the
  product boundary is the managed platform runtime.

## Implementation Direction

Land this in vertical slices:

1. **Run record:** record the parts with real producers first, especially `agent_spec_hash`,
   `effective_base_id`, data snapshot set, and toolset hash. Add catalog, policy, credential, and
   schema hashes only when their services exist.
2. **Spec SDK/CLI:** validate, canonicalize, diff, publish, and bind `AgentSpec` artifacts.
3. **Tool SDK:** generate manifests, JSON Schemas, test harnesses, MCP wrappers, and contract tests.
4. **MCP bridge in `tool_service`:** register one config-pinned MCP endpoint first, then discover
   tools, invoke calls, normalize errors, and emit audit/events.
5. **`tool_catalog_service`:** add dynamic tool definitions, versions, tenant enablement, credential
   bindings, MCP registrations, and catalog projections when the first design-partner path proves the
   need.
6. **Memory and policy services:** add `MemoryPort` and `PolicyPort` only with service-owned
   runtimes and fail-closed validation paths.
7. **Durable activity host:** model long-running and side-effecting actions as Temporal-backed
   capabilities with approval and idempotency.
8. **Sub-agent references:** add parent/child trajectory linkage, recursion limits, streaming, and
   bounded handoff behavior.
9. **Future untrusted runtime host:** add WASM/container execution only behind the same governed unit
   contract.

## Consequences

- BigHill can match the practical feature set of code-first agent SDKs without giving up the
  multi-tenant runtime boundary.
- Developers can still bring real logic: tools, MCP, memory, policy, durable activities, sub-agents,
  and future untrusted runtimes are allowed through governed hosts.
- The declarative spec remains useful because it is the stable binding for the platform. It is not
  the only place behavior can live.
- Evaluation and training can attribute behavior to exact capability versions and policies, not just
  to the prompt and model.
- This is more work than an in-process SDK. Each capability class needs a host, policy path, audit
  path, and tests. That is the cost of being a governed platform rather than a library.
