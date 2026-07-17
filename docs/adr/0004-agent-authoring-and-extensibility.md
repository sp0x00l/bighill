# ADR 0004: Agent Authoring and Extensibility

## Status

Accepted.

## Context

Peer agent frameworks expose an SDK as the developer authoring surface. The common capability shape
is clear: custom tools, memory, guardrails, tenant or org context, tracing, token accounting,
declarative configuration, MCP tool integration, sub-agents or handoffs, runtime settings,
structured output, and CLI support for authoring and operations.

BigHill is not an embeddable agent SDK. It is a managed platform with service-owned state, governed
runtime execution, tenant isolation, content-addressed agent specs, trajectory capture, owned model
serving, and a self-improving lifecycle. The current authoring surface is intentionally narrower:
write a declarative `AgentSpec` YAML/JSON artifact, validate and canonicalize it, then publish it by
hash. Tools are currently platform-provided (`search_knowledge` locally and `http_get` through
`tool_service`).

The gap is real: developers cannot yet author custom tools, attach MCP servers, wire memory, compose
sub-agents, or package reusable logic. BigHill should match that high-level authoring capability set,
but it should not copy a code-first, in-process agent runtime model. The platform needs an
SDK-shaped authoring story, and the right unit for customer code is the tool, because letting
customer code run inside the inference process would violate the safety boundary that `tool_service`
exists to provide.

## Position

We agree with the strategic direction: BigHill should expose a platform-plus-SDK authoring story with
custom tools, MCP, memory, guardrails, sub-agent composition, streaming, tracing, and token
accounting. We disagree with making arbitrary agent code the primary extension model. Agents should
remain declarative governed artifacts; developer-authored code should enter through sandboxed tools
and platform extension services.

## Decision

Keep agents declarative and make tools the code-extensibility unit.

1. **Agents remain declarative, content-addressed artifacts.** The `AgentSpec` is the governed
   authoring contract for prompts, model binding, retrieval config, tool bindings, budgets, stop
   conditions, and guardrail policy. Future memory config, sub-agent references, approval policy,
   and policy-service bindings are added to the spec only when their runtime slices exist. The spec
   never embeds arbitrary customer code.

2. **The SDK surface is tool-authoring, not agent-runtime embedding.** BigHill should provide SDKs
   and CLIs that help developers author, validate, test, package, and publish tools. Those SDKs may
   also validate and publish declarative agent specs, but the platform remains the runtime. The SDK
   must not become a second in-process agent engine whose behavior bypasses platform validation,
   trajectory capture, or tenant policy.

3. **MCP is the preferred interoperability protocol for customer and third-party tools.** A custom
   tool should be publishable as an MCP server or as a platform-native tool adapter that is exposed
   through the same catalog contract. `tool_service` is the boundary that connects to MCP servers,
   discovers tools, validates schemas, invokes calls, and translates results/errors back to the agent
   loop.

4. **`tool_service` remains the sandbox and choke point.** Any tool that reaches outside inference,
   executes customer code, accesses tenant credentials, calls external APIs, performs SQL, or invokes
   an MCP server runs behind `tool_service`. The boundary enforces tenant allowlists, credential
   binding, argument-schema validation, egress policy, timeouts, response caps, audit, and future
   approval/policy checks. No customer-authored tool runs in-process in `inference_service`.

5. **`tool_catalog_service` becomes the dynamic tool authority.** The catalog owns tool definitions,
   versions, tenant enablement, credential bindings, MCP server registrations, and marketplace-style
   discovery. Until that service exists, V1 keeps the current bounded registries: local
   `search_knowledge` in `inference_service` and remote world-acting tools in `tool_service`.
   `inference_service` should eventually resolve a toolset from catalog projections and pin a
   `resolved_toolset_hash` plus tool implementation versions onto each trajectory.

6. **Sub-agents are declarative references, not arbitrary child runtimes.** A future sub-agent is an
   `AgentSpec` reference or handoff target with its own model/tool/memory/budget policy. It is
   invoked like a bounded tool call, runs with isolated context and explicit permissions, records its
   own trajectory, and has recursion/depth limits. This gives the platform sub-agent composition
   without allowing developer code to reinterpret the parent runtime.

7. **Memory, guardrails, and policy are platform bindings.** Agent specs may bind to memory,
   guardrail, and policy services when those services exist. They do not load customer memory code or
   arbitrary guardrail code into the agent process. Policy decisions remain fail-closed and
   user-visible.

8. **Streaming, tracing, and token accounting are platform runtime capabilities.** The live run path
   should stream model/tool/run events through the platform event/socket path, record trajectory
   steps for audit and training, and attach token/cost/accounting data to runs. These capabilities
   are part of the managed runtime, not optional behavior inside a customer SDK.

9. **The self-improving lifecycle stays the differentiator.** Peer SDKs can embed an agent runtime,
   but they do not own BigHill's platform loop: trajectory -> evaluation -> preference data ->
   adapter training -> promotion gate -> serving reconciliation. Keeping agents as governed
   artifacts preserves the provenance needed for that lifecycle.

## Non-Goals

- Do not make arbitrary agent code upload the primary authoring model.
- Do not execute customer-authored tools inside `inference_service`.
- Do not add memory, sub-agent, MCP, or policy schema fields before their runtime, validation,
  storage, policy, and tests exist.
- Do not require optional extension services on the hot path. Extension decisions must project into
  core-owned binding state or catalog projections before a run starts.
- Do not make BigHill primarily an embeddable SDK. It may ship SDKs, but the product boundary is the
  managed platform runtime.

## Implementation Direction

The authoring stack should land as vertical slices:

1. **Spec SDK/CLI:** validate, canonicalize, diff, publish, and bind `AgentSpec` artifacts against
   the platform schema.
2. **Tool SDK:** generate tool manifests, JSON Schemas, test harnesses, MCP server wrappers, and
   conformance tests for timeout/error/result contracts.
3. **MCP bridge in `tool_service`:** register MCP server endpoints, discover tool schemas, invoke
   tools with tenant credentials and egress policy, normalize errors, and emit boundary audit.
4. **`tool_catalog_service`:** own dynamic tool definitions, versions, tenant enablement, credential
   bindings, and catalog projections consumed by inference/tool-service.
5. **Resolved toolset pinning:** each run records the exact advertised tool definitions, catalog
   versions, MCP server versions where available, and implementation versions.
6. **Memory and policy services:** add spec sections only when there is a service-owned runtime and a
   fail-closed validation path.
7. **Sub-agent references:** introduce declarative sub-agent bindings as bounded handoffs once the
   run/trajectory model supports parent/child relationships, streaming, and recursion limits.

## Consequences

- BigHill aligns with the platform-plus-SDK shape of peer agent systems while preserving a stronger
  multi-tenant safety boundary.
- Developer extensibility exists, but it is intentionally expressed as tools and service bindings,
  not arbitrary agent runtime code.
- The agent spec remains content-addressed and suitable for validation, audit, replay, evaluation,
  training data extraction, and promotion decisions.
- The platform can support MCP without letting MCP servers inherit inference-service secrets or
  network reachability.
- Some highly custom agent logic must be modeled as a tool, a declarative sub-agent, or a platform
  extension service. That is less flexible than an in-process SDK, but it is easier to isolate,
  observe, and improve.
