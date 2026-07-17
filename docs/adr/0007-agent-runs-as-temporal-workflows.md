# ADR 0007: Interactive Agent Runs as Temporal Workflows

## Status

Proposed.

P0 for the agent rails path. Extends [ADR 0002](0002-temporal-and-event-delivery-boundaries.md) (Temporal and event-delivery boundaries) to the agent runtime.

## Context

A multi-step agent run is long-running async work: it makes several sequential model and tool calls, each taking seconds, so a whole run can take tens of seconds to minutes.

The public edge is a REST API Gateway with a short integration timeout. A synchronous agent generation through the gateway is therefore capped by the edge, which makes real multi-step agents infeasible on the request path.

The platform already runs durable async work on Temporal (materialization, training) per ADR 0002. An in-process goroutine execution model for agent runs would introduce a second, weaker paradigm with its own retry, timeout, idempotency, and cleanup machinery.

## Decision

Interactive agent submissions execute through **Temporal workflows**. Delivery to the client is asynchronous over WebSockets via `socket_service`.

V1 is a **step-orchestrated workflow**:

- Workflow code owns the deterministic step loop, budget checks, loop detection, transient-tool retry counters, and stop-condition selection.
- Non-deterministic effects run only in activities:
  - `PrepareAgentRun`
  - `GenerateAgentStep`
  - `RecordAgentStep`
  - `InvokeAgentTool`
  - `RecordAgentToolInvocation`
  - terminal `CompleteAgentRun` / `FailAgentRun`
- Model and tool effect activities have `MaximumAttempts = 1`; a failed effect fails the run honestly instead of replaying non-idempotent calls.
- Record/projection activities use bounded retries and deterministic ids where needed.

This provides step-level durability for completed activities: after a worker crash, Temporal replay resumes from the latest completed workflow event rather than restarting the whole loop. It does not retry partially completed non-idempotent model/tool calls.

The public contract is:

- **Submission.** `POST /v1/private/inference/endpoints/{id}/generations` in agent mode starts the workflow with a deterministic `workflow_id` derived from the idempotency key and returns **`202` + `run_id`**. RAG mode stays synchronous and returns a normal generation response.
- **Deadline.** `wall_ms` is used as the workflow and activity timeout. `deadline_at` is persisted as a read projection.
- **Idempotency.** A duplicate submission with the same key hits the same `workflow_id`; if it is running, the caller receives the existing run location. A failed workflow may be explicitly retried because the workflow id reuse policy permits duplicate failed executions.
- **Delivery.** Agent progress and completion are delivered through `shared_lib/userevents` -> Redis -> `socket_service`.
- **Authoritative record.** WebSocket delivery is live but lossy. `GET /v1/private/inference/agent-runs/{id}` is the authoritative, replayable record.
- **Reaper.** The stale-run reaper is a projection reconciler/backstop for runs left `RUNNING` after abnormal termination.

## Payload Discipline

Workflow state carries the bounded interactive message history, resolved tool schemas, retrieved contexts, token counters, and loop-detection state. This is acceptable for V1 because interactive agent specs are capped by `max_steps`, token budget, and `wall_ms`.

Long-running, side-effecting, or approval-paused agents must not reuse this bounded-state shape. That durable slice must move large payloads such as full message history and tool results behind persisted references and use continue-as-new when histories approach Temporal limits.

## Future Durable-Agent Slice

The future durable-agent slice adds side-effecting tools, human approval pauses, and longer autonomous runs. It must add reference-backed workflow state, continue-as-new, approval activity contracts, and explicit retry/idempotency policy per tool class before enabling retries for effect activities.

## Consequences

- V1 removes the REST edge timeout from agent execution without introducing goroutine-only execution.
- V1 provides step-level orchestration while keeping non-idempotent effects single-attempt.
- V1 has honest failure behavior: no hidden whole-run replay and no duplicate model/tool side effects from Temporal retries.
- WebSocket streaming and Temporal execution remain orthogonal: socket events are emitted by the runtime while Temporal owns async execution and timeout.
- A future durable-agent implementation can extend the same submission and read APIs with reference-backed workflow state.
