# ADR 0002: Temporal and Event Delivery Boundaries

## Status

Accepted

## Context

The platform uses Go services, Kafka facts, Postgres service databases, and Temporal workflows. Some events are emitted by services that own database state. Training workflows are owned by Temporal and do not have a local Postgres state transition for every workflow step.

## Decision

Use two delivery patterns deliberately:

- Data-plane services that persist source-of-truth state in Postgres publish boundary facts through a Postgres transactional outbox in the same transaction as the state change.
- Temporal-owned training workflows publish boundary facts from Temporal activities. Temporal provides durable at-least-once execution for those activity calls.
- Kafka remains for service-boundary facts and fan-out. Temporal owns internal workflow sequencing.
- Consumers must be idempotent. Training consumers deduplicate by `training_run_id`; registry consumers deduplicate by dataset id/version or snapshot ids; model consumers deduplicate by model id/version.

## Consequences

The data registry, feature materializer, and model registry use the Postgres outbox for facts that must exist if and only if their state write commits. Training publishes `model_training_completed` and `model_training_failed` directly from workflow activities, so duplicate delivery is possible and expected under Temporal retries.
