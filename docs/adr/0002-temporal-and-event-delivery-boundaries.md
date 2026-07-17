# ADR 0002: Temporal and Event Delivery Boundaries

## Status

Accepted.

## Context

The platform uses Go services, Kafka facts, Postgres service databases, and Temporal workflows.

Some events come from services that write source-of-truth state to Postgres. Other events come from
Temporal workflows, especially training workflows, where not every step has a matching local database
write.

## Decision

Use two delivery patterns:

- Services that write source-of-truth state to Postgres publish service-boundary facts through a
  transactional outbox in the same transaction as the state change.
- Temporal-owned training workflows publish service-boundary facts from Temporal activities.
  Temporal gives those activity calls durable at-least-once execution.
- Kafka remains the fan-out mechanism for facts that cross service boundaries.
- Temporal owns workflow sequencing inside a long-running workflow.
- Consumers must handle duplicates. Training consumers deduplicate by `training_run_id`; registry
  consumers deduplicate by dataset id/version or snapshot ids; model consumers deduplicate by model
  id/version.

## Consequences

The data registry, feature materializer, and model registry use the Postgres outbox when an event
must exist only if the backing state write commits.

Training publishes `model_training_completed` and `model_training_failed` from workflow activities.
Those activity calls may be retried, so duplicate delivery is possible and expected.
