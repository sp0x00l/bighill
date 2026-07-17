# ADR 0003: Effective-Base Identity

## Status

Accepted.

## Context

The self-improving agent loop needs a stable id for the thing that is actually served.

That served artifact starts as a foundation model plus the serving details that affect behavior, such
as artifact format and serving protocol. Later it may also include a shared or tenant-specific
adapter.

Training records, capability checks, and serving compatibility all need to point at the same served
artifact id.

The first attempt used a wide, UUID-keyed, org-scoped `effective_base_versions` table with many
nullable columns. Most of those columns had no writer. That was forward schema: the database claimed
the platform had capabilities that did not exist yet. It was removed.

We still need the identity, but it must grow only when real producers and readers exist.

## Decision

1. **Use a content-addressed id.** `effective_base_id` is a digest over
   `(descriptor_schema_version + canonical descriptor)`, serialized with the shared serializer. It
   is the table primary key. Two byte-identical descriptors produce the same row, so upserts are
   idempotent.

2. **Keep columns small and put the full shape in a versioned descriptor.** First-class columns exist
   only for values this slice writes and reads: `effective_base_id`, `foundation_model_id`,
   `descriptor_schema_version`, `foundation_checksum`, `descriptor`, and timestamps. The full
   composition lives in canonical `descriptor` JSONB. V1 descriptor keys are produced by
   model-registry serving reconciliation: `descriptor_schema_version`, `foundation_model_id`,
   `artifact_uri`, `artifact_format`, `foundation_checksum`, `serving_protocol`, and
   `serving_model`. To extend the identity, bump `descriptor_schema_version` and add keys that a
   real producer writes. Do not add empty columns.

3. **Make it platform-scoped, not org-scoped.** Effective bases are platform artifacts. Identical
   artifacts deduplicate to one digest across tenants. If an org may use a future tenant-merged base,
   model that as a separate grant: "org X may use base Y." Do not put `org_id` on the base row. A
   tenant-merged artifact gets its own digest and a grant to its owning org.

4. **Keep ownership in `model_registry_service`.** `model_registry_service` owns the
   `effective_base_versions` table. In this slice, it computes the V1 descriptor from the model
   record and observed serving status it already owns. A future model-serving producer can add richer
   artifact measurements, but only by writing real descriptor keys. Model registry includes
   `effective_base_id` on the model projection event it already emits.

5. **Key capability checks on the digest.** `capability_reports` keys on `effective_base_id`.
   Capability is measured once per served artifact and shared across tenants. V1 stores only signals
   that are actually probed: `supports_chat`, `supports_tool_calls`, and `supports_system_prompt`.
   `max_output_tokens` is request policy, not artifact capability, so it does not belong here.
   `context_window_tokens` can be added only when a real probe or metadata reader produces it. Do
   not use per-org RLS for this platform-level measurement.

6. **Never mutate identity.** A corrected or recomposed base creates a new digest. Consumers move to
   the new id. If the canonical form changes, bump `descriptor_schema_version` so ids remain stable
   within a descriptor version.

### Deferred

- **Adapter/base compatibility** lands when adapters exist to check. It should be a function over two
  descriptors, not stored flags.
- **A standalone `effective_base.registered` event** lands when a second consumer, such as agent
  registry or eval, needs it. Until then the id rides the existing model-projection event that
  `inference_service` already consumes. Do not add an event with no consumer.

### Guardrail

Extensible means versioned descriptor, computed functions, and events when needed. It does not mean
pre-declared empty columns.

The descriptor can grow. The column set stays tied to real queries. The same rule applies inside
JSONB: a key exists only if a producer wrote it; `false` means known false; `NULL` means unknown only
where a reader handles unknown.

## Consequences

- The same served artifact yields the same id everywhere.
- Capability is probed once per artifact, not once per tenant.
- Foreign keys reference a digest string, matching the `agent_spec_hash` pattern on `agent_runs`.
  This is larger than a UUID, but it gives content-addressed clarity and idempotent deduplication.
- `effective_base_versions` returns only with its producer and reader. This avoids the forward-schema
  pattern that caused the earlier table to be removed.
- Compatibility checks and the standalone registration event are recorded as intended design, but
  are not built until their consumers exist.
