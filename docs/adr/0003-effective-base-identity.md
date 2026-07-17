# ADR 0003: Effective-Base Identity

## Status

Accepted.

## Context

The self-improving agent lifecycle needs a stable identity for the *served artifact* — the concrete thing a model runs as: foundation model + quantization + tokenizer + chat template, and later a shared or tenant-merged adapter. Adapter training provenance, capability measurement, and serving compatibility all need to reference that identity.

The first attempt modeled it as a wide, UUID-keyed, org-scoped `effective_base_versions` table with many nullable attribute columns (merge recipe, shared adapter, tokenizer/template/quant hashes, capability report id) written by nothing. That is forward-schema: a table asserting a capability the platform did not have. It was removed in the V1 honesty remediation. We want the identity to be extensible without reintroducing dormant columns.

## Decision

1. **Content-addressed identity.** `effective_base_id` is a digest over `(descriptor_schema_version + canonical descriptor)`, serialized through the shared serializer, and is the table's primary key. This is the same content-hash-as-reference pattern already used for `agent_spec_hash` on `agent_runs`; two byte-identical artifacts collapse to one row, and upsert is idempotent.

2. **Minimal columns + versioned descriptor.** First-class columns exist only for what this slice writes and reads: `effective_base_id`, `foundation_model_id`, `descriptor_schema_version`, `foundation_checksum`, `descriptor`, and timestamps. The full composition lives in canonical `descriptor` JSONB whose v1 keys are all produced by model-registry serving reconciliation: `descriptor_schema_version`, `foundation_model_id`, `artifact_uri`, `artifact_format`, `foundation_checksum`, `serving_protocol`, and `serving_model`. The identity extends by bumping `descriptor_schema_version` and adding producer-written keys — never by adding empty columns.

3. **Platform-scoped, no `org_id`.** Effective bases are platform artifacts; identical artifacts deduplicate to one digest across tenants. Org access to future tenant-merged bases is modeled as a separate binding/grant relation ("org X may use base Y"), never as a column on the base row. A tenant-merged artifact has its own digest and is granted to its owning org.

4. **Ownership.** `model_registry_service` owns the `effective_base_versions` table (source of truth). In this slice, model-registry computes the v1 descriptor from the model record and observed serving status it already owns. A future model-serving producer can provide richer artifact measurements, but it must write real descriptor keys before the schema claims them. Model-registry stamps `effective_base_id` onto the model projection event it already emits.

5. **Capability keyed on the digest, artifact-intrinsic only.** `capability_reports` keys on `effective_base_id`; capability is measured against the served artifact, once per digest, and shared across tenants. Only artifact-intrinsic signals that are actually probed belong here in V1: `supports_chat`, `supports_tool_calls`, and `supports_system_prompt`. `max_output_tokens` is a request/runtime policy cap and does not belong in this row. `context_window_tokens` can be added only when a real artifact probe or metadata reader produces it. Per-org RLS on capability is deliberately not used for this platform measurement.

6. **Immutability.** Identity is never mutated. A corrected or recomposed base is a new digest that consumers re-point to. A change to the canonical form is a `descriptor_schema_version` bump, so ids stay stable within a version.

### Deferred (no consumer yet — would be phantom)

- **Adapter/base compatibility function** (foundation match, tokenizer/template hash equality, quant compatibility) lands in the serving-compatibility slice, when adapters exist to check. It is a pure function over two descriptors, not stored flags.
- **A standalone `effective_base.registered` event** lands when a second consumer (agent registry or eval) needs it. Until then the id rides the existing model-projection event that `inference_service` already consumes — same producer-push direction, no consumerless event.

### Guardrail

Extensible means *versioned descriptor + computed functions + events*, not pre-declared columns. The descriptor grows; the column set stays query-driven. The honesty rule applies inside the JSONB: a key exists only if a producer wrote it, `false` means known-false, and `NULL` means unknown only where a reader handles unknown.

## Consequences

- Identity is reversible and deduplicated: the same served artifact yields the same id everywhere, and capability is probed once per artifact rather than per tenant.
- Foreign keys reference a digest string, consistent with `agent_spec_hash` on `agent_runs`; we accept a larger key than a UUID surrogate in exchange for content-addressed clarity and idempotent dedup.
- `effective_base_versions` returns to the schema only in the slice that also ships its producer (`model_serving` → `model_registry`) and its reader (capability re-point), reversing the forward-schema pattern that removed it.
- The compatibility function and the standalone registration event are recorded here as intended design but are not built until their consumers exist.
