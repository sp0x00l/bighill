CREATE TYPE inference_model_status_enum AS ENUM ('PENDING', 'CANDIDATE', 'EVALUATED', 'READY', 'FAILED');
CREATE TYPE inference_model_load_status_enum AS ENUM ('NOT_LOADED', 'LOADED', 'FAILED');
CREATE TYPE inference_model_kind_enum AS ENUM ('BASE', 'FINE_TUNED');
CREATE TYPE inference_model_source_enum AS ENUM ('TRAINING', 'UPLOAD', 'HUGGING_FACE');
CREATE TYPE serving_protocol_enum AS ENUM ('OLLAMA_GENERATE', 'OPENAI_CHAT_COMPLETIONS');
CREATE TYPE inference_dataset_processing_state_enum AS ENUM ('PENDING', 'RAW_MATERIALIZED', 'FEATURE_MATERIALIZED', 'EMBEDDINGS_MATERIALIZED', 'GRAPH_MATERIALIZED', 'FAILED');
CREATE TYPE inference_request_status_enum AS ENUM ('COMPLETED', 'FAILED');
CREATE TYPE table_format_enum AS ENUM ('PARQUET', 'ICEBERG');
CREATE TYPE catalog_provider_enum AS ENUM ('LOCAL', 'POLARIS');
CREATE TYPE processing_profile_enum AS ENUM (
    'GENERIC_PARQUET_PROCESSING_PROFILE',
    'TEXT_RAG_PROCESSING_PROFILE',
    'INSTRUCTION_TUNING_PROCESSING_PROFILE'
);
CREATE TYPE agent_spec_status_enum AS ENUM ('DRAFT', 'VALIDATED', 'PROMOTED', 'FAILED');
CREATE TYPE agent_endpoint_mode_enum AS ENUM ('rag', 'agent');
CREATE TYPE agent_run_status_enum AS ENUM ('RUNNING', 'COMPLETED', 'FAILED');
CREATE TYPE agent_stop_reason_enum AS ENUM ('FINAL_ANSWER', 'MAX_STEPS', 'BUDGET_EXCEEDED', 'TOOL_ERROR', 'RUNTIME_ERROR', 'LOOP_DETECTED', 'DEADLINE_EXCEEDED', 'ABANDONED');
CREATE TYPE tool_error_type_enum AS ENUM ('TRANSIENT', 'PERMANENT', 'POLICY_DENIED');

CREATE EXTENSION IF NOT EXISTS citext;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE OR REPLACE FUNCTION updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE IF NOT EXISTS bighill_inference_db.tenants (
    id uuid PRIMARY KEY,
    email citext NOT NULL DEFAULT '',
    huggingface_token_ciphertext text NOT NULL DEFAULT '',
    deleted boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS index_tenants_deleted
ON bighill_inference_db.tenants(deleted, updated_at);

CREATE TRIGGER tenants_updated_at
BEFORE UPDATE ON bighill_inference_db.tenants
FOR EACH ROW
EXECUTE FUNCTION updated_at_column();

CREATE TABLE IF NOT EXISTS bighill_inference_db.inference_models (
    model_id uuid PRIMARY KEY,
    user_id uuid REFERENCES bighill_inference_db.tenants(id),
    org_id uuid,
    training_run_id uuid,
    dataset_id uuid,
    idempotency_key uuid NOT NULL,
    model_kind inference_model_kind_enum NOT NULL DEFAULT 'FINE_TUNED',
    source inference_model_source_enum NOT NULL DEFAULT 'TRAINING',
    source_uri text NOT NULL DEFAULT '',
    source_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    name text NOT NULL,
    lineage_name text NOT NULL,
    model_version integer NOT NULL,
    base_model text NOT NULL,
    artifact_location text NOT NULL DEFAULT '',
    artifact_format text NOT NULL DEFAULT '',
    artifact_checksum text NOT NULL DEFAULT '',
    artifact_size_bytes bigint NOT NULL DEFAULT 0,
    adapter_uri text NOT NULL DEFAULT '',
    serving_target text NOT NULL DEFAULT '',
    serving_model text NOT NULL DEFAULT '',
    serving_protocol serving_protocol_enum,
    serving_load_status inference_model_load_status_enum NOT NULL DEFAULT 'NOT_LOADED',
    effective_base_id text NOT NULL DEFAULT '',
    metrics_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    status inference_model_status_enum NOT NULL,
    failure_reason text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT inference_models_tenant_ownership_ck CHECK (
        user_id IS NOT NULL AND org_id IS NOT NULL
    ),
    CONSTRAINT inference_models_loaded_serving_runtime_ck CHECK (
        serving_load_status <> 'LOADED'
        OR (
            serving_protocol IS NOT NULL
            AND btrim(serving_target) <> ''
            AND btrim(serving_model) <> ''
        )
    )
);

CREATE INDEX IF NOT EXISTS index_inference_models_training_run_id
ON bighill_inference_db.inference_models(training_run_id);

CREATE INDEX IF NOT EXISTS index_inference_models_user_id
ON bighill_inference_db.inference_models(user_id);

CREATE INDEX IF NOT EXISTS index_inference_models_org_id
ON bighill_inference_db.inference_models(org_id);

CREATE INDEX IF NOT EXISTS index_inference_models_dataset_id
ON bighill_inference_db.inference_models(dataset_id);

CREATE INDEX IF NOT EXISTS index_inference_models_status
ON bighill_inference_db.inference_models(status);

CREATE INDEX IF NOT EXISTS index_inference_models_effective_base_id
ON bighill_inference_db.inference_models(effective_base_id)
WHERE btrim(effective_base_id) <> '';

CREATE TRIGGER inference_models_updated_at
BEFORE UPDATE ON bighill_inference_db.inference_models
FOR EACH ROW
EXECUTE FUNCTION updated_at_column();

CREATE TABLE IF NOT EXISTS bighill_inference_db.inference_datasets (
    dataset_id uuid PRIMARY KEY,
    user_id uuid NOT NULL REFERENCES bighill_inference_db.tenants(id),
    org_id uuid NOT NULL,
    idempotency_key uuid NOT NULL UNIQUE,
    dataset_version integer NOT NULL DEFAULT 1,
    processing_state inference_dataset_processing_state_enum NOT NULL DEFAULT 'PENDING',
    storage_location text NOT NULL DEFAULT '',
    table_namespace text NOT NULL DEFAULT '',
    table_name text NOT NULL DEFAULT '',
    table_format table_format_enum NOT NULL DEFAULT 'PARQUET',
    catalog_provider catalog_provider_enum NOT NULL DEFAULT 'LOCAL',
    processing_profile processing_profile_enum NOT NULL DEFAULT 'GENERIC_PARQUET_PROCESSING_PROFILE',
    schema_version integer NOT NULL DEFAULT 1,
    schema_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    raw_snapshot_id uuid,
    feature_snapshot_id uuid,
    embedding_snapshot_id uuid,
    vector_store text NOT NULL DEFAULT '',
    collection_name text NOT NULL DEFAULT '',
    embedding_dimensions integer NOT NULL DEFAULT 0,
    embedding_count bigint NOT NULL DEFAULT 0,
    embedding_strategy_version text NOT NULL DEFAULT '',
    embedding_chunker_name text NOT NULL DEFAULT '',
    embedding_chunker_version text NOT NULL DEFAULT '',
    embedding_chunk_size integer NOT NULL DEFAULT 0,
    embedding_chunk_overlap integer NOT NULL DEFAULT 0,
    embedding_provider text NOT NULL DEFAULT '',
    embedding_model text NOT NULL DEFAULT '',
    graph_snapshot_id uuid,
    graph_provenance_hash text NOT NULL DEFAULT '',
    graph_node_count bigint NOT NULL DEFAULT 0,
    graph_edge_count bigint NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS index_inference_datasets_processing_state
ON bighill_inference_db.inference_datasets(processing_state);

CREATE INDEX IF NOT EXISTS index_inference_datasets_org_id
ON bighill_inference_db.inference_datasets(org_id);

CREATE INDEX IF NOT EXISTS index_inference_datasets_embedding_snapshot_id
ON bighill_inference_db.inference_datasets(embedding_snapshot_id);
CREATE INDEX IF NOT EXISTS index_inference_datasets_graph_snapshot_id
ON bighill_inference_db.inference_datasets(graph_snapshot_id);

CREATE TRIGGER inference_datasets_updated_at
BEFORE UPDATE ON bighill_inference_db.inference_datasets
FOR EACH ROW
EXECUTE FUNCTION updated_at_column();

CREATE TABLE IF NOT EXISTS bighill_inference_db.inference_requests (
    request_id uuid PRIMARY KEY,
    user_id uuid NOT NULL REFERENCES bighill_inference_db.tenants(id),
    org_id uuid NOT NULL,
    dataset_id uuid NOT NULL,
    model_id uuid,
    embedding_snapshot_id uuid,
    query_text text NOT NULL,
    top_k integer NOT NULL,
    metadata_filters jsonb NOT NULL DEFAULT '{}'::jsonb,
    retrieved_context_ids jsonb NOT NULL DEFAULT '[]'::jsonb,
    retrieved_contexts jsonb NOT NULL DEFAULT '[]'::jsonb,
    prompt_text text NOT NULL DEFAULT '',
    answer_text text NOT NULL DEFAULT '',
    prompt_strategy_version text NOT NULL,
    generation_protocol text NOT NULL DEFAULT '',
    generation_model text NOT NULL,
    latency_ms bigint NOT NULL,
    status inference_request_status_enum NOT NULL,
    error_message text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS index_inference_requests_dataset_id
ON bighill_inference_db.inference_requests(dataset_id);

CREATE INDEX IF NOT EXISTS index_inference_requests_user_id
ON bighill_inference_db.inference_requests(user_id);

CREATE INDEX IF NOT EXISTS index_inference_requests_org_id
ON bighill_inference_db.inference_requests(org_id);

CREATE INDEX IF NOT EXISTS index_inference_requests_embedding_snapshot_id
ON bighill_inference_db.inference_requests(embedding_snapshot_id);

CREATE INDEX IF NOT EXISTS index_inference_requests_created_at
ON bighill_inference_db.inference_requests(created_at);

CREATE TABLE IF NOT EXISTS bighill_inference_db.inference_feedback (
    feedback_id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    idempotency_key uuid NOT NULL UNIQUE,
    request_id uuid NOT NULL REFERENCES bighill_inference_db.inference_requests(request_id),
    user_id uuid NOT NULL REFERENCES bighill_inference_db.tenants(id),
    org_id uuid NOT NULL,
    accepted boolean NOT NULL,
    rating integer NOT NULL,
    comment text NOT NULL DEFAULT '',
    preferred_answer text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS index_inference_feedback_request_id
ON bighill_inference_db.inference_feedback(request_id);

CREATE INDEX IF NOT EXISTS index_inference_feedback_user_id
ON bighill_inference_db.inference_feedback(user_id);

CREATE INDEX IF NOT EXISTS index_inference_feedback_org_id
ON bighill_inference_db.inference_feedback(org_id);

CREATE TABLE IF NOT EXISTS bighill_inference_db.preference_examples (
    preference_example_id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    feedback_id uuid NOT NULL UNIQUE REFERENCES bighill_inference_db.inference_feedback(feedback_id),
    request_id uuid NOT NULL,
    user_id uuid NOT NULL REFERENCES bighill_inference_db.tenants(id),
    org_id uuid NOT NULL,
    dataset_id uuid NOT NULL,
    model_id uuid NOT NULL,
    prompt_text text NOT NULL,
    accepted_answer text NOT NULL DEFAULT '',
    rejected_answer text NOT NULL DEFAULT '',
    rating integer NOT NULL,
    feedback_label text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS index_preference_examples_dataset_id
ON bighill_inference_db.preference_examples(dataset_id);

CREATE INDEX IF NOT EXISTS index_preference_examples_user_id
ON bighill_inference_db.preference_examples(user_id);

CREATE INDEX IF NOT EXISTS index_preference_examples_org_id
ON bighill_inference_db.preference_examples(org_id);

CREATE INDEX IF NOT EXISTS index_preference_examples_model_id
ON bighill_inference_db.preference_examples(model_id);

CREATE TABLE IF NOT EXISTS bighill_inference_db.lineage_eval_sets (
    org_id uuid NOT NULL,
    lineage_name text NOT NULL,
    eval_set_version integer NOT NULL,
    eval_dataset_uri text NOT NULL,
    checksum text NOT NULL DEFAULT '',
    example_count integer NOT NULL DEFAULT 0,
    source text NOT NULL CHECK (source IN ('CURATED', 'FROZEN_GEN0')),
    is_active boolean NOT NULL DEFAULT true,
    frozen_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, lineage_name, eval_set_version)
);

CREATE UNIQUE INDEX IF NOT EXISTS index_lineage_eval_sets_one_active
ON bighill_inference_db.lineage_eval_sets(org_id, lineage_name)
WHERE is_active = true;

CREATE TABLE IF NOT EXISTS bighill_inference_db.lineage_eval_examples (
    org_id uuid NOT NULL,
    lineage_name text NOT NULL,
    eval_set_version integer NOT NULL,
    preference_example_id uuid NOT NULL REFERENCES bighill_inference_db.preference_examples(preference_example_id),
    PRIMARY KEY (org_id, lineage_name, eval_set_version, preference_example_id),
    FOREIGN KEY (org_id, lineage_name, eval_set_version)
        REFERENCES bighill_inference_db.lineage_eval_sets(org_id, lineage_name, eval_set_version)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS index_lineage_eval_examples_preference_example_id
ON bighill_inference_db.lineage_eval_examples(preference_example_id);

CREATE TABLE IF NOT EXISTS bighill_inference_db.preference_dataset_snapshots (
    preference_dataset_id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id uuid NOT NULL REFERENCES bighill_inference_db.tenants(id),
    org_id uuid NOT NULL,
    endpoint_id uuid,
    dataset_id uuid,
    dataset_ids uuid[] NOT NULL DEFAULT '{}',
    model_id uuid NOT NULL,
    parent_model_kind inference_model_kind_enum NOT NULL,
    parent_artifact_uri text NOT NULL,
    parent_artifact_checksum text NOT NULL DEFAULT '',
    parent_adapter_uri text NOT NULL,
    parent_base_model text NOT NULL,
    parent_model_name text NOT NULL,
    parent_lineage_name text NOT NULL DEFAULT '',
    parent_model_version integer NOT NULL,
    source_request_id uuid,
    output_uri text NOT NULL,
    evaluation_output_uri text NOT NULL DEFAULT '',
    format text NOT NULL,
    eligibility_policy text NOT NULL,
    example_count integer NOT NULL,
    training_example_count integer NOT NULL DEFAULT 0,
    evaluation_example_count integer NOT NULL DEFAULT 0,
    min_examples integer NOT NULL,
    limit_count integer NOT NULL,
    integrity_key text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS index_preference_dataset_snapshots_dataset_id
ON bighill_inference_db.preference_dataset_snapshots(dataset_id, created_at);

CREATE INDEX IF NOT EXISTS index_preference_dataset_snapshots_user_id
ON bighill_inference_db.preference_dataset_snapshots(user_id, created_at);

CREATE INDEX IF NOT EXISTS index_preference_dataset_snapshots_org_id
ON bighill_inference_db.preference_dataset_snapshots(org_id, created_at);

CREATE INDEX IF NOT EXISTS index_preference_dataset_snapshots_model_id
ON bighill_inference_db.preference_dataset_snapshots(model_id, created_at);

CREATE INDEX IF NOT EXISTS index_preference_dataset_snapshots_endpoint_id
ON bighill_inference_db.preference_dataset_snapshots(endpoint_id, created_at);

CREATE TRIGGER preference_dataset_snapshots_updated_at
BEFORE UPDATE ON bighill_inference_db.preference_dataset_snapshots
FOR EACH ROW
EXECUTE FUNCTION updated_at_column();

CREATE TABLE IF NOT EXISTS bighill_inference_db.published_inference_endpoints (
    endpoint_id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    org_id uuid NOT NULL,
    model_id uuid NOT NULL,
    mode agent_endpoint_mode_enum NOT NULL DEFAULT 'rag',
    agent_spec_id uuid,
    agent_spec_hash text NOT NULL DEFAULT '',
    status text NOT NULL DEFAULT 'ready',
    rag_merge_strategy text NOT NULL DEFAULT 'reranker',
    display_name text NOT NULL,
    created_by_user_id uuid NOT NULL REFERENCES bighill_inference_db.tenants(id),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT published_inference_endpoint_status_ck CHECK (status IN ('ready', 'disabled')),
    CONSTRAINT published_inference_endpoint_rag_merge_strategy_ck CHECK (rag_merge_strategy IN ('reranker', 'score_normalized'))
);

CREATE INDEX IF NOT EXISTS index_published_inference_endpoints_org_id
ON bighill_inference_db.published_inference_endpoints(org_id, status, created_at);

CREATE INDEX IF NOT EXISTS index_published_inference_endpoints_model_id
ON bighill_inference_db.published_inference_endpoints(model_id);

CREATE INDEX IF NOT EXISTS index_published_inference_endpoints_agent_spec_id
ON bighill_inference_db.published_inference_endpoints(agent_spec_id);

CREATE UNIQUE INDEX IF NOT EXISTS index_published_inference_endpoints_natural_key
ON bighill_inference_db.published_inference_endpoints(org_id, model_id);

CREATE TRIGGER published_inference_endpoints_updated_at
BEFORE UPDATE ON bighill_inference_db.published_inference_endpoints
FOR EACH ROW
EXECUTE FUNCTION updated_at_column();

CREATE TABLE IF NOT EXISTS bighill_inference_db.published_endpoint_datasets (
    endpoint_id uuid NOT NULL REFERENCES bighill_inference_db.published_inference_endpoints(endpoint_id) ON DELETE CASCADE,
    dataset_id uuid NOT NULL,
    position integer NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (endpoint_id, dataset_id)
);

CREATE INDEX IF NOT EXISTS index_published_endpoint_datasets_dataset_id
ON bighill_inference_db.published_endpoint_datasets(dataset_id);

CREATE TABLE IF NOT EXISTS bighill_inference_db.capability_reports (
    capability_report_id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    effective_base_id text NOT NULL,
    supports_chat boolean NOT NULL DEFAULT false,
    supports_tool_calls boolean NOT NULL DEFAULT false,
    supports_system_prompt boolean NOT NULL DEFAULT false,
    raw_report jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT capability_reports_effective_base_id_ck CHECK (btrim(effective_base_id) <> '')
);

CREATE INDEX IF NOT EXISTS index_capability_reports_effective_base_id
ON bighill_inference_db.capability_reports(effective_base_id, created_at);

CREATE UNIQUE INDEX IF NOT EXISTS index_capability_reports_effective_base_unique
ON bighill_inference_db.capability_reports(effective_base_id);

CREATE TABLE IF NOT EXISTS bighill_inference_db.agent_specs (
    agent_spec_id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    org_id uuid NOT NULL,
    agent_lineage text NOT NULL,
    system_prompt text NOT NULL DEFAULT '',
    source_yaml text NOT NULL,
    canonical_json jsonb NOT NULL,
    schema_version text NOT NULL,
    content_hash text NOT NULL,
    validation_report text NOT NULL DEFAULT '',
    model_id uuid NOT NULL,
    tool_bindings jsonb NOT NULL DEFAULT '[]'::jsonb,
    retrieval_config jsonb NOT NULL DEFAULT '{}'::jsonb,
    budgets jsonb NOT NULL DEFAULT '{}'::jsonb,
    stop_conditions jsonb NOT NULL DEFAULT '{}'::jsonb,
    guardrails jsonb NOT NULL DEFAULT '{}'::jsonb,
    status agent_spec_status_enum NOT NULL DEFAULT 'DRAFT',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT agent_specs_content_hash_ck CHECK (btrim(content_hash) <> ''),
    CONSTRAINT agent_specs_budgets_ck CHECK (
        budgets ? 'max_steps'
        AND budgets ? 'token'
        AND budgets ? 'wall_ms'
        AND (budgets->>'max_steps')::integer > 0
        AND (budgets->>'token')::integer > 0
        AND (budgets->>'wall_ms')::integer >= 1000
    ),
    CONSTRAINT agent_specs_unique_content_hash UNIQUE (org_id, content_hash)
);

CREATE INDEX IF NOT EXISTS index_agent_specs_org_lineage
ON bighill_inference_db.agent_specs(org_id, agent_lineage, created_at);

CREATE TABLE IF NOT EXISTS bighill_inference_db.agent_runs (
    run_id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    org_id uuid NOT NULL,
    user_id uuid NOT NULL REFERENCES bighill_inference_db.tenants(id),
    endpoint_id uuid REFERENCES bighill_inference_db.published_inference_endpoints(endpoint_id),
    agent_spec_hash text NOT NULL,
    toolset_hash text NOT NULL,
    trajectory_schema_version text NOT NULL,
    decoding_params jsonb NOT NULL,
    status agent_run_status_enum NOT NULL DEFAULT 'RUNNING',
    stop_reason agent_stop_reason_enum,
    started_at timestamptz NOT NULL DEFAULT now(),
    deadline_at timestamptz NOT NULL,
    finished_at timestamptz,
    total_tokens integer NOT NULL DEFAULT 0,
    wall_ms integer NOT NULL,
    CONSTRAINT agent_runs_tuple_ck CHECK (
        btrim(agent_spec_hash) <> ''
        AND btrim(toolset_hash) <> ''
        AND btrim(trajectory_schema_version) <> ''
        AND jsonb_typeof(decoding_params) = 'object'
        AND decoding_params <> '{}'::jsonb
        AND wall_ms >= 1000
        AND deadline_at > started_at
    )
);

CREATE INDEX IF NOT EXISTS index_agent_runs_org_endpoint
ON bighill_inference_db.agent_runs(org_id, endpoint_id, started_at);

CREATE INDEX IF NOT EXISTS index_agent_runs_status_deadline
ON bighill_inference_db.agent_runs(status, deadline_at);

CREATE TABLE IF NOT EXISTS bighill_inference_db.agent_steps (
    step_id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    run_id uuid NOT NULL REFERENCES bighill_inference_db.agent_runs(run_id) ON DELETE CASCADE,
    org_id uuid NOT NULL,
    step_index integer NOT NULL,
    presented_tool_schemas jsonb NOT NULL,
    generation_result jsonb NOT NULL,
    finish_reason text NOT NULL,
    prompt_tokens integer NOT NULL DEFAULT 0,
    completion_tokens integer NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT agent_steps_unique_index UNIQUE (run_id, step_index),
    CONSTRAINT agent_steps_payload_ck CHECK (
        jsonb_typeof(presented_tool_schemas) = 'array'
        AND jsonb_typeof(generation_result) = 'object'
        AND generation_result <> '{}'::jsonb
    )
);

CREATE TABLE IF NOT EXISTS bighill_inference_db.agent_tool_invocations (
    invocation_id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    step_id uuid NOT NULL REFERENCES bighill_inference_db.agent_steps(step_id) ON DELETE CASCADE,
    run_id uuid NOT NULL REFERENCES bighill_inference_db.agent_runs(run_id) ON DELETE CASCADE,
    org_id uuid NOT NULL,
    tool_name text NOT NULL,
    tool_impl_version text NOT NULL,
    arguments jsonb NOT NULL,
    result jsonb NOT NULL,
    error_type tool_error_type_enum,
    latency_ms bigint NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT agent_tool_invocations_payload_ck CHECK (
        jsonb_typeof(arguments) = 'object'
        AND arguments <> '{}'::jsonb
        AND jsonb_typeof(result) = 'object'
        AND result <> '{}'::jsonb
    )
);

CREATE INDEX IF NOT EXISTS index_agent_tool_invocations_run_id
ON bighill_inference_db.agent_tool_invocations(run_id, created_at);

CREATE TABLE IF NOT EXISTS bighill_inference_db.outbox_messages (
    outbox_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    dispatch_key text NOT NULL UNIQUE,
    topic text NOT NULL,
    event_type text NOT NULL,
    resource_key uuid NOT NULL,
    payload bytea NOT NULL,
    headers jsonb NOT NULL DEFAULT '[]'::jsonb,
    status text NOT NULL DEFAULT 'PENDING',
    attempts integer NOT NULL DEFAULT 0,
    processing_owner text NOT NULL DEFAULT '',
    claim_token text NOT NULL DEFAULT '',
    lease_expires_at timestamptz,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    last_error text NOT NULL DEFAULT '',
    sent_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT outbox_messages_status_check CHECK (status IN ('PENDING', 'PROCESSING', 'SENT'))
);

CREATE INDEX IF NOT EXISTS index_outbox_messages_pending
ON bighill_inference_db.outbox_messages(status, next_attempt_at, created_at);

CREATE INDEX IF NOT EXISTS index_outbox_messages_processing
ON bighill_inference_db.outbox_messages(status, lease_expires_at, created_at);

CREATE INDEX IF NOT EXISTS index_outbox_messages_resource_key
ON bighill_inference_db.outbox_messages(resource_key, created_at);

CREATE TRIGGER outbox_messages_updated_at
BEFORE UPDATE ON bighill_inference_db.outbox_messages
FOR EACH ROW
EXECUTE FUNCTION updated_at_column();

ALTER TABLE bighill_inference_db.tenants ENABLE ROW LEVEL SECURITY;
ALTER TABLE bighill_inference_db.tenants FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_projection_isolation ON bighill_inference_db.tenants
USING (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_user_id', true), '')::uuid = id
)
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_user_id', true), '')::uuid = id
);

ALTER TABLE bighill_inference_db.inference_models ENABLE ROW LEVEL SECURITY;
ALTER TABLE bighill_inference_db.inference_models FORCE ROW LEVEL SECURITY;
CREATE POLICY inference_models_tenant_isolation ON bighill_inference_db.inference_models
USING (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
)
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
);

ALTER TABLE bighill_inference_db.inference_datasets ENABLE ROW LEVEL SECURITY;
ALTER TABLE bighill_inference_db.inference_datasets FORCE ROW LEVEL SECURITY;
CREATE POLICY inference_datasets_tenant_isolation ON bighill_inference_db.inference_datasets
USING (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
)
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
);

ALTER TABLE bighill_inference_db.inference_requests ENABLE ROW LEVEL SECURITY;
ALTER TABLE bighill_inference_db.inference_requests FORCE ROW LEVEL SECURITY;
CREATE POLICY inference_requests_tenant_isolation ON bighill_inference_db.inference_requests
USING (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
)
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
);

ALTER TABLE bighill_inference_db.inference_feedback ENABLE ROW LEVEL SECURITY;
ALTER TABLE bighill_inference_db.inference_feedback FORCE ROW LEVEL SECURITY;
CREATE POLICY inference_feedback_tenant_isolation ON bighill_inference_db.inference_feedback
USING (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
)
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
);

ALTER TABLE bighill_inference_db.preference_examples ENABLE ROW LEVEL SECURITY;
ALTER TABLE bighill_inference_db.preference_examples FORCE ROW LEVEL SECURITY;
CREATE POLICY preference_examples_tenant_isolation ON bighill_inference_db.preference_examples
USING (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
)
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
);

ALTER TABLE bighill_inference_db.lineage_eval_sets ENABLE ROW LEVEL SECURITY;
ALTER TABLE bighill_inference_db.lineage_eval_sets FORCE ROW LEVEL SECURITY;
CREATE POLICY lineage_eval_sets_tenant_isolation ON bighill_inference_db.lineage_eval_sets
USING (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
)
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
);

ALTER TABLE bighill_inference_db.lineage_eval_examples ENABLE ROW LEVEL SECURITY;
ALTER TABLE bighill_inference_db.lineage_eval_examples FORCE ROW LEVEL SECURITY;
CREATE POLICY lineage_eval_examples_tenant_isolation ON bighill_inference_db.lineage_eval_examples
USING (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
)
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
);

ALTER TABLE bighill_inference_db.preference_dataset_snapshots ENABLE ROW LEVEL SECURITY;
ALTER TABLE bighill_inference_db.preference_dataset_snapshots FORCE ROW LEVEL SECURITY;
CREATE POLICY preference_dataset_snapshots_tenant_isolation ON bighill_inference_db.preference_dataset_snapshots
USING (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
)
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
);

ALTER TABLE bighill_inference_db.published_inference_endpoints ENABLE ROW LEVEL SECURITY;
ALTER TABLE bighill_inference_db.published_inference_endpoints FORCE ROW LEVEL SECURITY;
CREATE POLICY published_inference_endpoints_tenant_isolation ON bighill_inference_db.published_inference_endpoints
USING (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
)
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
);

ALTER TABLE bighill_inference_db.published_endpoint_datasets ENABLE ROW LEVEL SECURITY;
ALTER TABLE bighill_inference_db.published_endpoint_datasets FORCE ROW LEVEL SECURITY;
CREATE POLICY published_endpoint_datasets_tenant_isolation ON bighill_inference_db.published_endpoint_datasets
USING (
    current_setting('app.system_context', true) = 'true'
    OR EXISTS (
        SELECT 1
        FROM bighill_inference_db.published_inference_endpoints endpoint
        WHERE endpoint.endpoint_id = published_endpoint_datasets.endpoint_id
          AND NULLIF(current_setting('app.current_org_id', true), '')::uuid = endpoint.org_id
    )
)
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
    OR EXISTS (
        SELECT 1
        FROM bighill_inference_db.published_inference_endpoints endpoint
        WHERE endpoint.endpoint_id = published_endpoint_datasets.endpoint_id
          AND NULLIF(current_setting('app.current_org_id', true), '')::uuid = endpoint.org_id
    )
);

ALTER TABLE bighill_inference_db.agent_specs ENABLE ROW LEVEL SECURITY;
ALTER TABLE bighill_inference_db.agent_specs FORCE ROW LEVEL SECURITY;
CREATE POLICY agent_specs_tenant_isolation ON bighill_inference_db.agent_specs
USING (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
)
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
);

ALTER TABLE bighill_inference_db.agent_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE bighill_inference_db.agent_runs FORCE ROW LEVEL SECURITY;
CREATE POLICY agent_runs_tenant_isolation ON bighill_inference_db.agent_runs
USING (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
)
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
);

ALTER TABLE bighill_inference_db.agent_steps ENABLE ROW LEVEL SECURITY;
ALTER TABLE bighill_inference_db.agent_steps FORCE ROW LEVEL SECURITY;
CREATE POLICY agent_steps_tenant_isolation ON bighill_inference_db.agent_steps
USING (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
)
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
);

ALTER TABLE bighill_inference_db.agent_tool_invocations ENABLE ROW LEVEL SECURITY;
ALTER TABLE bighill_inference_db.agent_tool_invocations FORCE ROW LEVEL SECURITY;
CREATE POLICY agent_tool_invocations_tenant_isolation ON bighill_inference_db.agent_tool_invocations
USING (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
)
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
);
