CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE OR REPLACE FUNCTION updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE IF NOT EXISTS bighill_agent_registry_db.agent_lineages (
    org_id uuid NOT NULL,
    agent_lineage text NOT NULL,
    created_by_user_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, agent_lineage),
    CONSTRAINT agent_lineages_lineage_ck CHECK (btrim(agent_lineage) <> '')
);

CREATE TRIGGER agent_lineages_updated_at
BEFORE UPDATE ON bighill_agent_registry_db.agent_lineages
FOR EACH ROW
EXECUTE FUNCTION updated_at_column();

CREATE TABLE IF NOT EXISTS bighill_agent_registry_db.agent_spec_versions (
    org_id uuid NOT NULL,
    agent_lineage text NOT NULL,
    agent_spec_hash text NOT NULL,
    model_id uuid NOT NULL,
    registered_by_user_id uuid NOT NULL,
    registered_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, agent_spec_hash),
    FOREIGN KEY (org_id, agent_lineage)
        REFERENCES bighill_agent_registry_db.agent_lineages(org_id, agent_lineage)
        ON DELETE CASCADE,
    CONSTRAINT agent_spec_versions_hash_ck CHECK (btrim(agent_spec_hash) <> '')
);

CREATE INDEX IF NOT EXISTS index_agent_spec_versions_lineage
ON bighill_agent_registry_db.agent_spec_versions(org_id, agent_lineage, registered_at DESC);

CREATE TABLE IF NOT EXISTS bighill_agent_registry_db.agent_endpoint_bindings (
    org_id uuid NOT NULL,
    agent_lineage text NOT NULL,
    endpoint_id uuid NOT NULL,
    created_by_user_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, endpoint_id),
    FOREIGN KEY (org_id, agent_lineage)
        REFERENCES bighill_agent_registry_db.agent_lineages(org_id, agent_lineage)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS index_agent_endpoint_bindings_lineage
ON bighill_agent_registry_db.agent_endpoint_bindings(org_id, agent_lineage, created_at);

CREATE TABLE IF NOT EXISTS bighill_agent_registry_db.agent_champion_states (
    org_id uuid NOT NULL,
    agent_lineage text NOT NULL,
    champion_agent_spec_hash text NOT NULL,
    champion_adapter_id uuid,
    serving_model_id uuid,
    previous_agent_spec_hash text NOT NULL DEFAULT '',
    decision_id uuid NOT NULL,
    decided_by uuid NOT NULL,
    decided_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, agent_lineage),
    FOREIGN KEY (org_id, agent_lineage)
        REFERENCES bighill_agent_registry_db.agent_lineages(org_id, agent_lineage)
        ON DELETE CASCADE,
    CONSTRAINT agent_champion_states_hash_ck CHECK (btrim(champion_agent_spec_hash) <> '')
);

CREATE INDEX IF NOT EXISTS index_agent_champion_states_decided_at
ON bighill_agent_registry_db.agent_champion_states(decided_at DESC);

CREATE TRIGGER agent_champion_states_updated_at
BEFORE UPDATE ON bighill_agent_registry_db.agent_champion_states
FOR EACH ROW
EXECUTE FUNCTION updated_at_column();

CREATE TYPE bighill_agent_registry_db.golden_task_split_enum AS ENUM (
    'SEED_TRAIN',
    'DEV_EVAL',
    'PROMOTION_HOLDOUT'
);

CREATE TABLE IF NOT EXISTS bighill_agent_registry_db.golden_tasks (
    task_id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    org_id uuid NOT NULL,
    agent_lineage text NOT NULL,
    split bighill_agent_registry_db.golden_task_split_enum NOT NULL,
    split_version integer NOT NULL,
    group_key text NOT NULL DEFAULT '',
    prompt text NOT NULL,
    normalized_prompt_hash text NOT NULL,
    content_fingerprint text NOT NULL,
    near_duplicate_fingerprint text NOT NULL,
    expected_tool_plan_hash text NOT NULL DEFAULT '',
    expected_answer text NOT NULL,
    expected_answer_rubric_id text NOT NULL,
    labels_hash text NOT NULL DEFAULT '',
    created_by_user_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (org_id, agent_lineage)
        REFERENCES bighill_agent_registry_db.agent_lineages(org_id, agent_lineage)
        ON DELETE CASCADE,
    CONSTRAINT golden_tasks_split_version_ck CHECK (split_version > 0),
    CONSTRAINT golden_tasks_prompt_ck CHECK (btrim(prompt) <> ''),
    CONSTRAINT golden_tasks_normalized_prompt_hash_ck CHECK (btrim(normalized_prompt_hash) <> ''),
    CONSTRAINT golden_tasks_content_fingerprint_ck CHECK (btrim(content_fingerprint) <> ''),
    CONSTRAINT golden_tasks_near_duplicate_fingerprint_ck CHECK (btrim(near_duplicate_fingerprint) <> ''),
    CONSTRAINT golden_tasks_expected_answer_ck CHECK (btrim(expected_answer) <> ''),
    CONSTRAINT golden_tasks_rubric_ck CHECK (btrim(expected_answer_rubric_id) <> '')
);

CREATE UNIQUE INDEX IF NOT EXISTS unique_golden_tasks_fingerprint
ON bighill_agent_registry_db.golden_tasks(org_id, agent_lineage, split_version, content_fingerprint);

CREATE UNIQUE INDEX IF NOT EXISTS unique_golden_tasks_org_task
ON bighill_agent_registry_db.golden_tasks(org_id, task_id);

CREATE INDEX IF NOT EXISTS index_golden_tasks_near_duplicate
ON bighill_agent_registry_db.golden_tasks(org_id, agent_lineage, split_version, near_duplicate_fingerprint);

CREATE INDEX IF NOT EXISTS index_golden_tasks_split
ON bighill_agent_registry_db.golden_tasks(org_id, agent_lineage, split_version, split, created_at);

CREATE INDEX IF NOT EXISTS index_golden_tasks_group
ON bighill_agent_registry_db.golden_tasks(org_id, agent_lineage, split_version, group_key)
WHERE group_key <> '';

CREATE TABLE IF NOT EXISTS bighill_agent_registry_db.agent_run_labels (
    label_id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    org_id uuid NOT NULL,
    run_id uuid NOT NULL,
    agent_lineage text NOT NULL,
    agent_spec_hash text NOT NULL,
    toolset_hash text NOT NULL,
    effective_base_id text NOT NULL,
    data_snapshot_hash text NOT NULL,
    content_fingerprint text NOT NULL,
    near_duplicate_fingerprint text NOT NULL,
    evaluator text NOT NULL,
    task_success boolean NOT NULL,
    tool_selection_score numeric(8,6) NOT NULL,
    groundedness numeric(8,6) NOT NULL,
    policy_violations integer NOT NULL,
    confidence numeric(8,6) NOT NULL,
    label_source text NOT NULL,
    rubric_version text NOT NULL,
    created_by_user_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (org_id, agent_lineage)
        REFERENCES bighill_agent_registry_db.agent_lineages(org_id, agent_lineage)
        ON DELETE CASCADE,
    CONSTRAINT agent_run_labels_tuple_ck CHECK (
        btrim(agent_spec_hash) <> ''
        AND btrim(toolset_hash) <> ''
        AND btrim(effective_base_id) <> ''
        AND btrim(data_snapshot_hash) <> ''
        AND btrim(content_fingerprint) <> ''
        AND btrim(near_duplicate_fingerprint) <> ''
        AND btrim(evaluator) <> ''
        AND btrim(label_source) <> ''
        AND btrim(rubric_version) <> ''
    ),
    CONSTRAINT agent_run_labels_score_ck CHECK (
        tool_selection_score >= 0 AND tool_selection_score <= 1
        AND groundedness >= 0 AND groundedness <= 1
        AND confidence >= 0 AND confidence <= 1
        AND policy_violations >= 0
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS index_agent_run_labels_unique_evaluator
ON bighill_agent_registry_db.agent_run_labels(org_id, run_id, evaluator, rubric_version);

CREATE INDEX IF NOT EXISTS index_agent_run_labels_lineage
ON bighill_agent_registry_db.agent_run_labels(org_id, agent_lineage, created_at DESC);

CREATE TABLE IF NOT EXISTS bighill_agent_registry_db.agent_trajectory_datasets (
    dataset_id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    org_id uuid NOT NULL,
    agent_lineage text NOT NULL,
    golden_split_version integer NOT NULL,
    content_hash text NOT NULL UNIQUE,
    dataset_uri text NOT NULL,
    format text NOT NULL,
    label_count integer NOT NULL,
    manifest jsonb NOT NULL,
    effective_base_id text NOT NULL,
    agent_spec_hash text NOT NULL,
    toolset_hash text NOT NULL,
    data_snapshot_hash text NOT NULL,
    created_by_user_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (org_id, agent_lineage)
        REFERENCES bighill_agent_registry_db.agent_lineages(org_id, agent_lineage)
        ON DELETE CASCADE,
    CONSTRAINT agent_trajectory_datasets_tuple_ck CHECK (
        golden_split_version > 0
        AND btrim(content_hash) <> ''
        AND btrim(dataset_uri) <> ''
        AND btrim(format) <> ''
        AND label_count > 0
        AND jsonb_typeof(manifest) = 'object'
        AND btrim(effective_base_id) <> ''
        AND btrim(agent_spec_hash) <> ''
        AND btrim(toolset_hash) <> ''
        AND btrim(data_snapshot_hash) <> ''
    )
);

CREATE INDEX IF NOT EXISTS index_agent_trajectory_datasets_lineage
ON bighill_agent_registry_db.agent_trajectory_datasets(org_id, agent_lineage, created_at DESC);

CREATE TABLE IF NOT EXISTS bighill_agent_registry_db.agent_adapters (
    adapter_id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    org_id uuid NOT NULL,
    agent_lineage text NOT NULL,
    dataset_id uuid NOT NULL,
    training_run_id uuid NOT NULL,
    serving_model_id uuid,
    adapter_uri text NOT NULL,
    adapter_checksum text NOT NULL,
    training_provider text NOT NULL,
    trained_against_effective_base_id text NOT NULL,
    trained_against_agent_spec_hash text NOT NULL,
    trained_against_toolset_hash text NOT NULL,
    trained_against_data_snapshot_hash text NOT NULL,
    trained_against_rubric_version text NOT NULL,
    trained_against_golden_split_version integer NOT NULL,
    status text NOT NULL,
    promotion_passed boolean NOT NULL DEFAULT false,
    created_by_user_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (dataset_id)
        REFERENCES bighill_agent_registry_db.agent_trajectory_datasets(dataset_id)
        ON DELETE RESTRICT,
    FOREIGN KEY (org_id, agent_lineage)
        REFERENCES bighill_agent_registry_db.agent_lineages(org_id, agent_lineage)
        ON DELETE CASCADE,
    CONSTRAINT agent_adapters_status_ck CHECK (status IN ('TRAINING', 'CANDIDATE', 'EVALUATED', 'PROMOTED', 'REJECTED', 'FAILED')),
    CONSTRAINT agent_adapters_tuple_ck CHECK (
        btrim(training_provider) <> ''
        AND btrim(trained_against_effective_base_id) <> ''
        AND btrim(trained_against_agent_spec_hash) <> ''
        AND btrim(trained_against_toolset_hash) <> ''
        AND btrim(trained_against_data_snapshot_hash) <> ''
        AND btrim(trained_against_rubric_version) <> ''
        AND trained_against_golden_split_version > 0
        AND (
            status IN ('TRAINING', 'FAILED')
            OR (
                serving_model_id IS NOT NULL
                AND
                btrim(adapter_uri) <> ''
                AND btrim(adapter_checksum) <> ''
            )
        )
    )
);

CREATE INDEX IF NOT EXISTS index_agent_adapters_lineage
ON bighill_agent_registry_db.agent_adapters(org_id, agent_lineage, created_at DESC);

CREATE TRIGGER agent_adapters_updated_at
BEFORE UPDATE ON bighill_agent_registry_db.agent_adapters
FOR EACH ROW
EXECUTE FUNCTION updated_at_column();

CREATE TABLE IF NOT EXISTS bighill_agent_registry_db.agent_eval_reports (
    report_id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    org_id uuid NOT NULL,
    agent_lineage text NOT NULL,
    agent_spec_hash text NOT NULL,
    adapter_id uuid,
    endpoint_id uuid NOT NULL,
    split bighill_agent_registry_db.golden_task_split_enum NOT NULL,
    split_version integer NOT NULL,
    rubric_version text NOT NULL,
    task_count integer NOT NULL,
    task_success_rate double precision NOT NULL,
    tool_success_rate double precision NOT NULL,
    groundedness_rate double precision NOT NULL,
    passed boolean NOT NULL,
    gate_reason text NOT NULL,
    promoted_decision_id uuid,
    evaluated_by uuid NOT NULL,
    evaluated_at timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (org_id, agent_lineage)
        REFERENCES bighill_agent_registry_db.agent_lineages(org_id, agent_lineage)
        ON DELETE CASCADE,
    FOREIGN KEY (org_id, agent_spec_hash)
        REFERENCES bighill_agent_registry_db.agent_spec_versions(org_id, agent_spec_hash)
        ON DELETE CASCADE,
    FOREIGN KEY (adapter_id)
        REFERENCES bighill_agent_registry_db.agent_adapters(adapter_id)
        ON DELETE RESTRICT,
    FOREIGN KEY (org_id, endpoint_id)
        REFERENCES bighill_agent_registry_db.agent_endpoint_bindings(org_id, endpoint_id)
        ON DELETE CASCADE,
    CONSTRAINT agent_eval_reports_hash_ck CHECK (btrim(agent_spec_hash) <> ''),
    CONSTRAINT agent_eval_reports_split_version_ck CHECK (split_version > 0),
    CONSTRAINT agent_eval_reports_rubric_ck CHECK (btrim(rubric_version) <> ''),
    CONSTRAINT agent_eval_reports_task_count_ck CHECK (task_count > 0),
    CONSTRAINT agent_eval_reports_task_success_rate_ck CHECK (task_success_rate >= 0 AND task_success_rate <= 1),
    CONSTRAINT agent_eval_reports_tool_success_rate_ck CHECK (tool_success_rate >= 0 AND tool_success_rate <= 1),
    CONSTRAINT agent_eval_reports_groundedness_rate_ck CHECK (groundedness_rate >= 0 AND groundedness_rate <= 1),
    CONSTRAINT agent_eval_reports_gate_reason_ck CHECK (btrim(gate_reason) <> '')
);

CREATE INDEX IF NOT EXISTS index_agent_eval_reports_lineage
ON bighill_agent_registry_db.agent_eval_reports(org_id, agent_lineage, split_version, evaluated_at DESC);

CREATE INDEX IF NOT EXISTS index_agent_eval_reports_spec
ON bighill_agent_registry_db.agent_eval_reports(org_id, agent_spec_hash, evaluated_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS unique_agent_eval_reports_org_report
ON bighill_agent_registry_db.agent_eval_reports(org_id, report_id);

CREATE TABLE IF NOT EXISTS bighill_agent_registry_db.agent_eval_task_results (
    org_id uuid NOT NULL,
    report_id uuid NOT NULL,
    task_id uuid NOT NULL,
    run_id uuid,
    status text NOT NULL,
    stop_reason text NOT NULL,
    task_success boolean NOT NULL,
    tool_success boolean NOT NULL,
    groundedness boolean NOT NULL,
    failure_reason text NOT NULL DEFAULT '',
    PRIMARY KEY (org_id, report_id, task_id),
    FOREIGN KEY (org_id, report_id)
        REFERENCES bighill_agent_registry_db.agent_eval_reports(org_id, report_id)
        ON DELETE CASCADE,
    FOREIGN KEY (org_id, task_id)
        REFERENCES bighill_agent_registry_db.golden_tasks(org_id, task_id)
        ON DELETE RESTRICT,
    CONSTRAINT agent_eval_task_results_status_ck CHECK (btrim(status) <> ''),
    CONSTRAINT agent_eval_task_results_stop_reason_ck CHECK (btrim(stop_reason) <> '')
);

CREATE INDEX IF NOT EXISTS index_agent_eval_task_results_run
ON bighill_agent_registry_db.agent_eval_task_results(org_id, run_id);

CREATE TABLE IF NOT EXISTS bighill_agent_registry_db.outbox_messages (
    outbox_id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    dispatch_key text NOT NULL UNIQUE,
    topic text NOT NULL,
    event_type text NOT NULL,
    resource_key uuid NOT NULL,
    payload bytea NOT NULL,
    headers jsonb NOT NULL DEFAULT '[]'::jsonb,
    status text NOT NULL DEFAULT 'PENDING',
    attempts integer NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    processing_owner text NOT NULL DEFAULT '',
    claim_token text NOT NULL DEFAULT '',
    lease_expires_at timestamptz,
    last_error text NOT NULL DEFAULT '',
    sent_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT outbox_messages_status_check CHECK (status IN ('PENDING', 'PROCESSING', 'SENT'))
);

CREATE INDEX IF NOT EXISTS index_outbox_messages_pending
ON bighill_agent_registry_db.outbox_messages(status, next_attempt_at, created_at);

CREATE INDEX IF NOT EXISTS index_outbox_messages_processing
ON bighill_agent_registry_db.outbox_messages(status, lease_expires_at, created_at);

CREATE INDEX IF NOT EXISTS index_outbox_messages_resource_key
ON bighill_agent_registry_db.outbox_messages(resource_key, created_at);

ALTER TABLE bighill_agent_registry_db.agent_lineages ENABLE ROW LEVEL SECURITY;
ALTER TABLE bighill_agent_registry_db.agent_lineages FORCE ROW LEVEL SECURITY;
CREATE POLICY agent_lineages_tenant_isolation ON bighill_agent_registry_db.agent_lineages
USING (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
)
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
);

ALTER TABLE bighill_agent_registry_db.agent_spec_versions ENABLE ROW LEVEL SECURITY;
ALTER TABLE bighill_agent_registry_db.agent_spec_versions FORCE ROW LEVEL SECURITY;
CREATE POLICY agent_spec_versions_tenant_isolation ON bighill_agent_registry_db.agent_spec_versions
USING (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
)
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
);

ALTER TABLE bighill_agent_registry_db.agent_endpoint_bindings ENABLE ROW LEVEL SECURITY;
ALTER TABLE bighill_agent_registry_db.agent_endpoint_bindings FORCE ROW LEVEL SECURITY;
CREATE POLICY agent_endpoint_bindings_tenant_isolation ON bighill_agent_registry_db.agent_endpoint_bindings
USING (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
)
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
);

ALTER TABLE bighill_agent_registry_db.agent_champion_states ENABLE ROW LEVEL SECURITY;
ALTER TABLE bighill_agent_registry_db.agent_champion_states FORCE ROW LEVEL SECURITY;
CREATE POLICY agent_champion_states_tenant_isolation ON bighill_agent_registry_db.agent_champion_states
USING (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
)
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
);

ALTER TABLE bighill_agent_registry_db.golden_tasks ENABLE ROW LEVEL SECURITY;
ALTER TABLE bighill_agent_registry_db.golden_tasks FORCE ROW LEVEL SECURITY;
CREATE POLICY golden_tasks_tenant_isolation ON bighill_agent_registry_db.golden_tasks
USING (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
)
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
);

ALTER TABLE bighill_agent_registry_db.agent_run_labels ENABLE ROW LEVEL SECURITY;
ALTER TABLE bighill_agent_registry_db.agent_run_labels FORCE ROW LEVEL SECURITY;
CREATE POLICY agent_run_labels_tenant_isolation ON bighill_agent_registry_db.agent_run_labels
USING (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
)
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
);

ALTER TABLE bighill_agent_registry_db.agent_trajectory_datasets ENABLE ROW LEVEL SECURITY;
ALTER TABLE bighill_agent_registry_db.agent_trajectory_datasets FORCE ROW LEVEL SECURITY;
CREATE POLICY agent_trajectory_datasets_tenant_isolation ON bighill_agent_registry_db.agent_trajectory_datasets
USING (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
)
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
);

ALTER TABLE bighill_agent_registry_db.agent_adapters ENABLE ROW LEVEL SECURITY;
ALTER TABLE bighill_agent_registry_db.agent_adapters FORCE ROW LEVEL SECURITY;
CREATE POLICY agent_adapters_tenant_isolation ON bighill_agent_registry_db.agent_adapters
USING (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
)
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
);

ALTER TABLE bighill_agent_registry_db.agent_eval_reports ENABLE ROW LEVEL SECURITY;
ALTER TABLE bighill_agent_registry_db.agent_eval_reports FORCE ROW LEVEL SECURITY;
CREATE POLICY agent_eval_reports_tenant_isolation ON bighill_agent_registry_db.agent_eval_reports
USING (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
)
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
);

ALTER TABLE bighill_agent_registry_db.agent_eval_task_results ENABLE ROW LEVEL SECURITY;
ALTER TABLE bighill_agent_registry_db.agent_eval_task_results FORCE ROW LEVEL SECURITY;
CREATE POLICY agent_eval_task_results_tenant_isolation ON bighill_agent_registry_db.agent_eval_task_results
USING (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
)
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
);
