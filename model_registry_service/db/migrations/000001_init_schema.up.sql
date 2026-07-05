CREATE TYPE model_status_enum AS ENUM ('PENDING', 'CANDIDATE', 'EVALUATED', 'READY', 'FAILED');
CREATE TYPE model_load_status_enum AS ENUM ('NOT_LOADED', 'LOADED', 'FAILED');
CREATE TYPE model_kind_enum AS ENUM ('BASE', 'FINE_TUNED');
CREATE TYPE model_source_enum AS ENUM ('TRAINING', 'UPLOAD', 'HUGGING_FACE');

CREATE EXTENSION IF NOT EXISTS citext;

CREATE OR REPLACE FUNCTION updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE IF NOT EXISTS bighill_model_registry_db.tenants (
    id uuid PRIMARY KEY,
    email citext NOT NULL DEFAULT '',
    huggingface_token_ciphertext text NOT NULL DEFAULT '',
    deleted boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS index_tenants_deleted
ON bighill_model_registry_db.tenants(deleted, updated_at);

CREATE TRIGGER tenants_updated_at
BEFORE UPDATE ON bighill_model_registry_db.tenants
FOR EACH ROW
EXECUTE FUNCTION updated_at_column();

CREATE TABLE IF NOT EXISTS bighill_model_registry_db.models (
    model_id uuid PRIMARY KEY,
    user_id uuid REFERENCES bighill_model_registry_db.tenants(id),
    idempotency_key uuid UNIQUE NOT NULL,
    training_run_id uuid,
    dataset_id uuid,
    model_kind model_kind_enum NOT NULL DEFAULT 'FINE_TUNED',
    source model_source_enum NOT NULL DEFAULT 'TRAINING',
    source_uri text NOT NULL DEFAULT '',
    source_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    name text NOT NULL,
    model_version integer NOT NULL,
    base_model text NOT NULL,
    artifact_location text NOT NULL,
    artifact_format text NOT NULL,
    artifact_checksum text NOT NULL,
    artifact_size_bytes bigint NOT NULL,
    adapter_uri text NOT NULL DEFAULT '',
    serving_target text NOT NULL DEFAULT '',
    serving_model text NOT NULL DEFAULT '',
    serving_load_status model_load_status_enum NOT NULL DEFAULT 'NOT_LOADED',
    serving_status_idempotency_key uuid NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000',
    metrics_metadata jsonb NOT NULL,
    promotion_report_uri text NOT NULL DEFAULT '',
    promotion_deltas jsonb NOT NULL DEFAULT '{}'::jsonb,
    status model_status_enum NOT NULL DEFAULT 'PENDING',
    failure_reason text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT models_tenant_ownership_ck CHECK (
        (model_kind = 'BASE' AND user_id IS NULL)
        OR (model_kind <> 'BASE' AND user_id IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS index_models_training_run_id
ON bighill_model_registry_db.models(training_run_id);

CREATE INDEX IF NOT EXISTS index_models_user_id
ON bighill_model_registry_db.models(user_id);

CREATE INDEX IF NOT EXISTS index_models_dataset_id
ON bighill_model_registry_db.models(dataset_id);

CREATE INDEX IF NOT EXISTS index_models_champion_lookup
ON bighill_model_registry_db.models(user_id, name, model_version DESC)
WHERE status = 'READY' AND serving_load_status = 'LOADED';

CREATE TRIGGER models_updated_at
BEFORE UPDATE ON bighill_model_registry_db.models
FOR EACH ROW
EXECUTE FUNCTION updated_at_column();

ALTER TABLE bighill_model_registry_db.tenants ENABLE ROW LEVEL SECURITY;
ALTER TABLE bighill_model_registry_db.tenants FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_projection_isolation ON bighill_model_registry_db.tenants
USING (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_user_id', true), '')::uuid = id
)
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_user_id', true), '')::uuid = id
);

ALTER TABLE bighill_model_registry_db.models ENABLE ROW LEVEL SECURITY;
ALTER TABLE bighill_model_registry_db.models FORCE ROW LEVEL SECURITY;
CREATE POLICY models_tenant_isolation ON bighill_model_registry_db.models
USING (
    current_setting('app.system_context', true) = 'true'
    OR user_id IS NULL
    OR NULLIF(current_setting('app.current_user_id', true), '')::uuid = user_id
)
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
    OR (model_kind = 'BASE' AND user_id IS NULL)
    OR NULLIF(current_setting('app.current_user_id', true), '')::uuid = user_id
);
