CREATE EXTENSION IF NOT EXISTS citext;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TYPE table_format_enum AS ENUM ('PARQUET', 'ICEBERG');
CREATE TYPE catalog_provider_enum AS ENUM ('LOCAL', 'POLARIS');
CREATE TYPE processing_profile_enum AS ENUM (
    'GENERIC_PARQUET_PROCESSING_PROFILE',
    'TEXT_RAG_PROCESSING_PROFILE',
    'INSTRUCTION_TUNING_PROCESSING_PROFILE'
);
CREATE TYPE upload_session_status_enum AS ENUM ('PENDING', 'PROMOTED', 'REJECTED', 'EXPIRED');
CREATE TYPE upload_resource_type_enum AS ENUM ('DATA_FILE', 'MODEL_ARTIFACT');
CREATE TYPE model_source_enum AS ENUM ('TRAINING', 'UPLOAD', 'HUGGING_FACE');

CREATE OR REPLACE FUNCTION updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE IF NOT EXISTS bighill_ingestion_db.tenants(
    id uuid PRIMARY KEY,
    email citext NOT NULL DEFAULT '',
    huggingface_token_ciphertext text NOT NULL DEFAULT '',
    deleted boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS index_tenants_deleted
ON bighill_ingestion_db.tenants(deleted, updated_at);

CREATE TRIGGER tenants_updated_at
BEFORE UPDATE ON bighill_ingestion_db.tenants
FOR EACH ROW
EXECUTE FUNCTION updated_at_column();

CREATE TABLE IF NOT EXISTS bighill_ingestion_db.datasets(
    dataset_id uuid NOT NULL UNIQUE,
    user_id uuid NOT NULL REFERENCES bighill_ingestion_db.tenants(id),
    org_id uuid NOT NULL,
    storage_location text NOT NULL DEFAULT '',
    table_namespace text NOT NULL DEFAULT '',
    table_name text NOT NULL DEFAULT '',
    table_format table_format_enum NOT NULL DEFAULT 'PARQUET',
    catalog_provider catalog_provider_enum NOT NULL DEFAULT 'LOCAL',
    processing_profile processing_profile_enum NOT NULL DEFAULT 'GENERIC_PARQUET_PROCESSING_PROFILE',
    schema_version integer NOT NULL DEFAULT 0,
    schema_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,

    blacklisted BOOLEAN DEFAULT FALSE
);

CREATE INDEX index_dataset_user_id ON bighill_ingestion_db.datasets(dataset_id, user_id);
CREATE INDEX index_datasets_org_id ON bighill_ingestion_db.datasets(org_id);

CREATE TABLE IF NOT EXISTS bighill_ingestion_db.outbox_messages (
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
ON bighill_ingestion_db.outbox_messages(status, next_attempt_at, created_at);

CREATE INDEX IF NOT EXISTS index_outbox_messages_processing
ON bighill_ingestion_db.outbox_messages(status, lease_expires_at, created_at);

CREATE INDEX IF NOT EXISTS index_outbox_messages_resource_key
ON bighill_ingestion_db.outbox_messages(resource_key, created_at);

CREATE TABLE IF NOT EXISTS bighill_ingestion_db.upload_sessions (
    upload_id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    resource_type upload_resource_type_enum NOT NULL DEFAULT 'DATA_FILE',
    resource_id uuid NOT NULL DEFAULT uuid_generate_v4(),
    dataset_id uuid REFERENCES bighill_ingestion_db.datasets(dataset_id),
    user_id uuid NOT NULL REFERENCES bighill_ingestion_db.tenants(id),
    org_id uuid NOT NULL,
    client_nonce text NOT NULL DEFAULT '',
    file_name text NOT NULL DEFAULT '',
    staging_key text NOT NULL DEFAULT '',
    final_key text NOT NULL DEFAULT '',
    storage_location text NOT NULL DEFAULT '',
    declared_format text NOT NULL,
    declared_content_type text NOT NULL,
    declared_size_bytes bigint NOT NULL DEFAULT 0,
    actual_size_bytes bigint NOT NULL DEFAULT 0,
    checksum text NOT NULL DEFAULT '',
    status upload_session_status_enum NOT NULL,
    table_namespace text NOT NULL DEFAULT '',
    table_name text NOT NULL DEFAULT '',
    table_format table_format_enum,
    catalog_provider catalog_provider_enum,
    processing_profile processing_profile_enum,
    artifact_type text NOT NULL DEFAULT '',
    model_name text NOT NULL DEFAULT '',
    model_version text NOT NULL DEFAULT '',
    base_model text NOT NULL DEFAULT '',
    source model_source_enum NOT NULL DEFAULT 'UPLOAD',
    source_uri text NOT NULL DEFAULT '',
    manifest_location text NOT NULL DEFAULT '',
    hf_repo_id text NOT NULL DEFAULT '',
    hf_revision text NOT NULL DEFAULT '',
    hf_commit_sha text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS index_upload_sessions_dataset_user
ON bighill_ingestion_db.upload_sessions(dataset_id, user_id, created_at);
CREATE INDEX IF NOT EXISTS index_upload_sessions_dataset_org
ON bighill_ingestion_db.upload_sessions(dataset_id, org_id, created_at);

CREATE INDEX IF NOT EXISTS index_upload_sessions_resource_user
ON bighill_ingestion_db.upload_sessions(resource_type, resource_id, user_id, created_at);
CREATE INDEX IF NOT EXISTS index_upload_sessions_resource_org
ON bighill_ingestion_db.upload_sessions(resource_type, resource_id, org_id, created_at);

CREATE INDEX IF NOT EXISTS index_upload_sessions_pending_expiry
ON bighill_ingestion_db.upload_sessions(status, expires_at);

CREATE UNIQUE INDEX IF NOT EXISTS index_upload_sessions_client_nonce
ON bighill_ingestion_db.upload_sessions(resource_type, org_id, user_id, client_nonce)
WHERE client_nonce <> '';

ALTER TABLE bighill_ingestion_db.tenants ENABLE ROW LEVEL SECURITY;
ALTER TABLE bighill_ingestion_db.tenants FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_projection_isolation ON bighill_ingestion_db.tenants
USING (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_user_id', true), '')::uuid = id
)
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_user_id', true), '')::uuid = id
);

ALTER TABLE bighill_ingestion_db.datasets ENABLE ROW LEVEL SECURITY;
ALTER TABLE bighill_ingestion_db.datasets FORCE ROW LEVEL SECURITY;
CREATE POLICY datasets_tenant_isolation ON bighill_ingestion_db.datasets
USING (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
)
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
);

ALTER TABLE bighill_ingestion_db.upload_sessions ENABLE ROW LEVEL SECURITY;
ALTER TABLE bighill_ingestion_db.upload_sessions FORCE ROW LEVEL SECURITY;
CREATE POLICY upload_sessions_tenant_isolation ON bighill_ingestion_db.upload_sessions
USING (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
)
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
);
