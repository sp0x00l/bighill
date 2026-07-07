CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public;
CREATE EXTENSION IF NOT EXISTS citext;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE OR REPLACE FUNCTION updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TYPE snapshot_status_enum AS ENUM ('PENDING', 'READY', 'FAILED');
CREATE TYPE table_format_enum AS ENUM ('PARQUET', 'ICEBERG');
CREATE TYPE catalog_provider_enum AS ENUM ('LOCAL', 'POLARIS');
CREATE TYPE processing_profile_enum AS ENUM (
    'GENERIC_PARQUET_PROCESSING_PROFILE',
    'TEXT_RAG_PROCESSING_PROFILE',
    'INSTRUCTION_TUNING_PROCESSING_PROFILE'
);

CREATE TABLE IF NOT EXISTS bighill_feature_materializer_db.tenants (
    id uuid PRIMARY KEY,
    email citext NOT NULL DEFAULT '',
    huggingface_token_ciphertext text NOT NULL DEFAULT '',
    deleted boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS index_tenants_deleted
ON bighill_feature_materializer_db.tenants(deleted, updated_at);

CREATE TRIGGER tenants_updated_at
BEFORE UPDATE ON bighill_feature_materializer_db.tenants
FOR EACH ROW
EXECUTE FUNCTION updated_at_column();

CREATE TABLE IF NOT EXISTS bighill_feature_materializer_db.raw_snapshots (
    raw_snapshot_id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    dataset_id uuid NOT NULL,
    user_id uuid NOT NULL REFERENCES bighill_feature_materializer_db.tenants(id),
    org_id uuid NOT NULL,
    idempotency_key uuid NOT NULL UNIQUE,
    source_storage_location text NOT NULL,
    storage_location text NOT NULL,
    content_type text NOT NULL,
    file_extension text NOT NULL,
    table_namespace text NOT NULL,
    table_name text NOT NULL,
    table_format table_format_enum NOT NULL,
    catalog_provider catalog_provider_enum NOT NULL,
    processing_profile processing_profile_enum NOT NULL DEFAULT 'GENERIC_PARQUET_PROCESSING_PROFILE',
    schema_version integer NOT NULL DEFAULT 1,
    schema_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    status snapshot_status_enum NOT NULL DEFAULT 'PENDING',
    failure_reason text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS index_raw_snapshots_dataset_id ON bighill_feature_materializer_db.raw_snapshots(dataset_id);
CREATE INDEX IF NOT EXISTS index_raw_snapshots_user_id ON bighill_feature_materializer_db.raw_snapshots(user_id);
CREATE INDEX IF NOT EXISTS index_raw_snapshots_org_id ON bighill_feature_materializer_db.raw_snapshots(org_id);

CREATE TRIGGER raw_snapshots_updated_at
BEFORE UPDATE ON bighill_feature_materializer_db.raw_snapshots
FOR EACH ROW
EXECUTE FUNCTION updated_at_column();

CREATE TABLE IF NOT EXISTS bighill_feature_materializer_db.feature_snapshots (
    feature_snapshot_id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    raw_snapshot_id uuid NOT NULL REFERENCES bighill_feature_materializer_db.raw_snapshots(raw_snapshot_id),
    dataset_id uuid NOT NULL,
    user_id uuid NOT NULL REFERENCES bighill_feature_materializer_db.tenants(id),
    org_id uuid NOT NULL,
    idempotency_key uuid NOT NULL UNIQUE,
    storage_location text NOT NULL DEFAULT '',
    table_namespace text NOT NULL,
    table_name text NOT NULL,
    table_format table_format_enum NOT NULL,
    catalog_provider catalog_provider_enum NOT NULL,
    processing_profile processing_profile_enum NOT NULL DEFAULT 'GENERIC_PARQUET_PROCESSING_PROFILE',
    schema_version integer NOT NULL DEFAULT 1,
    schema_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    status snapshot_status_enum NOT NULL DEFAULT 'PENDING',
    failure_reason text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS index_feature_snapshots_raw_snapshot_id ON bighill_feature_materializer_db.feature_snapshots(raw_snapshot_id);
CREATE INDEX IF NOT EXISTS index_feature_snapshots_dataset_id ON bighill_feature_materializer_db.feature_snapshots(dataset_id);
CREATE INDEX IF NOT EXISTS index_feature_snapshots_org_id ON bighill_feature_materializer_db.feature_snapshots(org_id);

CREATE TRIGGER feature_snapshots_updated_at
BEFORE UPDATE ON bighill_feature_materializer_db.feature_snapshots
FOR EACH ROW
EXECUTE FUNCTION updated_at_column();

CREATE TABLE IF NOT EXISTS bighill_feature_materializer_db.embedding_snapshots (
    embedding_snapshot_id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    feature_snapshot_id uuid NOT NULL REFERENCES bighill_feature_materializer_db.feature_snapshots(feature_snapshot_id),
    dataset_id uuid NOT NULL,
    user_id uuid NOT NULL REFERENCES bighill_feature_materializer_db.tenants(id),
    org_id uuid NOT NULL,
    idempotency_key uuid NOT NULL UNIQUE,
    vector_store text NOT NULL DEFAULT '',
    collection_name text NOT NULL DEFAULT '',
    embedding_dimensions integer NOT NULL DEFAULT 0,
    embedding_count bigint NOT NULL DEFAULT 0,
    strategy_version text NOT NULL DEFAULT 'rag-v1',
    extractor_name text NOT NULL DEFAULT 'go-document-extractor-suite',
    extractor_version text NOT NULL DEFAULT 'v1',
    cleaner_name text NOT NULL DEFAULT 'go-basic-text-cleaner',
    cleaner_version text NOT NULL DEFAULT 'v1',
    chunker_name text NOT NULL DEFAULT 'go-token-window',
    chunker_version text NOT NULL DEFAULT 'v1',
    chunk_size integer NOT NULL DEFAULT 384,
    chunk_overlap integer NOT NULL DEFAULT 64,
    embedding_provider text NOT NULL,
    embedding_model text NOT NULL DEFAULT 'bge-small-en-v1.5',
    active_for_retrieval boolean NOT NULL DEFAULT false,
    status snapshot_status_enum NOT NULL DEFAULT 'PENDING',
    failure_reason text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS index_embedding_snapshots_feature_snapshot_id ON bighill_feature_materializer_db.embedding_snapshots(feature_snapshot_id);
CREATE INDEX IF NOT EXISTS index_embedding_snapshots_dataset_id ON bighill_feature_materializer_db.embedding_snapshots(dataset_id);
CREATE INDEX IF NOT EXISTS index_embedding_snapshots_user_id ON bighill_feature_materializer_db.embedding_snapshots(user_id);
CREATE INDEX IF NOT EXISTS index_embedding_snapshots_org_id ON bighill_feature_materializer_db.embedding_snapshots(org_id);
CREATE UNIQUE INDEX IF NOT EXISTS index_embedding_snapshots_active_dataset
ON bighill_feature_materializer_db.embedding_snapshots(dataset_id)
WHERE active_for_retrieval = true;

CREATE TRIGGER embedding_snapshots_updated_at
BEFORE UPDATE ON bighill_feature_materializer_db.embedding_snapshots
FOR EACH ROW
EXECUTE FUNCTION updated_at_column();

CREATE TABLE IF NOT EXISTS bighill_feature_materializer_db.embedding_records (
    embedding_record_id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    embedding_snapshot_id uuid NOT NULL REFERENCES bighill_feature_materializer_db.embedding_snapshots(embedding_snapshot_id) ON DELETE CASCADE,
    dataset_id uuid NOT NULL,
    user_id uuid NOT NULL REFERENCES bighill_feature_materializer_db.tenants(id),
    org_id uuid NOT NULL,
    chunk_index integer NOT NULL DEFAULT 0,
    source_text text NOT NULL,
    embedding vector NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS index_embedding_records_snapshot_id ON bighill_feature_materializer_db.embedding_records(embedding_snapshot_id);
CREATE INDEX IF NOT EXISTS index_embedding_records_dataset_id ON bighill_feature_materializer_db.embedding_records(dataset_id);
CREATE INDEX IF NOT EXISTS index_embedding_records_user_id ON bighill_feature_materializer_db.embedding_records(user_id);
CREATE INDEX IF NOT EXISTS index_embedding_records_org_id ON bighill_feature_materializer_db.embedding_records(org_id);
-- Retrieval queries must use embedding::vector(384), cosine distance, and vector_dims(embedding) = 384 to use this partial ANN index.
CREATE INDEX IF NOT EXISTS index_embedding_records_embedding_384_hnsw
ON bighill_feature_materializer_db.embedding_records
USING hnsw ((embedding::vector(384)) vector_cosine_ops)
WHERE vector_dims(embedding) = 384;

CREATE TABLE IF NOT EXISTS bighill_feature_materializer_db.outbox_messages (
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
ON bighill_feature_materializer_db.outbox_messages(status, next_attempt_at, created_at);

CREATE INDEX IF NOT EXISTS index_outbox_messages_processing
ON bighill_feature_materializer_db.outbox_messages(status, lease_expires_at, created_at);

CREATE INDEX IF NOT EXISTS index_outbox_messages_resource_key
ON bighill_feature_materializer_db.outbox_messages(resource_key, created_at);

CREATE TRIGGER outbox_messages_updated_at
BEFORE UPDATE ON bighill_feature_materializer_db.outbox_messages
FOR EACH ROW
EXECUTE FUNCTION updated_at_column();

ALTER TABLE bighill_feature_materializer_db.tenants ENABLE ROW LEVEL SECURITY;
ALTER TABLE bighill_feature_materializer_db.tenants FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_projection_isolation ON bighill_feature_materializer_db.tenants
USING (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_user_id', true), '')::uuid = id
)
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_user_id', true), '')::uuid = id
);

ALTER TABLE bighill_feature_materializer_db.raw_snapshots ENABLE ROW LEVEL SECURITY;
ALTER TABLE bighill_feature_materializer_db.raw_snapshots FORCE ROW LEVEL SECURITY;
CREATE POLICY raw_snapshots_tenant_isolation ON bighill_feature_materializer_db.raw_snapshots
USING (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
)
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
);

ALTER TABLE bighill_feature_materializer_db.feature_snapshots ENABLE ROW LEVEL SECURITY;
ALTER TABLE bighill_feature_materializer_db.feature_snapshots FORCE ROW LEVEL SECURITY;
CREATE POLICY feature_snapshots_tenant_isolation ON bighill_feature_materializer_db.feature_snapshots
USING (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
)
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
);

ALTER TABLE bighill_feature_materializer_db.embedding_snapshots ENABLE ROW LEVEL SECURITY;
ALTER TABLE bighill_feature_materializer_db.embedding_snapshots FORCE ROW LEVEL SECURITY;
CREATE POLICY embedding_snapshots_tenant_isolation ON bighill_feature_materializer_db.embedding_snapshots
USING (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
)
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
);

ALTER TABLE bighill_feature_materializer_db.embedding_records ENABLE ROW LEVEL SECURITY;
ALTER TABLE bighill_feature_materializer_db.embedding_records FORCE ROW LEVEL SECURITY;
CREATE POLICY embedding_records_tenant_isolation ON bighill_feature_materializer_db.embedding_records
USING (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
)
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
);
