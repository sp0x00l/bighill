CREATE TYPE origin_enum AS ENUM ('standard', 'community');
CREATE TYPE storage_type_enum AS ENUM ('S3', 'AZURE_STORAGE', 'GCS', 'POSTGRES', 'MYSQL', 'ORACLE', 'MONGO', 'CLICKHOUSE');
CREATE TYPE status_enum AS ENUM ('draft', 'published', 'blacklisted');
CREATE TYPE table_format_enum AS ENUM ('PARQUET', 'ICEBERG');
CREATE TYPE catalog_provider_enum AS ENUM ('LOCAL', 'POLARIS');
CREATE TYPE processing_profile_enum AS ENUM ('GENERIC_PARQUET', 'TEXT_RAG', 'INSTRUCTION_TUNING');
CREATE TYPE dataset_processing_state_enum AS ENUM (
    'PENDING',
    'RAW_MATERIALIZED',
    'FEATURE_MATERIALIZED',
    'EMBEDDINGS_MATERIALIZED',
    'FAILED'
);

CREATE TABLE IF NOT EXISTS bighill_data_registry_db.datasets(
    id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
    idempotency_key uuid UNIQUE NOT NULL,
    user_id uuid NOT NULL,
    title VARCHAR(255) NOT NULL,
    description TEXT,
    origin origin_enum NOT NULL DEFAULT 'standard',
    location VARCHAR(255),
    source_type storage_type_enum,
    source_connector_id uuid,
    source_query TEXT NOT NULL DEFAULT '',
    source_database TEXT NOT NULL DEFAULT '',
    source_collection TEXT NOT NULL DEFAULT '',
    status status_enum NOT NULL DEFAULT 'draft',
    processing_state dataset_processing_state_enum NOT NULL DEFAULT 'PENDING',
    category VARCHAR(255),
    table_namespace VARCHAR(255) NOT NULL DEFAULT 'default',
    table_name VARCHAR(255) NOT NULL,
    table_format table_format_enum NOT NULL DEFAULT 'PARQUET',
    catalog_provider catalog_provider_enum NOT NULL DEFAULT 'LOCAL',
    processing_profile processing_profile_enum NOT NULL DEFAULT 'GENERIC_PARQUET',
    schema_version INTEGER NOT NULL DEFAULT 1,
    schema_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    dataset_version INTEGER NOT NULL DEFAULT 1,
    raw_snapshot_id uuid,
    feature_snapshot_id uuid,
    embedding_snapshot_id uuid,
    vector_store TEXT NOT NULL DEFAULT '',
    collection_name TEXT NOT NULL DEFAULT '',
    embedding_dimensions INTEGER NOT NULL DEFAULT 0,
    embedding_count BIGINT NOT NULL DEFAULT 0,
    embedding_strategy_version TEXT NOT NULL DEFAULT '',
    embedding_chunker_name TEXT NOT NULL DEFAULT '',
    embedding_chunker_version TEXT NOT NULL DEFAULT '',
    embedding_chunk_size INTEGER NOT NULL DEFAULT 0,
    embedding_chunk_overlap INTEGER NOT NULL DEFAULT 0,
    embedding_provider TEXT NOT NULL DEFAULT '',
    embedding_model TEXT NOT NULL DEFAULT '',
    published_at TIMESTAMP WITHOUT TIME ZONE, 
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT LOCALTIMESTAMP,
    updated_at TIMESTAMP WITHOUT TIME ZONE,
    deleted BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS bighill_data_registry_db.connectors(
    -- the connector id is used as the stable catalog resource name.
    -- It needs to be created before saving the db connector record.
    id uuid UNIQUE NOT NULL PRIMARY KEY,
    idempotency_key uuid UNIQUE NOT NULL,
    user_id uuid NOT NULL,
    catalog_id uuid UNIQUE NOT NULL,
    storage_type storage_type_enum NOT NULL,
    config BYTEA NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT LOCALTIMESTAMP,
    updated_at TIMESTAMP WITHOUT TIME ZONE,
    deleted BOOLEAN DEFAULT FALSE
); 

-- dataset_id is a foreign key in metadata table
CREATE TABLE IF NOT EXISTS bighill_data_registry_db.metadata(
    id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
    dataset_id uuid NOT NULL REFERENCES bighill_data_registry_db.datasets(id) ON DELETE CASCADE ON UPDATE CASCADE,
    schema_name VARCHAR(255) NOT NULL,
    schema_version INTEGER DEFAULT 1,
    metadata TEXT NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT LOCALTIMESTAMP,
    updated_at TIMESTAMP WITHOUT TIME ZONE,
    deleted BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS bighill_data_registry_db.outbox_messages (
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

CREATE INDEX index_user_id ON bighill_data_registry_db.datasets(user_id);
CREATE INDEX index_dataset_processing_state ON bighill_data_registry_db.datasets(processing_state);
CREATE INDEX index_dataset_table_ref ON bighill_data_registry_db.datasets(catalog_provider, table_namespace, table_name);
CREATE INDEX index_dataset_raw_snapshot_id ON bighill_data_registry_db.datasets(raw_snapshot_id);
CREATE INDEX index_dataset_feature_snapshot_id ON bighill_data_registry_db.datasets(feature_snapshot_id);
CREATE INDEX index_dataset_embedding_snapshot_id ON bighill_data_registry_db.datasets(embedding_snapshot_id);
CREATE INDEX index_dataset_source_connector_id ON bighill_data_registry_db.datasets(source_connector_id);
CREATE INDEX index_dataset_id_connectors ON bighill_data_registry_db.connectors(user_id);
CREATE INDEX index_dataset_id_metadata ON bighill_data_registry_db.metadata(dataset_id);
CREATE INDEX index_outbox_messages_pending
ON bighill_data_registry_db.outbox_messages(status, next_attempt_at, created_at);
CREATE INDEX index_outbox_messages_processing
ON bighill_data_registry_db.outbox_messages(status, lease_expires_at, created_at);
CREATE INDEX index_outbox_messages_resource_key
ON bighill_data_registry_db.outbox_messages(resource_key, created_at);

CREATE TRIGGER updated_at_trigger BEFORE INSERT OR UPDATE ON bighill_data_registry_db.datasets FOR EACH ROW EXECUTE FUNCTION updated_at_column();
CREATE TRIGGER updated_at_trigger BEFORE INSERT OR UPDATE ON bighill_data_registry_db.connectors FOR EACH ROW EXECUTE FUNCTION updated_at_column();
CREATE TRIGGER updated_at_trigger BEFORE INSERT OR UPDATE ON bighill_data_registry_db.metadata FOR EACH ROW EXECUTE FUNCTION updated_at_column();
