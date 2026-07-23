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

CREATE TABLE IF NOT EXISTS bighill_feature_materializer_db.dataset_materialization_event_state (
    dataset_id uuid NOT NULL,
    org_id uuid NOT NULL,
    next_event_seq BIGINT NOT NULL DEFAULT 1 CHECK (next_event_seq > 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (dataset_id, org_id)
);

CREATE INDEX IF NOT EXISTS index_dataset_materialization_event_state_org_id
ON bighill_feature_materializer_db.dataset_materialization_event_state(org_id);

CREATE TRIGGER dataset_materialization_event_state_updated_at
BEFORE UPDATE ON bighill_feature_materializer_db.dataset_materialization_event_state
FOR EACH ROW
EXECUTE FUNCTION updated_at_column();

CREATE TABLE IF NOT EXISTS bighill_feature_materializer_db.raw_snapshots (
    raw_snapshot_id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    dataset_id uuid NOT NULL,
    user_id uuid NOT NULL REFERENCES bighill_feature_materializer_db.tenants(id),
    org_id uuid NOT NULL,
    materialization_event_seq BIGINT NOT NULL DEFAULT 0 CHECK (materialization_event_seq >= 0),
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
    materialization_event_seq BIGINT NOT NULL DEFAULT 0 CHECK (materialization_event_seq >= 0),
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
    materialization_event_seq BIGINT NOT NULL DEFAULT 0 CHECK (materialization_event_seq >= 0),
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

CREATE TABLE IF NOT EXISTS bighill_feature_materializer_db.graph_snapshots (
    graph_snapshot_id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    feature_snapshot_id uuid NOT NULL REFERENCES bighill_feature_materializer_db.feature_snapshots(feature_snapshot_id),
    embedding_snapshot_id uuid NOT NULL REFERENCES bighill_feature_materializer_db.embedding_snapshots(embedding_snapshot_id),
    dataset_id uuid NOT NULL,
    user_id uuid NOT NULL REFERENCES bighill_feature_materializer_db.tenants(id),
    org_id uuid NOT NULL,
    materialization_event_seq BIGINT NOT NULL DEFAULT 0 CHECK (materialization_event_seq >= 0),
    idempotency_key uuid NOT NULL UNIQUE,
    provenance_hash text NOT NULL DEFAULT '',
    extraction_model text NOT NULL DEFAULT '',
    extraction_prompt_version text NOT NULL DEFAULT '',
    extraction_schema_version text NOT NULL DEFAULT '',
    chunk_count bigint NOT NULL DEFAULT 0 CHECK (chunk_count >= 0),
    chunks_processed bigint NOT NULL DEFAULT 0 CHECK (chunks_processed >= 0),
    entity_count bigint NOT NULL DEFAULT 0 CHECK (entity_count >= 0),
    edge_count bigint NOT NULL DEFAULT 0 CHECK (edge_count >= 0),
    active_for_retrieval boolean NOT NULL DEFAULT false,
    status snapshot_status_enum NOT NULL DEFAULT 'PENDING',
    failure_reason text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT graph_snapshots_ready_ck CHECK (
        status != 'READY'
        OR (
            btrim(provenance_hash) <> ''
            AND btrim(extraction_model) <> ''
            AND btrim(extraction_prompt_version) <> ''
            AND btrim(extraction_schema_version) <> ''
            AND chunks_processed = chunk_count
        )
    )
);

CREATE INDEX IF NOT EXISTS index_graph_snapshots_feature_snapshot_id ON bighill_feature_materializer_db.graph_snapshots(feature_snapshot_id);
CREATE INDEX IF NOT EXISTS index_graph_snapshots_embedding_snapshot_id ON bighill_feature_materializer_db.graph_snapshots(embedding_snapshot_id);
CREATE INDEX IF NOT EXISTS index_graph_snapshots_dataset_id ON bighill_feature_materializer_db.graph_snapshots(dataset_id);
CREATE INDEX IF NOT EXISTS index_graph_snapshots_org_id ON bighill_feature_materializer_db.graph_snapshots(org_id);
CREATE UNIQUE INDEX IF NOT EXISTS index_graph_snapshots_active_dataset
ON bighill_feature_materializer_db.graph_snapshots(dataset_id)
WHERE active_for_retrieval = true;

CREATE TRIGGER graph_snapshots_updated_at
BEFORE UPDATE ON bighill_feature_materializer_db.graph_snapshots
FOR EACH ROW
EXECUTE FUNCTION updated_at_column();

CREATE TABLE IF NOT EXISTS bighill_feature_materializer_db.graph_nodes (
    graph_node_id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    graph_snapshot_id uuid NOT NULL REFERENCES bighill_feature_materializer_db.graph_snapshots(graph_snapshot_id) ON DELETE CASCADE,
    dataset_id uuid NOT NULL,
    user_id uuid NOT NULL REFERENCES bighill_feature_materializer_db.tenants(id),
    org_id uuid NOT NULL,
    entity_key text NOT NULL,
    name text NOT NULL,
    entity_type text NOT NULL,
    description text NOT NULL DEFAULT '',
    mention_count integer NOT NULL DEFAULT 0 CHECK (mention_count >= 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (graph_snapshot_id, entity_key)
);

CREATE INDEX IF NOT EXISTS index_graph_nodes_dataset_id ON bighill_feature_materializer_db.graph_nodes(dataset_id);
CREATE INDEX IF NOT EXISTS index_graph_nodes_org_id ON bighill_feature_materializer_db.graph_nodes(org_id);
CREATE INDEX IF NOT EXISTS index_graph_nodes_name_type ON bighill_feature_materializer_db.graph_nodes(lower(name), lower(entity_type));

CREATE TABLE IF NOT EXISTS bighill_feature_materializer_db.graph_node_aliases (
    graph_node_alias_id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    graph_snapshot_id uuid NOT NULL REFERENCES bighill_feature_materializer_db.graph_snapshots(graph_snapshot_id) ON DELETE CASCADE,
    graph_node_id uuid NOT NULL REFERENCES bighill_feature_materializer_db.graph_nodes(graph_node_id) ON DELETE CASCADE,
    dataset_id uuid NOT NULL,
    user_id uuid NOT NULL REFERENCES bighill_feature_materializer_db.tenants(id),
    org_id uuid NOT NULL,
    source_entity_key text NOT NULL,
    alias text NOT NULL,
    entity_type text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (graph_snapshot_id, graph_node_id, source_entity_key)
);

CREATE INDEX IF NOT EXISTS index_graph_node_aliases_snapshot_alias_type ON bighill_feature_materializer_db.graph_node_aliases(graph_snapshot_id, lower(alias), lower(entity_type));
CREATE INDEX IF NOT EXISTS index_graph_node_aliases_org_id ON bighill_feature_materializer_db.graph_node_aliases(org_id);

CREATE TABLE IF NOT EXISTS bighill_feature_materializer_db.graph_node_embeddings (
    graph_node_embedding_id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    graph_snapshot_id uuid NOT NULL REFERENCES bighill_feature_materializer_db.graph_snapshots(graph_snapshot_id) ON DELETE CASCADE,
    graph_node_id uuid NOT NULL REFERENCES bighill_feature_materializer_db.graph_nodes(graph_node_id) ON DELETE CASCADE,
    embedding_snapshot_id uuid NOT NULL REFERENCES bighill_feature_materializer_db.embedding_snapshots(embedding_snapshot_id) ON DELETE CASCADE,
    dataset_id uuid NOT NULL,
    user_id uuid NOT NULL REFERENCES bighill_feature_materializer_db.tenants(id),
    org_id uuid NOT NULL,
    embedding_text text NOT NULL,
    embedding vector NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (graph_node_id, embedding_snapshot_id)
);

CREATE INDEX IF NOT EXISTS index_graph_node_embeddings_snapshot_id ON bighill_feature_materializer_db.graph_node_embeddings(graph_snapshot_id);
CREATE INDEX IF NOT EXISTS index_graph_node_embeddings_org_id ON bighill_feature_materializer_db.graph_node_embeddings(org_id);
-- Graph semantic seeding must use embedding::vector(384), cosine distance, and vector_dims(embedding) = 384 to use this partial ANN index.
CREATE INDEX IF NOT EXISTS index_graph_node_embeddings_embedding_384_hnsw
ON bighill_feature_materializer_db.graph_node_embeddings
USING hnsw ((embedding::vector(384)) vector_cosine_ops)
WHERE vector_dims(embedding) = 384;

CREATE TABLE IF NOT EXISTS bighill_feature_materializer_db.graph_edges (
    graph_edge_id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    graph_snapshot_id uuid NOT NULL REFERENCES bighill_feature_materializer_db.graph_snapshots(graph_snapshot_id) ON DELETE CASCADE,
    dataset_id uuid NOT NULL,
    user_id uuid NOT NULL REFERENCES bighill_feature_materializer_db.tenants(id),
    org_id uuid NOT NULL,
    source_node_id uuid NOT NULL REFERENCES bighill_feature_materializer_db.graph_nodes(graph_node_id) ON DELETE CASCADE,
    target_node_id uuid NOT NULL REFERENCES bighill_feature_materializer_db.graph_nodes(graph_node_id) ON DELETE CASCADE,
    relation_type text NOT NULL,
    description text NOT NULL DEFAULT '',
    weight double precision NOT NULL DEFAULT 1 CHECK (weight >= 0),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS index_graph_edges_snapshot_source ON bighill_feature_materializer_db.graph_edges(graph_snapshot_id, source_node_id);
CREATE INDEX IF NOT EXISTS index_graph_edges_snapshot_target ON bighill_feature_materializer_db.graph_edges(graph_snapshot_id, target_node_id);
CREATE INDEX IF NOT EXISTS index_graph_edges_org_id ON bighill_feature_materializer_db.graph_edges(org_id);

CREATE TABLE IF NOT EXISTS bighill_feature_materializer_db.graph_node_chunks (
    graph_node_chunk_id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    graph_snapshot_id uuid NOT NULL REFERENCES bighill_feature_materializer_db.graph_snapshots(graph_snapshot_id) ON DELETE CASCADE,
    graph_node_id uuid NOT NULL REFERENCES bighill_feature_materializer_db.graph_nodes(graph_node_id) ON DELETE CASCADE,
    embedding_record_id uuid NOT NULL REFERENCES bighill_feature_materializer_db.embedding_records(embedding_record_id) ON DELETE CASCADE,
    embedding_snapshot_id uuid NOT NULL REFERENCES bighill_feature_materializer_db.embedding_snapshots(embedding_snapshot_id) ON DELETE CASCADE,
    dataset_id uuid NOT NULL,
    user_id uuid NOT NULL REFERENCES bighill_feature_materializer_db.tenants(id),
    org_id uuid NOT NULL,
    chunk_index integer NOT NULL DEFAULT 0,
    source_text text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (graph_node_id, embedding_record_id)
);

CREATE INDEX IF NOT EXISTS index_graph_node_chunks_snapshot_id ON bighill_feature_materializer_db.graph_node_chunks(graph_snapshot_id);
CREATE INDEX IF NOT EXISTS index_graph_node_chunks_node_id ON bighill_feature_materializer_db.graph_node_chunks(graph_node_id);
CREATE INDEX IF NOT EXISTS index_graph_node_chunks_org_id ON bighill_feature_materializer_db.graph_node_chunks(org_id);

CREATE TABLE IF NOT EXISTS bighill_feature_materializer_db.graph_communities (
    graph_community_id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    graph_snapshot_id uuid NOT NULL REFERENCES bighill_feature_materializer_db.graph_snapshots(graph_snapshot_id) ON DELETE CASCADE,
    dataset_id uuid NOT NULL,
    user_id uuid NOT NULL REFERENCES bighill_feature_materializer_db.tenants(id),
    org_id uuid NOT NULL,
    community_key text NOT NULL,
    algorithm text NOT NULL,
    community_level integer NOT NULL DEFAULT 0 CHECK (community_level >= 0),
    title text NOT NULL,
    summary text NOT NULL DEFAULT '',
    rank double precision NOT NULL DEFAULT 0 CHECK (rank >= 0),
    entity_count integer NOT NULL DEFAULT 0 CHECK (entity_count >= 0),
    edge_count integer NOT NULL DEFAULT 0 CHECK (edge_count >= 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (graph_snapshot_id, community_key)
);

CREATE INDEX IF NOT EXISTS index_graph_communities_snapshot_id ON bighill_feature_materializer_db.graph_communities(graph_snapshot_id);
CREATE INDEX IF NOT EXISTS index_graph_communities_org_id ON bighill_feature_materializer_db.graph_communities(org_id);
CREATE INDEX IF NOT EXISTS index_graph_communities_title ON bighill_feature_materializer_db.graph_communities(graph_snapshot_id, lower(title));

CREATE TABLE IF NOT EXISTS bighill_feature_materializer_db.graph_community_members (
    graph_community_member_id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    graph_community_id uuid NOT NULL REFERENCES bighill_feature_materializer_db.graph_communities(graph_community_id) ON DELETE CASCADE,
    graph_snapshot_id uuid NOT NULL REFERENCES bighill_feature_materializer_db.graph_snapshots(graph_snapshot_id) ON DELETE CASCADE,
    graph_node_id uuid NOT NULL REFERENCES bighill_feature_materializer_db.graph_nodes(graph_node_id) ON DELETE CASCADE,
    dataset_id uuid NOT NULL,
    user_id uuid NOT NULL REFERENCES bighill_feature_materializer_db.tenants(id),
    org_id uuid NOT NULL,
    community_key text NOT NULL,
    entity_key text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (graph_snapshot_id, graph_node_id)
);

CREATE INDEX IF NOT EXISTS index_graph_community_members_community_id ON bighill_feature_materializer_db.graph_community_members(graph_community_id);
CREATE INDEX IF NOT EXISTS index_graph_community_members_snapshot_id ON bighill_feature_materializer_db.graph_community_members(graph_snapshot_id);
CREATE INDEX IF NOT EXISTS index_graph_community_members_org_id ON bighill_feature_materializer_db.graph_community_members(org_id);

CREATE TABLE IF NOT EXISTS bighill_feature_materializer_db.graph_community_reports (
    graph_community_report_id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    graph_community_id uuid NOT NULL REFERENCES bighill_feature_materializer_db.graph_communities(graph_community_id) ON DELETE CASCADE,
    graph_snapshot_id uuid NOT NULL REFERENCES bighill_feature_materializer_db.graph_snapshots(graph_snapshot_id) ON DELETE CASCADE,
    embedding_snapshot_id uuid NOT NULL REFERENCES bighill_feature_materializer_db.embedding_snapshots(embedding_snapshot_id) ON DELETE CASCADE,
    dataset_id uuid NOT NULL,
    user_id uuid NOT NULL REFERENCES bighill_feature_materializer_db.tenants(id),
    org_id uuid NOT NULL,
    community_key text NOT NULL,
    community_level integer NOT NULL DEFAULT 0 CHECK (community_level >= 0),
    title text NOT NULL,
    summary text NOT NULL DEFAULT '',
    report_text text NOT NULL,
    rank double precision NOT NULL DEFAULT 0 CHECK (rank >= 0),
    report_version text NOT NULL,
    embedding_text text NOT NULL,
    embedding vector,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (graph_community_id, report_version)
);

CREATE INDEX IF NOT EXISTS index_graph_community_reports_snapshot_id ON bighill_feature_materializer_db.graph_community_reports(graph_snapshot_id);
CREATE INDEX IF NOT EXISTS index_graph_community_reports_org_id ON bighill_feature_materializer_db.graph_community_reports(org_id);
CREATE INDEX IF NOT EXISTS index_graph_community_reports_title ON bighill_feature_materializer_db.graph_community_reports(graph_snapshot_id, lower(title));
-- Global graph search must use embedding::vector(384), cosine distance, and vector_dims(embedding) = 384 to use this partial ANN index.
CREATE INDEX IF NOT EXISTS index_graph_community_reports_embedding_384_hnsw
ON bighill_feature_materializer_db.graph_community_reports
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

ALTER TABLE bighill_feature_materializer_db.dataset_materialization_event_state ENABLE ROW LEVEL SECURITY;
ALTER TABLE bighill_feature_materializer_db.dataset_materialization_event_state FORCE ROW LEVEL SECURITY;
CREATE POLICY dataset_materialization_event_state_tenant_isolation ON bighill_feature_materializer_db.dataset_materialization_event_state
USING (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
)
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
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

ALTER TABLE bighill_feature_materializer_db.graph_snapshots ENABLE ROW LEVEL SECURITY;
ALTER TABLE bighill_feature_materializer_db.graph_snapshots FORCE ROW LEVEL SECURITY;
CREATE POLICY graph_snapshots_tenant_isolation ON bighill_feature_materializer_db.graph_snapshots
USING (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
)
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
);

ALTER TABLE bighill_feature_materializer_db.graph_nodes ENABLE ROW LEVEL SECURITY;
ALTER TABLE bighill_feature_materializer_db.graph_nodes FORCE ROW LEVEL SECURITY;
CREATE POLICY graph_nodes_tenant_isolation ON bighill_feature_materializer_db.graph_nodes
USING (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
)
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
);

ALTER TABLE bighill_feature_materializer_db.graph_node_aliases ENABLE ROW LEVEL SECURITY;
ALTER TABLE bighill_feature_materializer_db.graph_node_aliases FORCE ROW LEVEL SECURITY;
CREATE POLICY graph_node_aliases_tenant_isolation ON bighill_feature_materializer_db.graph_node_aliases
USING (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
)
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
);

ALTER TABLE bighill_feature_materializer_db.graph_node_embeddings ENABLE ROW LEVEL SECURITY;
ALTER TABLE bighill_feature_materializer_db.graph_node_embeddings FORCE ROW LEVEL SECURITY;
CREATE POLICY graph_node_embeddings_tenant_isolation ON bighill_feature_materializer_db.graph_node_embeddings
USING (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
)
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
);

ALTER TABLE bighill_feature_materializer_db.graph_edges ENABLE ROW LEVEL SECURITY;
ALTER TABLE bighill_feature_materializer_db.graph_edges FORCE ROW LEVEL SECURITY;
CREATE POLICY graph_edges_tenant_isolation ON bighill_feature_materializer_db.graph_edges
USING (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
)
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
);

ALTER TABLE bighill_feature_materializer_db.graph_node_chunks ENABLE ROW LEVEL SECURITY;
ALTER TABLE bighill_feature_materializer_db.graph_node_chunks FORCE ROW LEVEL SECURITY;
CREATE POLICY graph_node_chunks_tenant_isolation ON bighill_feature_materializer_db.graph_node_chunks
USING (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
)
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
);

ALTER TABLE bighill_feature_materializer_db.graph_communities ENABLE ROW LEVEL SECURITY;
ALTER TABLE bighill_feature_materializer_db.graph_communities FORCE ROW LEVEL SECURITY;
CREATE POLICY graph_communities_tenant_isolation ON bighill_feature_materializer_db.graph_communities
USING (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
)
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
);

ALTER TABLE bighill_feature_materializer_db.graph_community_members ENABLE ROW LEVEL SECURITY;
ALTER TABLE bighill_feature_materializer_db.graph_community_members FORCE ROW LEVEL SECURITY;
CREATE POLICY graph_community_members_tenant_isolation ON bighill_feature_materializer_db.graph_community_members
USING (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
)
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
);

ALTER TABLE bighill_feature_materializer_db.graph_community_reports ENABLE ROW LEVEL SECURITY;
ALTER TABLE bighill_feature_materializer_db.graph_community_reports FORCE ROW LEVEL SECURITY;
CREATE POLICY graph_community_reports_tenant_isolation ON bighill_feature_materializer_db.graph_community_reports
USING (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
)
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
);
