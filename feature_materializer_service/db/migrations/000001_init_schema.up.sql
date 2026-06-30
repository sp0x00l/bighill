CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public;

CREATE TYPE snapshot_status_enum AS ENUM ('PENDING', 'READY', 'FAILED');

CREATE TABLE IF NOT EXISTS bighill_feature_materializer_db.raw_snapshots (
    raw_snapshot_id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    dataset_id uuid NOT NULL,
    user_id uuid NOT NULL,
    idempotency_key uuid NOT NULL UNIQUE,
    source_storage_location text NOT NULL,
    storage_location text NOT NULL,
    content_type text NOT NULL,
    file_extension text NOT NULL,
    table_namespace text NOT NULL,
    table_name text NOT NULL,
    table_format text NOT NULL,
    catalog_provider text NOT NULL,
    status snapshot_status_enum NOT NULL DEFAULT 'PENDING',
    failure_reason text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS index_raw_snapshots_dataset_id ON bighill_feature_materializer_db.raw_snapshots(dataset_id);
CREATE INDEX IF NOT EXISTS index_raw_snapshots_user_id ON bighill_feature_materializer_db.raw_snapshots(user_id);

CREATE TRIGGER raw_snapshots_updated_at
BEFORE UPDATE ON bighill_feature_materializer_db.raw_snapshots
FOR EACH ROW
EXECUTE FUNCTION updated_at_column();

CREATE TABLE IF NOT EXISTS bighill_feature_materializer_db.feature_snapshots (
    feature_snapshot_id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    raw_snapshot_id uuid NOT NULL REFERENCES bighill_feature_materializer_db.raw_snapshots(raw_snapshot_id),
    dataset_id uuid NOT NULL,
    idempotency_key uuid NOT NULL UNIQUE,
    storage_location text NOT NULL DEFAULT '',
    table_namespace text NOT NULL,
    table_name text NOT NULL,
    table_format text NOT NULL,
    catalog_provider text NOT NULL,
    status snapshot_status_enum NOT NULL DEFAULT 'PENDING',
    failure_reason text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS index_feature_snapshots_raw_snapshot_id ON bighill_feature_materializer_db.feature_snapshots(raw_snapshot_id);
CREATE INDEX IF NOT EXISTS index_feature_snapshots_dataset_id ON bighill_feature_materializer_db.feature_snapshots(dataset_id);

CREATE TRIGGER feature_snapshots_updated_at
BEFORE UPDATE ON bighill_feature_materializer_db.feature_snapshots
FOR EACH ROW
EXECUTE FUNCTION updated_at_column();

CREATE TABLE IF NOT EXISTS bighill_feature_materializer_db.embedding_snapshots (
    embedding_snapshot_id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    feature_snapshot_id uuid NOT NULL REFERENCES bighill_feature_materializer_db.feature_snapshots(feature_snapshot_id),
    dataset_id uuid NOT NULL,
    idempotency_key uuid NOT NULL UNIQUE,
    vector_store text NOT NULL DEFAULT '',
    collection_name text NOT NULL DEFAULT '',
    status snapshot_status_enum NOT NULL DEFAULT 'PENDING',
    failure_reason text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS index_embedding_snapshots_feature_snapshot_id ON bighill_feature_materializer_db.embedding_snapshots(feature_snapshot_id);
CREATE INDEX IF NOT EXISTS index_embedding_snapshots_dataset_id ON bighill_feature_materializer_db.embedding_snapshots(dataset_id);

CREATE TRIGGER embedding_snapshots_updated_at
BEFORE UPDATE ON bighill_feature_materializer_db.embedding_snapshots
FOR EACH ROW
EXECUTE FUNCTION updated_at_column();
