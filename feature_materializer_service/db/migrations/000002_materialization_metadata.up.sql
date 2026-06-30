ALTER TABLE bighill_feature_materializer_db.raw_snapshots
    ADD COLUMN IF NOT EXISTS schema_version integer NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS schema_metadata jsonb NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE bighill_feature_materializer_db.feature_snapshots
    ADD COLUMN IF NOT EXISTS user_id uuid;

UPDATE bighill_feature_materializer_db.feature_snapshots feature
SET user_id = raw.user_id
FROM bighill_feature_materializer_db.raw_snapshots raw
WHERE feature.raw_snapshot_id = raw.raw_snapshot_id
  AND feature.user_id IS NULL;

ALTER TABLE bighill_feature_materializer_db.feature_snapshots
    ALTER COLUMN user_id SET NOT NULL,
    ADD COLUMN IF NOT EXISTS schema_version integer NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS schema_metadata jsonb NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE bighill_feature_materializer_db.embedding_snapshots
    ADD COLUMN IF NOT EXISTS embedding_dimensions integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS embedding_count bigint NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS bighill_feature_materializer_db.embedding_records (
    embedding_record_id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    embedding_snapshot_id uuid NOT NULL REFERENCES bighill_feature_materializer_db.embedding_snapshots(embedding_snapshot_id) ON DELETE CASCADE,
    dataset_id uuid NOT NULL,
    source_text text NOT NULL,
    embedding vector(384) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS index_embedding_records_snapshot_id ON bighill_feature_materializer_db.embedding_records(embedding_snapshot_id);
CREATE INDEX IF NOT EXISTS index_embedding_records_dataset_id ON bighill_feature_materializer_db.embedding_records(dataset_id);
