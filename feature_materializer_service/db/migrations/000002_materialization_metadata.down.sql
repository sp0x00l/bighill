DROP INDEX IF EXISTS bighill_feature_materializer_db.index_embedding_records_dataset_id;
DROP INDEX IF EXISTS bighill_feature_materializer_db.index_embedding_records_snapshot_id;
DROP TABLE IF EXISTS bighill_feature_materializer_db.embedding_records;

ALTER TABLE bighill_feature_materializer_db.embedding_snapshots
    DROP COLUMN IF EXISTS embedding_count,
    DROP COLUMN IF EXISTS embedding_dimensions;

ALTER TABLE bighill_feature_materializer_db.feature_snapshots
    DROP COLUMN IF EXISTS schema_metadata,
    DROP COLUMN IF EXISTS schema_version,
    DROP COLUMN IF EXISTS user_id;

ALTER TABLE bighill_feature_materializer_db.raw_snapshots
    DROP COLUMN IF EXISTS schema_metadata,
    DROP COLUMN IF EXISTS schema_version;
