DROP TRIGGER IF EXISTS embedding_snapshots_updated_at ON bighill_feature_materializer_db.embedding_snapshots;
DROP TRIGGER IF EXISTS feature_snapshots_updated_at ON bighill_feature_materializer_db.feature_snapshots;
DROP TRIGGER IF EXISTS raw_snapshots_updated_at ON bighill_feature_materializer_db.raw_snapshots;

DROP TABLE IF EXISTS bighill_feature_materializer_db.embedding_snapshots;
DROP TABLE IF EXISTS bighill_feature_materializer_db.feature_snapshots;
DROP TABLE IF EXISTS bighill_feature_materializer_db.raw_snapshots;

DROP TYPE IF EXISTS snapshot_status_enum;

DROP EXTENSION IF EXISTS vector;
