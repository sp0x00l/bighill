DROP INDEX IF EXISTS bighill_feature_materializer_db.index_embedding_snapshots_user_id;

ALTER TABLE bighill_feature_materializer_db.embedding_snapshots
    DROP COLUMN IF EXISTS user_id;
