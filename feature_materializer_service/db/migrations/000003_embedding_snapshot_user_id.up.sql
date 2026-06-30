ALTER TABLE bighill_feature_materializer_db.embedding_snapshots
    ADD COLUMN IF NOT EXISTS user_id uuid;

UPDATE bighill_feature_materializer_db.embedding_snapshots embedding
SET user_id = feature.user_id
FROM bighill_feature_materializer_db.feature_snapshots feature
WHERE embedding.feature_snapshot_id = feature.feature_snapshot_id
  AND embedding.user_id IS NULL;

ALTER TABLE bighill_feature_materializer_db.embedding_snapshots
    ALTER COLUMN user_id SET NOT NULL;

CREATE INDEX IF NOT EXISTS index_embedding_snapshots_user_id ON bighill_feature_materializer_db.embedding_snapshots(user_id);
