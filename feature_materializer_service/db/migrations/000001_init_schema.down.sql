DROP TRIGGER IF EXISTS outbox_messages_updated_at ON bighill_feature_materializer_db.outbox_messages;
DROP TRIGGER IF EXISTS embedding_snapshots_updated_at ON bighill_feature_materializer_db.embedding_snapshots;
DROP TRIGGER IF EXISTS feature_snapshots_updated_at ON bighill_feature_materializer_db.feature_snapshots;
DROP TRIGGER IF EXISTS raw_snapshots_updated_at ON bighill_feature_materializer_db.raw_snapshots;
DROP TRIGGER IF EXISTS dataset_materialization_event_state_updated_at ON bighill_feature_materializer_db.dataset_materialization_event_state;
DROP TRIGGER IF EXISTS tenants_updated_at ON bighill_feature_materializer_db.tenants;

DROP TABLE IF EXISTS bighill_feature_materializer_db.outbox_messages;
DROP TABLE IF EXISTS bighill_feature_materializer_db.embedding_records;
DROP TABLE IF EXISTS bighill_feature_materializer_db.embedding_snapshots;
DROP TABLE IF EXISTS bighill_feature_materializer_db.feature_snapshots;
DROP TABLE IF EXISTS bighill_feature_materializer_db.raw_snapshots;
DROP TABLE IF EXISTS bighill_feature_materializer_db.dataset_materialization_event_state;
DROP TABLE IF EXISTS bighill_feature_materializer_db.tenants;

DROP TYPE IF EXISTS processing_profile_enum;
DROP TYPE IF EXISTS catalog_provider_enum;
DROP TYPE IF EXISTS table_format_enum;
DROP TYPE IF EXISTS snapshot_status_enum;

DROP EXTENSION IF EXISTS vector;

DROP FUNCTION IF EXISTS updated_at_column();
