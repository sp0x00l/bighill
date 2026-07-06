DROP INDEX IF EXISTS bighill_ingestion_db.index_upload_sessions_client_nonce;
DROP INDEX IF EXISTS bighill_ingestion_db.index_upload_sessions_pending_expiry;
DROP INDEX IF EXISTS bighill_ingestion_db.index_upload_sessions_resource_user;
DROP INDEX IF EXISTS bighill_ingestion_db.index_upload_sessions_dataset_user;
DROP TABLE IF EXISTS bighill_ingestion_db.upload_sessions;
DROP INDEX IF EXISTS bighill_ingestion_db.index_outbox_messages_resource_key;
DROP INDEX IF EXISTS bighill_ingestion_db.index_outbox_messages_processing;
DROP INDEX IF EXISTS bighill_ingestion_db.index_outbox_messages_pending;
DROP TABLE IF EXISTS bighill_ingestion_db.outbox_messages;
DROP INDEX IF EXISTS bighill_ingestion_db.index_tenants_deleted;
DROP INDEX IF EXISTS index_dataset_user_id;
DROP TABLE IF EXISTS bighill_ingestion_db.datasets;
DROP TABLE IF EXISTS bighill_ingestion_db.tenants;

DROP TYPE IF EXISTS upload_resource_type_enum;
DROP TYPE IF EXISTS upload_session_status_enum;
DROP TYPE IF EXISTS model_source_enum;
DROP TYPE IF EXISTS processing_profile_enum;
DROP TYPE IF EXISTS catalog_provider_enum;
DROP TYPE IF EXISTS table_format_enum;

DROP FUNCTION IF EXISTS updated_at_column();
