DROP INDEX IF EXISTS bighill_data_ingestion_db.index_outbox_messages_resource_key;
DROP INDEX IF EXISTS bighill_data_ingestion_db.index_outbox_messages_processing;
DROP INDEX IF EXISTS bighill_data_ingestion_db.index_outbox_messages_pending;
DROP TABLE IF EXISTS bighill_data_ingestion_db.outbox_messages;
DROP INDEX IF EXISTS index_dataset_user_id;
DROP TABLE IF EXISTS bighill_data_ingestion_db.datasets;
