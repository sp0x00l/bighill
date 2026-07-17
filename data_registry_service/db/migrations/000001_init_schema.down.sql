DROP TRIGGER IF EXISTS updated_at_trigger ON bighill_data_registry_db.datasets;
DROP TRIGGER IF EXISTS updated_at_trigger ON bighill_data_registry_db.connectors;
DROP TRIGGER IF EXISTS updated_at_trigger ON bighill_data_registry_db.metadata;
DROP TRIGGER IF EXISTS updated_at_trigger ON bighill_data_registry_db.dataset_materialization_event_state;
DROP TRIGGER IF EXISTS updated_at_trigger ON bighill_data_registry_db.tenants;

DROP INDEX IF EXISTS bighill_data_registry_db.index_outbox_messages_resource_key;
DROP INDEX IF EXISTS bighill_data_registry_db.index_outbox_messages_processing;
DROP INDEX IF EXISTS bighill_data_registry_db.index_outbox_messages_pending;
DROP INDEX IF EXISTS bighill_data_registry_db.index_dataset_materialization_event_state_org_id;
DROP INDEX IF EXISTS bighill_data_registry_db.index_metadata_user_id;
DROP INDEX IF EXISTS bighill_data_registry_db.index_tenants_deleted;
DROP INDEX IF EXISTS bighill_data_registry_db.index_dataset_processing_state;
DROP INDEX IF EXISTS bighill_data_registry_db.index_user_id;
DROP INDEX IF EXISTS bighill_data_registry_db.index_dataset_table_ref;
DROP INDEX IF EXISTS bighill_data_registry_db.index_dataset_raw_snapshot_id;
DROP INDEX IF EXISTS bighill_data_registry_db.index_dataset_feature_snapshot_id;
DROP INDEX IF EXISTS bighill_data_registry_db.index_dataset_embedding_snapshot_id;
DROP INDEX IF EXISTS bighill_data_registry_db.index_dataset_graph_snapshot_id;
DROP INDEX IF EXISTS bighill_data_registry_db.index_dataset_source_connector_id;
DROP INDEX IF EXISTS bighill_data_registry_db.index_dataset_id_connectors;
DROP INDEX IF EXISTS bighill_data_registry_db.index_dataset_id_metadata;

DROP TABLE IF EXISTS bighill_data_registry_db.outbox_messages;
DROP TABLE IF EXISTS bighill_data_registry_db.dataset_materialization_event_state;
DROP TABLE IF EXISTS bighill_data_registry_db.metadata;
DROP TABLE IF EXISTS bighill_data_registry_db.datasets;
DROP TABLE IF EXISTS bighill_data_registry_db.connectors;
DROP TABLE IF EXISTS bighill_data_registry_db.tenants;

DROP TYPE IF EXISTS dataset_processing_state_enum;
DROP TYPE IF EXISTS dataset_origin_enum;
DROP TYPE IF EXISTS storage_type_enum;
DROP TYPE IF EXISTS dataset_status_enum;
DROP TYPE IF EXISTS table_format_enum;
DROP TYPE IF EXISTS catalog_provider_enum;
DROP TYPE IF EXISTS processing_profile_enum;

DROP FUNCTION IF EXISTS updated_at_column();
