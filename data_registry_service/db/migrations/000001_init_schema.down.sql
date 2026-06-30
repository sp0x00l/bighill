DROP INDEX IF EXISTS index_user_id;
DROP INDEX IF EXISTS index_dataset_table_ref;
DROP INDEX IF EXISTS index_dataset_id_connectors;
DROP INDEX IF EXISTS index_dataset_id_metadata;

DROP TABLE IF EXISTS bighill_data_registry_db.metadata;
DROP TABLE IF EXISTS bighill_data_registry_db.datasets;
DROP TABLE IF EXISTS bighill_data_registry_db.connectors;

DROP TYPE IF EXISTS origin_enum;
DROP TYPE IF EXISTS storage_type_enum;
DROP TYPE IF EXISTS status_enum;
DROP TYPE IF EXISTS table_format_enum;
DROP TYPE IF EXISTS catalog_provider_enum;

DROP TRIGGER IF EXISTS updated_at_trigger ON bighill_data_registry_db.datasets;
DROP TRIGGER IF EXISTS updated_at_trigger ON bighill_data_registry_db.connectors;
DROP TRIGGER IF EXISTS updated_at_trigger ON bighill_data_registry_db.metadata;
