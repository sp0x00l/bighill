DROP INDEX IF EXISTS bighill_data_registry_db.index_dataset_processing_state;

ALTER TABLE bighill_data_registry_db.datasets
    DROP COLUMN IF EXISTS processing_state;

DROP TYPE IF EXISTS dataset_processing_state_enum;
