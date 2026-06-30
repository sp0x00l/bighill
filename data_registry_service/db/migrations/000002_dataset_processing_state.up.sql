CREATE TYPE dataset_processing_state_enum AS ENUM (
    'PENDING',
    'RAW_MATERIALIZED',
    'FEATURE_MATERIALIZED',
    'EMBEDDINGS_MATERIALIZED',
    'FAILED'
);

ALTER TABLE bighill_data_registry_db.datasets
    ADD COLUMN IF NOT EXISTS processing_state dataset_processing_state_enum NOT NULL DEFAULT 'PENDING';

CREATE INDEX IF NOT EXISTS index_dataset_processing_state ON bighill_data_registry_db.datasets(processing_state);
