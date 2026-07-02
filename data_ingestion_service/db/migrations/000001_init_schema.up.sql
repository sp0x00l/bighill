CREATE TABLE IF NOT EXISTS bighill_data_ingestion_db.datasets(
    dataset_id uuid NOT NULL UNIQUE,
    user_id uuid NOT NULL,
    storage_location text NOT NULL DEFAULT '',
    table_namespace text NOT NULL,
    table_name text NOT NULL,
    table_format text NOT NULL,
    catalog_provider text NOT NULL,
    processing_profile text NOT NULL,
    schema_version integer NOT NULL DEFAULT 0,
    schema_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,

    blacklisted BOOLEAN DEFAULT FALSE
);

CREATE INDEX index_dataset_user_id ON bighill_data_ingestion_db.datasets(dataset_id, user_id);
