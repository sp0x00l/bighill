CREATE TABLE IF NOT EXISTS bighill_data_ingestion_db.datasets(
    dataset_id uuid NOT NULL UNIQUE,
    user_id uuid NOT NULL,

    blacklisted BOOLEAN DEFAULT FALSE
);

CREATE INDEX index_dataset_user_id ON bighill_data_ingestion_db.datasets(dataset_id, user_id);
