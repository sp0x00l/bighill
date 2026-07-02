CREATE TYPE model_status_enum AS ENUM ('PENDING', 'CANDIDATE', 'EVALUATED', 'READY', 'FAILED');
CREATE TYPE model_load_status_enum AS ENUM ('NOT_LOADED', 'LOADED', 'FAILED');

CREATE TABLE IF NOT EXISTS bighill_model_registry_db.models (
    model_id uuid PRIMARY KEY,
    idempotency_key uuid UNIQUE NOT NULL,
    training_run_id uuid NOT NULL,
    dataset_id uuid NOT NULL,
    name text NOT NULL,
    model_version integer NOT NULL,
    base_model text NOT NULL,
    artifact_location text NOT NULL,
    artifact_format text NOT NULL,
    artifact_checksum text NOT NULL,
    artifact_size_bytes bigint NOT NULL,
    adapter_uri text NOT NULL DEFAULT '',
    serving_target text NOT NULL DEFAULT '',
    serving_model text NOT NULL DEFAULT '',
    serving_load_status model_load_status_enum NOT NULL DEFAULT 'NOT_LOADED',
    metrics_metadata jsonb NOT NULL,
    status model_status_enum NOT NULL DEFAULT 'PENDING',
    failure_reason text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS index_models_training_run_id
ON bighill_model_registry_db.models(training_run_id);

CREATE INDEX IF NOT EXISTS index_models_dataset_id
ON bighill_model_registry_db.models(dataset_id);

CREATE TRIGGER models_updated_at
BEFORE UPDATE ON bighill_model_registry_db.models
FOR EACH ROW
EXECUTE FUNCTION updated_at_column();
