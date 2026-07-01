CREATE TYPE model_status_enum AS ENUM ('PENDING', 'READY', 'FAILED');

CREATE TABLE IF NOT EXISTS bighill_model_registry_db.models (
    model_id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    idempotency_key uuid UNIQUE NOT NULL,
    training_run_id uuid NOT NULL,
    dataset_id uuid NOT NULL,
    name text NOT NULL,
    model_version integer NOT NULL DEFAULT 1,
    base_model text NOT NULL,
    artifact_location text NOT NULL DEFAULT '',
    metrics_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
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
