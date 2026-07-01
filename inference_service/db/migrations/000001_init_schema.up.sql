CREATE TYPE inference_model_status_enum AS ENUM ('PENDING', 'READY', 'FAILED');

CREATE TABLE IF NOT EXISTS bighill_inference_db.inference_models (
    model_id uuid PRIMARY KEY,
    training_run_id uuid NOT NULL,
    dataset_id uuid NOT NULL,
    idempotency_key uuid NOT NULL,
    name text NOT NULL,
    model_version integer NOT NULL,
    base_model text NOT NULL,
    artifact_location text NOT NULL DEFAULT '',
    artifact_format text NOT NULL DEFAULT '',
    artifact_checksum text NOT NULL DEFAULT '',
    artifact_size_bytes bigint NOT NULL DEFAULT 0,
    metrics_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    status inference_model_status_enum NOT NULL,
    failure_reason text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS index_inference_models_training_run_id
ON bighill_inference_db.inference_models(training_run_id);

CREATE INDEX IF NOT EXISTS index_inference_models_dataset_id
ON bighill_inference_db.inference_models(dataset_id);

CREATE INDEX IF NOT EXISTS index_inference_models_status
ON bighill_inference_db.inference_models(status);

CREATE TRIGGER inference_models_updated_at
BEFORE UPDATE ON bighill_inference_db.inference_models
FOR EACH ROW
EXECUTE FUNCTION updated_at_column();
