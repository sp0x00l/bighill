CREATE UNIQUE INDEX IF NOT EXISTS index_models_training_run_id_unique
ON bighill_model_registry_db.models(training_run_id);
