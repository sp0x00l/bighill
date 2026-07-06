DROP TRIGGER IF EXISTS models_updated_at ON bighill_model_registry_db.models;
DROP TRIGGER IF EXISTS tenants_updated_at ON bighill_model_registry_db.tenants;
DROP TABLE IF EXISTS bighill_model_registry_db.models;
DROP TABLE IF EXISTS bighill_model_registry_db.tenants;
DROP TYPE IF EXISTS promotion_decision_enum;
DROP TYPE IF EXISTS model_source_enum;
DROP TYPE IF EXISTS model_kind_enum;
DROP TYPE IF EXISTS model_load_status_enum;
DROP TYPE IF EXISTS model_status_enum;

DROP FUNCTION IF EXISTS updated_at_column();
