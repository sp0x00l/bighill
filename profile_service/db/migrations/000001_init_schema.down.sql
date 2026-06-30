DROP TRIGGER IF EXISTS trg_oauth_identities_set_updated_at ON bighill_profile_db.oauth_identities;
DROP TRIGGER IF EXISTS trg_profiles_set_updated_at ON bighill_profile_db.profiles;
DROP TABLE IF EXISTS bighill_profile_db.oauth_identities;
DROP TABLE IF EXISTS bighill_profile_db.profiles;
DROP FUNCTION IF EXISTS updated_at_column();
