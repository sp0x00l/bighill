CREATE OR REPLACE FUNCTION updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE IF NOT EXISTS bighill_profile_db.profiles(
    id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
    idempotency_key uuid UNIQUE NOT NULL, 
    email citext NOT NULL,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    phone_number VARCHAR(20),
    date_of_birth DATE,
    country_code VARCHAR(10),
    address_line_1 VARCHAR(255),
    address_line_2 VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(100),
    postal_code VARCHAR(20),
    country VARCHAR(100),
    password_hash text NOT NULL,    
    email_verified BOOLEAN NOT NULL DEFAULT FALSE,
    email_verify_token_hash VARCHAR(64),
    email_verify_expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ,
    deleted BOOLEAN DEFAULT FALSE
);



CREATE TABLE IF NOT EXISTS bighill_profile_db.oauth_identities(
    id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
    profile_id uuid NOT NULL REFERENCES bighill_profile_db.profiles(id) ON DELETE CASCADE,
    provider VARCHAR(32) NOT NULL,
    provider_subject VARCHAR(255) NOT NULL,
    email citext NOT NULL,
    email_verified BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ
);


CREATE UNIQUE INDEX idx_profile_email_lower_deleted_false ON bighill_profile_db.profiles (lower(email)) WHERE deleted = false;
CREATE UNIQUE INDEX idx_profile_phone_number_deleted_false ON bighill_profile_db.profiles (phone_number) WHERE deleted = false;
CREATE UNIQUE INDEX idx_oauth_identities_provider_subject ON bighill_profile_db.oauth_identities (provider, provider_subject);

CREATE TRIGGER trg_profiles_set_updated_at BEFORE INSERT OR UPDATE ON bighill_profile_db.profiles FOR EACH ROW EXECUTE FUNCTION updated_at_column();
CREATE TRIGGER trg_oauth_identities_set_updated_at BEFORE INSERT OR UPDATE ON bighill_profile_db.oauth_identities FOR EACH ROW EXECUTE FUNCTION updated_at_column();
