CREATE EXTENSION IF NOT EXISTS citext;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE OR REPLACE FUNCTION updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TYPE org_member_role_enum AS ENUM ('consumer', 'ml_researcher', 'org_admin');
CREATE TYPE org_member_status_enum AS ENUM ('active', 'invited', 'disabled');

CREATE TABLE IF NOT EXISTS bighill_profile_db.profiles(
    id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
    idempotency_key uuid UNIQUE NOT NULL, 
    default_org_id uuid,
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
    huggingface_token_ciphertext text NOT NULL DEFAULT '',
    email_verified BOOLEAN NOT NULL DEFAULT FALSE,
    email_verify_token_hash VARCHAR(64),
    email_verify_expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ,
    deleted BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS bighill_profile_db.organizations(
    id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
    display_name VARCHAR(255) NOT NULL,
    created_by_user_id uuid REFERENCES bighill_profile_db.profiles(id),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ,
    deleted BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS bighill_profile_db.organization_memberships(
    org_id uuid NOT NULL REFERENCES bighill_profile_db.organizations(id) ON DELETE CASCADE,
    user_id uuid NOT NULL REFERENCES bighill_profile_db.profiles(id) ON DELETE CASCADE,
    role org_member_role_enum NOT NULL,
    status org_member_status_enum NOT NULL DEFAULT 'active',
    created_by_user_id uuid REFERENCES bighill_profile_db.profiles(id),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ,
    PRIMARY KEY (org_id, user_id)
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
CREATE INDEX idx_profiles_default_org_id ON bighill_profile_db.profiles(default_org_id);
CREATE INDEX idx_org_memberships_user_status ON bighill_profile_db.organization_memberships(user_id, status);
CREATE INDEX idx_org_memberships_org_status ON bighill_profile_db.organization_memberships(org_id, status);

CREATE TRIGGER trg_profiles_set_updated_at BEFORE INSERT OR UPDATE ON bighill_profile_db.profiles FOR EACH ROW EXECUTE FUNCTION updated_at_column();
CREATE TRIGGER trg_organizations_set_updated_at BEFORE INSERT OR UPDATE ON bighill_profile_db.organizations FOR EACH ROW EXECUTE FUNCTION updated_at_column();
CREATE TRIGGER trg_organization_memberships_set_updated_at BEFORE INSERT OR UPDATE ON bighill_profile_db.organization_memberships FOR EACH ROW EXECUTE FUNCTION updated_at_column();
CREATE TRIGGER trg_oauth_identities_set_updated_at BEFORE INSERT OR UPDATE ON bighill_profile_db.oauth_identities FOR EACH ROW EXECUTE FUNCTION updated_at_column();

ALTER TABLE bighill_profile_db.profiles
    ADD CONSTRAINT profiles_default_org_fk
    FOREIGN KEY (default_org_id) REFERENCES bighill_profile_db.organizations(id);
