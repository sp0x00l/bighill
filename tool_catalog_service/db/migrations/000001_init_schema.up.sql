CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TYPE bighill_tool_catalog_db.tool_capability_kind_enum AS ENUM ('HTTP_GET', 'MCP');
CREATE TYPE bighill_tool_catalog_db.tool_capability_lifecycle_status_enum AS ENUM ('ACTIVE');
CREATE TYPE bighill_tool_catalog_db.tenant_capability_grant_status_enum AS ENUM ('ACTIVE', 'REVOKED');

CREATE TABLE IF NOT EXISTS bighill_tool_catalog_db.tool_capability_versions (
    capability_version_id uuid PRIMARY KEY,
    capability_id text NOT NULL,
    version text NOT NULL,
    tool_name text NOT NULL,
    kind bighill_tool_catalog_db.tool_capability_kind_enum NOT NULL,
    mcp_server_endpoint text NOT NULL DEFAULT '',
    description text NOT NULL,
    parameters_json jsonb NOT NULL,
    implementation_version text NOT NULL,
    egress_hosts text[] NOT NULL,
    timeout_ms bigint NOT NULL,
    max_response_bytes bigint NOT NULL,
    credential_name text NOT NULL DEFAULT '',
    credential_required boolean NOT NULL DEFAULT false,
    lifecycle_status bighill_tool_catalog_db.tool_capability_lifecycle_status_enum NOT NULL,
    content_hash text NOT NULL,
    published_by_user_id uuid NOT NULL,
    published_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT tool_capability_versions_identity_ck CHECK (
        btrim(capability_id) <> ''
        AND btrim(version) <> ''
        AND btrim(tool_name) <> ''
        AND btrim(description) <> ''
        AND (kind <> 'MCP' OR btrim(mcp_server_endpoint) <> '')
        AND jsonb_typeof(parameters_json) = 'object'
        AND btrim(implementation_version) <> ''
        AND cardinality(egress_hosts) > 0
        AND timeout_ms > 0
        AND max_response_bytes > 0
        AND btrim(content_hash) <> ''
        AND published_by_user_id <> '00000000-0000-0000-0000-000000000000'::uuid
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS unique_tool_capability_versions_identity
ON bighill_tool_catalog_db.tool_capability_versions(capability_id, version);

CREATE UNIQUE INDEX IF NOT EXISTS unique_tool_capability_versions_content
ON bighill_tool_catalog_db.tool_capability_versions(content_hash);

CREATE INDEX IF NOT EXISTS index_tool_capability_versions_tool_name
ON bighill_tool_catalog_db.tool_capability_versions(tool_name);

ALTER TABLE bighill_tool_catalog_db.tool_capability_versions ENABLE ROW LEVEL SECURITY;
ALTER TABLE bighill_tool_catalog_db.tool_capability_versions FORCE ROW LEVEL SECURITY;
CREATE POLICY tool_capability_versions_read ON bighill_tool_catalog_db.tool_capability_versions
FOR SELECT
USING (true);
CREATE POLICY tool_capability_versions_system_insert ON bighill_tool_catalog_db.tool_capability_versions
FOR INSERT
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
);
CREATE POLICY tool_capability_versions_system_update ON bighill_tool_catalog_db.tool_capability_versions
FOR UPDATE
USING (
    current_setting('app.system_context', true) = 'true'
)
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
);
CREATE POLICY tool_capability_versions_system_delete ON bighill_tool_catalog_db.tool_capability_versions
FOR DELETE
USING (
    current_setting('app.system_context', true) = 'true'
);

CREATE TABLE IF NOT EXISTS bighill_tool_catalog_db.tenant_capability_grants (
    grant_id uuid PRIMARY KEY,
    org_id uuid NOT NULL,
    capability_version_id uuid NOT NULL,
    status bighill_tool_catalog_db.tenant_capability_grant_status_enum NOT NULL,
    granted_by_user_id uuid NOT NULL,
    granted_at timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (capability_version_id)
        REFERENCES bighill_tool_catalog_db.tool_capability_versions(capability_version_id)
        ON DELETE CASCADE,
    CONSTRAINT tenant_capability_grants_identity_ck CHECK (
        org_id <> '00000000-0000-0000-0000-000000000000'::uuid
        AND granted_by_user_id <> '00000000-0000-0000-0000-000000000000'::uuid
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS unique_tenant_capability_grants_org_capability
ON bighill_tool_catalog_db.tenant_capability_grants(org_id, capability_version_id);

CREATE INDEX IF NOT EXISTS index_tenant_capability_grants_org
ON bighill_tool_catalog_db.tenant_capability_grants(org_id, status);

ALTER TABLE bighill_tool_catalog_db.tenant_capability_grants ENABLE ROW LEVEL SECURITY;
ALTER TABLE bighill_tool_catalog_db.tenant_capability_grants FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_capability_grants_tenant_isolation ON bighill_tool_catalog_db.tenant_capability_grants
USING (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
)
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
);

CREATE TABLE IF NOT EXISTS bighill_tool_catalog_db.tool_credential_bindings (
    binding_id uuid PRIMARY KEY,
    org_id uuid NOT NULL,
    capability_id text NOT NULL,
    credential_ref text NOT NULL,
    bound_by_user_id uuid NOT NULL,
    bound_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT tool_credential_bindings_identity_ck CHECK (
        org_id <> '00000000-0000-0000-0000-000000000000'::uuid
        AND btrim(capability_id) <> ''
        AND btrim(credential_ref) <> ''
        AND bound_by_user_id <> '00000000-0000-0000-0000-000000000000'::uuid
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS unique_tool_credential_bindings_org_capability
ON bighill_tool_catalog_db.tool_credential_bindings(org_id, capability_id);

ALTER TABLE bighill_tool_catalog_db.tool_credential_bindings ENABLE ROW LEVEL SECURITY;
ALTER TABLE bighill_tool_catalog_db.tool_credential_bindings FORCE ROW LEVEL SECURITY;
CREATE POLICY tool_credential_bindings_tenant_isolation ON bighill_tool_catalog_db.tool_credential_bindings
USING (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
)
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
);

CREATE TABLE IF NOT EXISTS bighill_tool_catalog_db.outbox_messages (
    outbox_id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    dispatch_key text NOT NULL UNIQUE,
    topic text NOT NULL,
    event_type text NOT NULL,
    resource_key uuid NOT NULL,
    payload bytea NOT NULL,
    headers jsonb NOT NULL DEFAULT '[]'::jsonb,
    status text NOT NULL DEFAULT 'PENDING',
    attempts integer NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    processing_owner text NOT NULL DEFAULT '',
    claim_token text NOT NULL DEFAULT '',
    lease_expires_at timestamptz,
    last_error text NOT NULL DEFAULT '',
    sent_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT outbox_messages_status_check CHECK (status IN ('PENDING', 'PROCESSING', 'SENT'))
);

CREATE INDEX IF NOT EXISTS index_outbox_messages_pending
ON bighill_tool_catalog_db.outbox_messages(status, next_attempt_at, created_at);

CREATE INDEX IF NOT EXISTS index_outbox_messages_processing
ON bighill_tool_catalog_db.outbox_messages(status, lease_expires_at, created_at);

CREATE INDEX IF NOT EXISTS index_outbox_messages_resource_key
ON bighill_tool_catalog_db.outbox_messages(resource_key, created_at);
