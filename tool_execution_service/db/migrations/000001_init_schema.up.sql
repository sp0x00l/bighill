CREATE TYPE tool_executor_kind_enum AS ENUM ('UNKNOWN', 'HTTP_GET', 'CALCULATOR', 'MCP');
CREATE TYPE tool_error_type_enum AS ENUM ('TRANSIENT', 'PERMANENT', 'POLICY_DENIED');
CREATE TYPE tool_invocation_audit_status_enum AS ENUM ('COMPLETED', 'DENIED', 'FAILED');

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS bighill_tool_execution_db.tool_invocation_audit (
    invocation_id uuid PRIMARY KEY,
    org_id uuid NOT NULL,
    user_id uuid NOT NULL,
    tool_name text NOT NULL,
    tool_impl_version text NOT NULL DEFAULT '',
    executor_kind tool_executor_kind_enum NOT NULL,
    status tool_invocation_audit_status_enum NOT NULL,
    error_code text NOT NULL DEFAULT '',
    error_type tool_error_type_enum,
    latency_ms bigint NOT NULL DEFAULT 0,
    egress_host text NOT NULL DEFAULT '',
    trace_id text NOT NULL DEFAULT '',
    args_hash text NOT NULL,
    args_preview text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT tool_invocation_audit_payload_ck CHECK (
        invocation_id <> '00000000-0000-0000-0000-000000000000'::uuid
        AND org_id <> '00000000-0000-0000-0000-000000000000'::uuid
        AND user_id <> '00000000-0000-0000-0000-000000000000'::uuid
        AND btrim(tool_name) <> ''
        AND btrim(args_hash) <> ''
    )
);

CREATE INDEX IF NOT EXISTS index_tool_invocation_audit_org_created_at
ON bighill_tool_execution_db.tool_invocation_audit(org_id, created_at);

CREATE INDEX IF NOT EXISTS index_tool_invocation_audit_tool_created_at
ON bighill_tool_execution_db.tool_invocation_audit(tool_name, created_at);

CREATE TABLE IF NOT EXISTS bighill_tool_execution_db.tool_capability_projections (
    capability_version_id uuid PRIMARY KEY,
    capability_id text NOT NULL,
    version text NOT NULL,
    tool_name text NOT NULL,
    executor_kind tool_executor_kind_enum NOT NULL,
    description text NOT NULL,
    mcp_server_endpoint text NOT NULL DEFAULT '',
    parameters_json jsonb NOT NULL,
    implementation_version text NOT NULL,
    egress_hosts text[] NOT NULL,
    timeout_ms bigint NOT NULL,
    max_response_bytes bigint NOT NULL,
    credential_name text NOT NULL DEFAULT '',
    credential_required boolean NOT NULL DEFAULT false,
    lifecycle_status text NOT NULL,
    content_hash text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT tool_capability_projections_identity_ck CHECK (
        btrim(capability_id) <> ''
        AND btrim(version) <> ''
        AND btrim(tool_name) <> ''
        AND btrim(description) <> ''
        AND (executor_kind <> 'MCP' OR btrim(mcp_server_endpoint) <> '')
        AND jsonb_typeof(parameters_json) = 'object'
        AND btrim(implementation_version) <> ''
        AND cardinality(egress_hosts) > 0
        AND timeout_ms > 0
        AND max_response_bytes > 0
        AND btrim(lifecycle_status) <> ''
        AND btrim(content_hash) <> ''
    )
);

CREATE INDEX IF NOT EXISTS index_tool_capability_projections_tool_name
ON bighill_tool_execution_db.tool_capability_projections(tool_name);

CREATE TABLE IF NOT EXISTS bighill_tool_execution_db.tool_grant_projections (
    org_id uuid NOT NULL,
    capability_version_id uuid NOT NULL,
    status text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, capability_version_id),
    FOREIGN KEY (capability_version_id)
        REFERENCES bighill_tool_execution_db.tool_capability_projections(capability_version_id)
        ON DELETE CASCADE,
    CONSTRAINT tool_grant_projections_identity_ck CHECK (
        org_id <> '00000000-0000-0000-0000-000000000000'::uuid
        AND btrim(status) <> ''
    )
);

CREATE INDEX IF NOT EXISTS index_tool_grant_projections_org_status
ON bighill_tool_execution_db.tool_grant_projections(org_id, status);

ALTER TABLE bighill_tool_execution_db.tool_grant_projections ENABLE ROW LEVEL SECURITY;
ALTER TABLE bighill_tool_execution_db.tool_grant_projections FORCE ROW LEVEL SECURITY;
CREATE POLICY tool_grant_projections_tenant_isolation ON bighill_tool_execution_db.tool_grant_projections
USING (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
)
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
);

CREATE TABLE IF NOT EXISTS bighill_tool_execution_db.tool_credential_binding_projections (
    org_id uuid NOT NULL,
    capability_id text NOT NULL,
    credential_ref text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, capability_id),
    CONSTRAINT tool_credential_binding_projections_identity_ck CHECK (
        org_id <> '00000000-0000-0000-0000-000000000000'::uuid
        AND btrim(capability_id) <> ''
        AND btrim(credential_ref) <> ''
    )
);

ALTER TABLE bighill_tool_execution_db.tool_credential_binding_projections ENABLE ROW LEVEL SECURITY;
ALTER TABLE bighill_tool_execution_db.tool_credential_binding_projections FORCE ROW LEVEL SECURITY;
CREATE POLICY tool_credential_binding_projections_tenant_isolation ON bighill_tool_execution_db.tool_credential_binding_projections
USING (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
)
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
);

ALTER TABLE bighill_tool_execution_db.tool_invocation_audit ENABLE ROW LEVEL SECURITY;
ALTER TABLE bighill_tool_execution_db.tool_invocation_audit FORCE ROW LEVEL SECURITY;
CREATE POLICY tool_invocation_audit_tenant_isolation ON bighill_tool_execution_db.tool_invocation_audit
USING (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
)
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
);
