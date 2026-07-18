CREATE TYPE tool_executor_kind_enum AS ENUM ('UNKNOWN', 'HTTP_GET', 'CALCULATOR');
CREATE TYPE tool_error_type_enum AS ENUM ('TRANSIENT', 'PERMANENT', 'POLICY_DENIED');
CREATE TYPE tool_invocation_audit_status_enum AS ENUM ('COMPLETED', 'DENIED', 'FAILED');

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS bighill_tool_db.tool_invocation_audit (
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
ON bighill_tool_db.tool_invocation_audit(org_id, created_at);

CREATE INDEX IF NOT EXISTS index_tool_invocation_audit_tool_created_at
ON bighill_tool_db.tool_invocation_audit(tool_name, created_at);

ALTER TABLE bighill_tool_db.tool_invocation_audit ENABLE ROW LEVEL SECURITY;
ALTER TABLE bighill_tool_db.tool_invocation_audit FORCE ROW LEVEL SECURITY;
CREATE POLICY tool_invocation_audit_tenant_isolation ON bighill_tool_db.tool_invocation_audit
USING (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
)
WITH CHECK (
    current_setting('app.system_context', true) = 'true'
    OR NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id
);
