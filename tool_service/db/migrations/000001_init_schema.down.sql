DROP POLICY IF EXISTS tool_invocation_audit_tenant_isolation ON bighill_tool_db.tool_invocation_audit;
DROP INDEX IF EXISTS bighill_tool_db.index_tool_invocation_audit_tool_created_at;
DROP INDEX IF EXISTS bighill_tool_db.index_tool_invocation_audit_org_created_at;
DROP TABLE IF EXISTS bighill_tool_db.tool_invocation_audit;
DROP TYPE IF EXISTS tool_invocation_audit_status_enum;
DROP TYPE IF EXISTS tool_error_type_enum;
DROP TYPE IF EXISTS tool_executor_kind_enum;
