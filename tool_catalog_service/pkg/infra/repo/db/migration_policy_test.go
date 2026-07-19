package db_test

import (
	"os"
	"path/filepath"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("tool catalog migration policy", func() {
	It("keeps capability writes system-scoped and tenant rows org-scoped", func() {
		contentBytes, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "db", "migrations", "000001_init_schema.up.sql"))
		Expect(err).ToNot(HaveOccurred())
		content := string(contentBytes)

		Expect(content).To(ContainSubstring("ALTER TABLE bighill_tool_catalog_db.tool_capability_versions FORCE ROW LEVEL SECURITY"))
		Expect(content).To(ContainSubstring("CREATE POLICY tool_capability_versions_read"))
		Expect(content).To(ContainSubstring("CREATE POLICY tool_capability_versions_system_insert"))
		Expect(content).To(ContainSubstring("CREATE POLICY tool_capability_versions_system_update"))
		Expect(content).To(ContainSubstring("CREATE POLICY tool_capability_versions_system_delete"))
		Expect(content).To(ContainSubstring("current_setting('app.system_context', true) = 'true'"))
		for _, table := range []string{"tenant_capability_grants", "tool_credential_bindings"} {
			Expect(content).To(ContainSubstring("ALTER TABLE bighill_tool_catalog_db." + table + " FORCE ROW LEVEL SECURITY"))
		}
		Expect(content).To(ContainSubstring("NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id"))
		Expect(strings.Contains(content, "COALESCE(NULLIF(current_setting('app.current_user_id'")).To(BeFalse())
		Expect(strings.Contains(content, "COALESCE(NULLIF(current_setting('app.current_org_id'")).To(BeFalse())
		Expect(strings.Contains(content, "user_id = user_id")).To(BeFalse())
		Expect(strings.Contains(content, "org_id = org_id")).To(BeFalse())
		Expect(strings.Contains(content, "status = 'published'")).To(BeFalse())
	})
})
