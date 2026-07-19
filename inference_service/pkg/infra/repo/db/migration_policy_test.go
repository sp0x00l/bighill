package db_test

import (
	"os"
	"path/filepath"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("inference migration policy", func() {
	It("keeps tenant-scoped tables fail-closed", func() {
		contentBytes, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "db", "migrations", "000001_init_schema.up.sql"))
		Expect(err).ToNot(HaveOccurred())
		content := string(contentBytes)

		Expect(content).To(ContainSubstring("ALTER TABLE bighill_inference_db.tenants FORCE ROW LEVEL SECURITY"))
		Expect(content).To(ContainSubstring("NULLIF(current_setting('app.current_user_id', true), '')::uuid = id"))
		for _, table := range []string{
			"inference_models",
			"inference_datasets",
			"inference_requests",
			"inference_feedback",
			"preference_examples",
			"lineage_eval_sets",
			"lineage_eval_examples",
			"preference_dataset_snapshots",
			"published_inference_endpoints",
			"published_endpoint_datasets",
			"agent_specs",
			"agent_runs",
			"agent_steps",
			"agent_tool_invocations",
		} {
			Expect(content).To(ContainSubstring("ALTER TABLE bighill_inference_db." + table + " FORCE ROW LEVEL SECURITY"))
		}
		Expect(content).To(ContainSubstring("current_setting('app.system_context', true) = 'true'"))
		Expect(content).To(ContainSubstring("NULLIF(current_setting('app.current_org_id', true), '')::uuid"))
		Expect(strings.Contains(content, "COALESCE(NULLIF(current_setting('app.current_user_id'")).To(BeFalse())
		Expect(strings.Contains(content, "COALESCE(NULLIF(current_setting('app.current_org_id'")).To(BeFalse())
		Expect(strings.Contains(content, "user_id = user_id")).To(BeFalse())
		Expect(strings.Contains(content, "org_id = org_id")).To(BeFalse())
		Expect(strings.Contains(content, "status = 'published'")).To(BeFalse())
	})
})
