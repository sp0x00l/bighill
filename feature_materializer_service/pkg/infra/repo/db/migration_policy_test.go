package db_test

import (
	"os"
	"path/filepath"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("feature materializer migration policy", func() {
	It("keeps tenant-scoped tables fail-closed", func() {
		contentBytes, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "db", "migrations", "000001_init_schema.up.sql"))
		Expect(err).ToNot(HaveOccurred())
		content := string(contentBytes)

		Expect(content).To(ContainSubstring("ALTER TABLE bighill_feature_materializer_db.tenants FORCE ROW LEVEL SECURITY"))
		Expect(content).To(ContainSubstring("NULLIF(current_setting('app.current_user_id', true), '')::uuid = id"))
		for _, table := range []string{
			"dataset_materialization_event_state",
			"raw_snapshots",
			"feature_snapshots",
			"embedding_snapshots",
			"embedding_records",
			"graph_snapshots",
			"graph_nodes",
			"graph_node_aliases",
			"graph_node_embeddings",
			"graph_edges",
			"graph_node_chunks",
			"graph_communities",
			"graph_community_members",
			"graph_community_reports",
		} {
			Expect(content).To(ContainSubstring("ALTER TABLE bighill_feature_materializer_db." + table + " FORCE ROW LEVEL SECURITY"))
			Expect(content).To(ContainSubstring("CREATE POLICY " + table + "_tenant_isolation"))
		}
		Expect(content).To(ContainSubstring("current_setting('app.system_context', true) = 'true'"))
		Expect(content).To(ContainSubstring("NULLIF(current_setting('app.current_org_id', true), '')::uuid = org_id"))
		Expect(content).To(ContainSubstring("index_graph_node_embeddings_embedding_384_hnsw"))
		Expect(content).To(ContainSubstring("ON bighill_feature_materializer_db.graph_node_embeddings"))
		Expect(content).To(ContainSubstring("USING hnsw ((embedding::vector(384)) vector_cosine_ops)"))
		Expect(content).To(ContainSubstring("index_graph_community_reports_embedding_384_hnsw"))
		Expect(content).To(ContainSubstring("ON bighill_feature_materializer_db.graph_community_reports"))
		Expect(strings.Contains(content, "COALESCE(NULLIF(current_setting('app.current_user_id'")).To(BeFalse())
		Expect(strings.Contains(content, "COALESCE(NULLIF(current_setting('app.current_org_id'")).To(BeFalse())
		Expect(strings.Contains(content, "user_id = user_id")).To(BeFalse())
		Expect(strings.Contains(content, "org_id = org_id")).To(BeFalse())
		Expect(strings.Contains(content, "status = 'published'")).To(BeFalse())
	})
})
