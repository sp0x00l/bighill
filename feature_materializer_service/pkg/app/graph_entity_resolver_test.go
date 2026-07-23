package app_test

import (
	"context"
	"errors"

	usecase "feature_materializer_service/pkg/app"
	"feature_materializer_service/pkg/domain"
	"feature_materializer_service/pkg/domain/model"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("EmbeddingGraphEntityResolver", func() {
	It("merges same-type semantic duplicates and preserves resolver artifacts", func() {
		graphSnapshot := validGraphSnapshot()
		embeddingSnapshot := validSearchEmbeddingSnapshot(graphSnapshot.DatasetID)
		embeddingSnapshot.EmbeddingSnapshotID = graphSnapshot.EmbeddingSnapshotID
		embeddingSnapshot.UserID = graphSnapshot.UserID
		embeddingSnapshot.OrgID = graphSnapshot.OrgID
		embeddingSnapshot.EmbeddingDimensions = 2
		recordA := uuid.New()
		recordB := uuid.New()
		recordC := uuid.New()
		materialization := &model.GraphMaterialization{
			Snapshot: graphSnapshot,
			Nodes: []model.GraphNode{
				{GraphSnapshotID: graphSnapshot.GraphSnapshotID, DatasetID: graphSnapshot.DatasetID, UserID: graphSnapshot.UserID, OrgID: graphSnapshot.OrgID, EntityKey: "system:aurora relay", Name: "Aurora Relay", Type: "system", Description: "Routes payloads", MentionCount: 3},
				{GraphSnapshotID: graphSnapshot.GraphSnapshotID, DatasetID: graphSnapshot.DatasetID, UserID: graphSnapshot.UserID, OrgID: graphSnapshot.OrgID, EntityKey: "system:aurora gateway", Name: "Aurora Gateway", Type: "system", Description: "Routes payload traffic", MentionCount: 1},
				{GraphSnapshotID: graphSnapshot.GraphSnapshotID, DatasetID: graphSnapshot.DatasetID, UserID: graphSnapshot.UserID, OrgID: graphSnapshot.OrgID, EntityKey: "system:beacon hub", Name: "Beacon Hub", Type: "system", Description: "Receives payloads", MentionCount: 1},
			},
			NodeChunks: []model.GraphNodeChunk{
				{EntityKey: "system:aurora relay", EmbeddingRecordID: recordA, EmbeddingSnapshotID: embeddingSnapshot.EmbeddingSnapshotID, DatasetID: graphSnapshot.DatasetID, UserID: graphSnapshot.UserID, OrgID: graphSnapshot.OrgID, ChunkIndex: 0, SourceText: "Aurora Relay routes payloads."},
				{EntityKey: "system:aurora gateway", EmbeddingRecordID: recordB, EmbeddingSnapshotID: embeddingSnapshot.EmbeddingSnapshotID, DatasetID: graphSnapshot.DatasetID, UserID: graphSnapshot.UserID, OrgID: graphSnapshot.OrgID, ChunkIndex: 1, SourceText: "Aurora Gateway routes payload traffic."},
				{EntityKey: "system:beacon hub", EmbeddingRecordID: recordC, EmbeddingSnapshotID: embeddingSnapshot.EmbeddingSnapshotID, DatasetID: graphSnapshot.DatasetID, UserID: graphSnapshot.UserID, OrgID: graphSnapshot.OrgID, ChunkIndex: 2, SourceText: "Beacon Hub receives payloads."},
			},
			Edges: []model.GraphEdge{
				{DatasetID: graphSnapshot.DatasetID, UserID: graphSnapshot.UserID, OrgID: graphSnapshot.OrgID, SourceEntityKey: "system:aurora relay", TargetEntityKey: "system:beacon hub", RelationType: "CONNECTS_TO", Description: "routes to", Weight: 0.4},
				{DatasetID: graphSnapshot.DatasetID, UserID: graphSnapshot.UserID, OrgID: graphSnapshot.OrgID, SourceEntityKey: "system:aurora gateway", TargetEntityKey: "system:beacon hub", RelationType: "CONNECTS_TO", Description: "routes to strongly", Weight: 0.9},
			},
		}
		resolver := usecase.NewEmbeddingGraphEntityResolver(func(model.EmbeddingStrategy) (usecase.QueryEmbeddingProvider, error) {
			return queryEmbeddingProviderStub{
				dimensions: 2,
				vectors: [][]float32{
					{1.00, 0.00},
					{0.99, 0.01},
					{0.00, 1.00},
				},
			}, nil
		}, 0.98)

		resolved, err := resolver.ResolveGraphEntities(context.Background(), materialization, embeddingSnapshot)

		Expect(err).NotTo(HaveOccurred())
		Expect(resolved.Nodes).To(HaveLen(2))
		canonical := graphNodeByKey(resolved.Nodes, "system:aurora relay")
		Expect(canonical.MentionCount).To(Equal(4))
		Expect(graphNodeByKey(resolved.Nodes, "system:aurora gateway").EntityKey).To(BeEmpty())
		Expect(resolved.NodeAliases).To(HaveLen(3))
		gatewayAlias := graphNodeAliasBySourceKey(resolved.NodeAliases, "system:aurora gateway")
		Expect(gatewayAlias.CanonicalEntityKey).To(Equal("system:aurora relay"))
		Expect(gatewayAlias.Alias).To(Equal("Aurora Gateway"))
		Expect(resolved.NodeEmbeddings).To(HaveLen(2))
		Expect(graphNodeEmbeddingByKey(resolved.NodeEmbeddings, "system:aurora relay").Vector).To(HaveLen(2))
		gatewayChunk := graphNodeChunkByRecordID(resolved.NodeChunks, recordB)
		Expect(gatewayChunk.EntityKey).To(Equal("system:aurora relay"))
		Expect(resolved.Edges).To(HaveLen(1))
		Expect(resolved.Edges[0].SourceEntityKey).To(Equal("system:aurora relay"))
		Expect(resolved.Edges[0].TargetEntityKey).To(Equal("system:beacon hub"))
		Expect(resolved.Edges[0].Weight).To(Equal(0.9))
		Expect(resolved.Snapshot.EntityCount).To(Equal(int64(2)))
		Expect(resolved.Snapshot.EdgeCount).To(Equal(int64(1)))
	})

	It("resolves transitive duplicate clusters before choosing the canonical node", func() {
		graphSnapshot := validGraphSnapshot()
		embeddingSnapshot := validSearchEmbeddingSnapshot(graphSnapshot.DatasetID)
		embeddingSnapshot.EmbeddingSnapshotID = graphSnapshot.EmbeddingSnapshotID
		embeddingSnapshot.UserID = graphSnapshot.UserID
		embeddingSnapshot.OrgID = graphSnapshot.OrgID
		embeddingSnapshot.EmbeddingDimensions = 2
		materialization := &model.GraphMaterialization{
			Snapshot: graphSnapshot,
			Nodes: []model.GraphNode{
				{GraphSnapshotID: graphSnapshot.GraphSnapshotID, DatasetID: graphSnapshot.DatasetID, UserID: graphSnapshot.UserID, OrgID: graphSnapshot.OrgID, EntityKey: "system:alpha", Name: "Alpha", Type: "system", Description: "Left edge", MentionCount: 1},
				{GraphSnapshotID: graphSnapshot.GraphSnapshotID, DatasetID: graphSnapshot.DatasetID, UserID: graphSnapshot.UserID, OrgID: graphSnapshot.OrgID, EntityKey: "system:bridge", Name: "Bridge", Type: "system", Description: "Middle edge", MentionCount: 1},
				{GraphSnapshotID: graphSnapshot.GraphSnapshotID, DatasetID: graphSnapshot.DatasetID, UserID: graphSnapshot.UserID, OrgID: graphSnapshot.OrgID, EntityKey: "system:zeta", Name: "Zeta", Type: "system", Description: "Right edge", MentionCount: 3},
			},
		}
		resolver := usecase.NewEmbeddingGraphEntityResolver(func(model.EmbeddingStrategy) (usecase.QueryEmbeddingProvider, error) {
			return queryEmbeddingProviderStub{
				dimensions: 2,
				vectors: [][]float32{
					{1.00, 0.00},
					{0.707, 0.707},
					{0.00, 1.00},
				},
			}, nil
		}, 0.7)

		resolved, err := resolver.ResolveGraphEntities(context.Background(), materialization, embeddingSnapshot)

		Expect(err).NotTo(HaveOccurred())
		Expect(resolved.Nodes).To(HaveLen(1))
		Expect(resolved.Nodes[0].EntityKey).To(Equal("system:zeta"))
		Expect(resolved.Nodes[0].MentionCount).To(Equal(5))
		Expect(resolved.NodeAliases).To(HaveLen(3))
		Expect(graphNodeAliasBySourceKey(resolved.NodeAliases, "system:alpha").CanonicalEntityKey).To(Equal("system:zeta"))
		Expect(graphNodeAliasBySourceKey(resolved.NodeAliases, "system:bridge").CanonicalEntityKey).To(Equal("system:zeta"))
		Expect(resolved.NodeEmbeddings).To(HaveLen(1))
		Expect(resolved.Snapshot.EntityCount).To(Equal(int64(1)))
	})

	It("wraps provider failures with the graph entity resolution domain error", func() {
		graphSnapshot := validGraphSnapshot()
		embeddingSnapshot := validSearchEmbeddingSnapshot(graphSnapshot.DatasetID)
		embeddingSnapshot.EmbeddingSnapshotID = graphSnapshot.EmbeddingSnapshotID
		providerErr := errors.New("provider unavailable")
		resolver := usecase.NewEmbeddingGraphEntityResolver(func(model.EmbeddingStrategy) (usecase.QueryEmbeddingProvider, error) {
			return nil, providerErr
		}, 0.98)

		resolved, err := resolver.ResolveGraphEntities(context.Background(), &model.GraphMaterialization{
			Snapshot: graphSnapshot,
			Nodes: []model.GraphNode{{
				GraphSnapshotID: graphSnapshot.GraphSnapshotID,
				DatasetID:       graphSnapshot.DatasetID,
				UserID:          graphSnapshot.UserID,
				OrgID:           graphSnapshot.OrgID,
				EntityKey:       "system:aurora relay",
				Name:            "Aurora Relay",
				Type:            "system",
			}},
		}, embeddingSnapshot)

		Expect(resolved).To(BeNil())
		Expect(errors.Is(err, domain.ErrGraphEntityResolution)).To(BeTrue())
		Expect(errors.Is(err, providerErr)).To(BeTrue())
	})
})

func graphNodeByKey(nodes []model.GraphNode, entityKey string) model.GraphNode {
	for _, node := range nodes {
		if node.EntityKey == entityKey {
			return node
		}
	}
	return model.GraphNode{}
}

func graphNodeEmbeddingByKey(embeddings []model.GraphNodeEmbedding, entityKey string) model.GraphNodeEmbedding {
	for _, embedding := range embeddings {
		if embedding.EntityKey == entityKey {
			return embedding
		}
	}
	return model.GraphNodeEmbedding{}
}

func graphNodeAliasBySourceKey(aliases []model.GraphNodeAlias, sourceEntityKey string) model.GraphNodeAlias {
	for _, alias := range aliases {
		if alias.SourceEntityKey == sourceEntityKey {
			return alias
		}
	}
	return model.GraphNodeAlias{}
}

func graphNodeChunkByRecordID(chunks []model.GraphNodeChunk, embeddingRecordID uuid.UUID) model.GraphNodeChunk {
	for _, chunk := range chunks {
		if chunk.EmbeddingRecordID == embeddingRecordID {
			return chunk
		}
	}
	return model.GraphNodeChunk{}
}
