package grpc_test

import (
	"context"
	"testing"
	"time"

	"feature_materializer_service/pkg/domain/model"
	featuregrpc "feature_materializer_service/pkg/infra/network/grpc"
	featurepb "lib/data_contracts_lib/feature_materializer"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestFeatureMaterializerGrpc(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Feature materializer gRPC unit test suite")
}

type embeddingSearchUsecaseStub struct {
	userID    uuid.UUID
	datasetID uuid.UUID
	queryText string
	topK      int
	policy    model.RetrievalPolicy
	result    *model.EmbeddingSearchResult
	err       error
}

func (s *embeddingSearchUsecaseStub) SearchEmbeddings(_ context.Context, userID uuid.UUID, datasetID uuid.UUID, queryText string, topK int) (*model.EmbeddingSearchResult, error) {
	return s.SearchEmbeddingsWithPolicy(context.Background(), userID, datasetID, queryText, topK, model.RetrievalPolicy{})
}

func (s *embeddingSearchUsecaseStub) SearchEmbeddingsWithPolicy(_ context.Context, userID uuid.UUID, datasetID uuid.UUID, queryText string, topK int, policy model.RetrievalPolicy) (*model.EmbeddingSearchResult, error) {
	s.userID = userID
	s.datasetID = datasetID
	s.queryText = queryText
	s.topK = topK
	s.policy = policy
	return s.result, s.err
}

type graphSearchUsecaseStub struct {
	userID    uuid.UUID
	datasetID uuid.UUID
	queryText string
	topK      int
	maxHops   int
	mode      model.GraphSearchMode
	policy    model.RetrievalPolicy
	result    *model.GraphSearchResult
	err       error
}

func (s *graphSearchUsecaseStub) SearchGraph(_ context.Context, userID uuid.UUID, datasetID uuid.UUID, queryText string, topK int, maxHops int) (*model.GraphSearchResult, error) {
	return s.SearchGraphWithModeAndPolicy(context.Background(), userID, datasetID, queryText, topK, maxHops, model.GraphSearchModeLocal, model.RetrievalPolicy{})
}

func (s *graphSearchUsecaseStub) SearchGraphWithMode(_ context.Context, userID uuid.UUID, datasetID uuid.UUID, queryText string, topK int, maxHops int, mode model.GraphSearchMode) (*model.GraphSearchResult, error) {
	return s.SearchGraphWithModeAndPolicy(context.Background(), userID, datasetID, queryText, topK, maxHops, mode, model.RetrievalPolicy{})
}

func (s *graphSearchUsecaseStub) SearchGraphWithModeAndPolicy(_ context.Context, userID uuid.UUID, datasetID uuid.UUID, queryText string, topK int, maxHops int, mode model.GraphSearchMode, policy model.RetrievalPolicy) (*model.GraphSearchResult, error) {
	s.userID = userID
	s.datasetID = datasetID
	s.queryText = queryText
	s.topK = topK
	s.maxHops = maxHops
	s.mode = mode
	s.policy = policy
	if s.err != nil {
		return nil, s.err
	}
	if s.result != nil {
		return s.result, nil
	}
	return &model.GraphSearchResult{}, nil
}

var _ = Describe("FeatureMaterializerServer", func() {
	It("maps search requests and responses", func() {
		userID := uuid.New()
		orgID := uuid.New()
		datasetID := uuid.New()
		embeddingSnapshotID := uuid.New()
		featureSnapshotID := uuid.New()
		recordID := uuid.New()
		allowedID := uuid.New()
		deniedID := uuid.New()
		asOf := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
		uc := &embeddingSearchUsecaseStub{
			result: &model.EmbeddingSearchResult{
				EmbeddingSnapshot: &model.EmbeddingSnapshot{
					EmbeddingSnapshotID: embeddingSnapshotID,
					FeatureSnapshotID:   featureSnapshotID,
					UserID:              userID,
					OrgID:               orgID,
					DatasetID:           datasetID,
					VectorStore:         "pgvector",
					CollectionName:      "movies",
					EmbeddingDimensions: 384,
					StrategyVersion:     "rag-v1",
					ChunkerName:         "go-token-window",
					ChunkerVersion:      "v1",
					ChunkSize:           384,
					ChunkOverlap:        64,
					EmbeddingProvider:   "ollama",
					EmbeddingModel:      model.DefaultEmbeddingModel,
				},
				Matches: []model.EmbeddingRecord{{
					EmbeddingRecordID:   recordID,
					EmbeddingSnapshotID: embeddingSnapshotID,
					DatasetID:           datasetID,
					OrgID:               orgID,
					ChunkIndex:          3,
					SourceText:          "result chunk",
					Distance:            0.2,
					Similarity:          0.8,
					AssertionStatus:     model.AssertionStatusAdmitted,
				}},
				Disclosure: model.RetrievalDisclosure{
					Mode:            model.RetrievalModeExactAuthorized,
					PolicyID:        "policy-123",
					PolicyHash:      "sha256:abc",
					PrincipalID:     userID,
					Purpose:         "answer",
					AsOf:            asOf,
					ScanBudget:      9,
					CandidateCount:  1,
					AuthorizedCount: 1,
					ResultCount:     1,
					Underfilled:     true,
				},
			},
		}
		server := featuregrpc.NewFeatureMaterializerGrpcServer(uc, &graphSearchUsecaseStub{})

		response, err := server.SearchEmbeddings(context.Background(), &featurepb.SearchEmbeddingsRequest{
			DatasetId: datasetID.String(),
			UserId:    userID.String(),
			OrgId:     orgID.String(),
			QueryText: " query ",
			TopK:      9,
			RetrievalPolicy: &featurepb.RetrievalPolicy{
				Mode:               model.RetrievalModeExactAuthorized.String(),
				PolicyId:           "policy-123",
				PolicyHash:         "sha256:abc",
				PrincipalId:        userID.String(),
				Purpose:            "answer",
				Context:            map[string]string{"classification": "internal"},
				AsOf:               asOf.Format(time.RFC3339Nano),
				AllowedResourceIds: []string{allowedID.String()},
				DeniedResourceIds:  []string{deniedID.String()},
				ScanBudget:         9,
			},
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(uc.userID).To(Equal(userID))
		Expect(uc.datasetID).To(Equal(datasetID))
		Expect(uc.queryText).To(Equal("query"))
		Expect(uc.topK).To(Equal(9))
		Expect(uc.policy.Mode).To(Equal(model.RetrievalModeExactAuthorized))
		Expect(uc.policy.PolicyID).To(Equal("policy-123"))
		Expect(uc.policy.PolicyHash).To(Equal("sha256:abc"))
		Expect(uc.policy.PrincipalID).To(Equal(userID))
		Expect(uc.policy.Purpose).To(Equal("answer"))
		Expect(uc.policy.Context).To(HaveKeyWithValue("classification", "internal"))
		Expect(uc.policy.AsOf).To(Equal(asOf))
		Expect(uc.policy.AllowedResourceIDs).To(Equal([]uuid.UUID{allowedID}))
		Expect(uc.policy.DeniedResourceIDs).To(Equal([]uuid.UUID{deniedID}))
		Expect(uc.policy.ScanBudget).To(Equal(9))
		Expect(response.GetEmbeddingSnapshotId()).To(Equal(embeddingSnapshotID.String()))
		Expect(response.GetOrgId()).To(Equal(orgID.String()))
		Expect(response.GetFeatureSnapshotId()).To(Equal(featureSnapshotID.String()))
		Expect(response.GetEmbeddingDimensions()).To(Equal(int32(384)))
		Expect(response.GetMatches()).To(HaveLen(1))
		Expect(response.GetMatches()[0].GetEmbeddingRecordId()).To(Equal(recordID.String()))
		Expect(response.GetMatches()[0].GetOrgId()).To(Equal(orgID.String()))
		Expect(response.GetMatches()[0].GetDistance()).To(Equal(0.2))
		Expect(response.GetMatches()[0].GetSimilarity()).To(Equal(0.8))
		Expect(response.GetMatches()[0].GetAssertionStatus()).To(Equal(model.AssertionStatusAdmitted.String()))
		Expect(response.GetRetrievalDisclosure().GetMode()).To(Equal(model.RetrievalModeExactAuthorized.String()))
		Expect(response.GetRetrievalDisclosure().GetPolicyId()).To(Equal("policy-123"))
		Expect(response.GetRetrievalDisclosure().GetAsOf()).To(Equal(asOf.Format(time.RFC3339Nano)))
		Expect(response.GetRetrievalDisclosure().GetUnderfilled()).To(BeTrue())
	})

	It("rejects invalid requests", func() {
		server := featuregrpc.NewFeatureMaterializerGrpcServer(&embeddingSearchUsecaseStub{}, &graphSearchUsecaseStub{})

		_, err := server.SearchEmbeddings(context.Background(), &featurepb.SearchEmbeddingsRequest{
			UserId:    uuid.NewString(),
			OrgId:     uuid.NewString(),
			DatasetId: "not-a-uuid",
			QueryText: "query",
		})

		Expect(status.Code(err)).To(Equal(codes.InvalidArgument))
	})

	It("maps global graph search requests and community reports", func() {
		userID := uuid.New()
		orgID := uuid.New()
		datasetID := uuid.New()
		graphSnapshotID := uuid.New()
		featureSnapshotID := uuid.New()
		embeddingSnapshotID := uuid.New()
		reportID := uuid.New()
		communityID := uuid.New()
		uc := &graphSearchUsecaseStub{
			result: &model.GraphSearchResult{
				GraphSnapshot: &model.GraphSnapshot{
					GraphSnapshotID:     graphSnapshotID,
					FeatureSnapshotID:   featureSnapshotID,
					EmbeddingSnapshotID: embeddingSnapshotID,
					DatasetID:           datasetID,
					OrgID:               orgID,
					ProvenanceHash:      "graph-hash",
				},
				Mode: model.GraphSearchModeGlobal,
				CommunityReports: []model.GraphCommunityReportMatch{{
					GraphCommunityReportID: reportID,
					GraphCommunityID:       communityID,
					GraphSnapshotID:        graphSnapshotID,
					DatasetID:              datasetID,
					OrgID:                  orgID,
					CommunityKey:           "community:001:system:aurora relay",
					Level:                  0,
					Title:                  "Aurora Relay / Beacon Hub",
					Summary:                "Routing community",
					ReportText:             "Aurora Relay routes to Beacon Hub.",
					Rank:                   4,
					Score:                  0.91,
				}},
			},
		}
		server := featuregrpc.NewFeatureMaterializerGrpcServer(&embeddingSearchUsecaseStub{}, uc)

		response, err := server.SearchGraph(context.Background(), &featurepb.SearchGraphRequest{
			DatasetId: datasetID.String(),
			UserId:    userID.String(),
			OrgId:     orgID.String(),
			QueryText: " routing ",
			TopK:      3,
			MaxHops:   2,
			Mode:      "global",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(uc.userID).To(Equal(userID))
		Expect(uc.datasetID).To(Equal(datasetID))
		Expect(uc.queryText).To(Equal("routing"))
		Expect(uc.topK).To(Equal(3))
		Expect(uc.maxHops).To(Equal(2))
		Expect(uc.mode).To(Equal(model.GraphSearchModeGlobal))
		Expect(response.GetGraphSnapshotId()).To(Equal(graphSnapshotID.String()))
		Expect(response.GetMode()).To(Equal("global"))
		Expect(response.GetCommunityReports()).To(HaveLen(1))
		Expect(response.GetCommunityReports()[0].GetGraphCommunityReportId()).To(Equal(reportID.String()))
		Expect(response.GetCommunityReports()[0].GetTitle()).To(Equal("Aurora Relay / Beacon Hub"))
		Expect(response.GetCommunityReports()[0].GetScore()).To(Equal(0.91))
	})

	It("requires top-k at the boundary", func() {
		server := featuregrpc.NewFeatureMaterializerGrpcServer(&embeddingSearchUsecaseStub{}, &graphSearchUsecaseStub{})

		_, err := server.SearchEmbeddings(context.Background(), &featurepb.SearchEmbeddingsRequest{
			UserId:    uuid.NewString(),
			OrgId:     uuid.NewString(),
			DatasetId: uuid.NewString(),
			QueryText: "query",
		})

		Expect(status.Code(err)).To(Equal(codes.InvalidArgument))
	})

	It("treats a stopped gRPC server as a clean shutdown", func() {
		server := featuregrpc.NewFeatureMaterializerGrpcServer(&embeddingSearchUsecaseStub{}, &graphSearchUsecaseStub{})
		lis := bufconn.Listen(1024)
		result := make(chan error, 1)

		go func() {
			result <- server.Serve(lis)
		}()
		time.Sleep(10 * time.Millisecond)
		server.Close()

		Eventually(result).Should(Receive(BeNil()))
	})
})
