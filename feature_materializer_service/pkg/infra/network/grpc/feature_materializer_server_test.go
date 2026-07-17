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
	result    *model.EmbeddingSearchResult
	err       error
}

func (s *embeddingSearchUsecaseStub) SearchEmbeddings(_ context.Context, userID uuid.UUID, datasetID uuid.UUID, queryText string, topK int) (*model.EmbeddingSearchResult, error) {
	s.userID = userID
	s.datasetID = datasetID
	s.queryText = queryText
	s.topK = topK
	return s.result, s.err
}

type graphSearchUsecaseStub struct {
	userID    uuid.UUID
	datasetID uuid.UUID
	queryText string
	topK      int
	maxHops   int
	result    *model.GraphSearchResult
	err       error
}

func (s *graphSearchUsecaseStub) SearchGraph(_ context.Context, userID uuid.UUID, datasetID uuid.UUID, queryText string, topK int, maxHops int) (*model.GraphSearchResult, error) {
	s.userID = userID
	s.datasetID = datasetID
	s.queryText = queryText
	s.topK = topK
	s.maxHops = maxHops
	return s.result, s.err
}

var _ = Describe("FeatureMaterializerServer", func() {
	It("maps search requests and responses", func() {
		userID := uuid.New()
		orgID := uuid.New()
		datasetID := uuid.New()
		embeddingSnapshotID := uuid.New()
		featureSnapshotID := uuid.New()
		recordID := uuid.New()
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
				}},
			},
		}
		server := featuregrpc.NewFeatureMaterializerGrpcServer(uc, &graphSearchUsecaseStub{})

		response, err := server.SearchEmbeddings(context.Background(), &featurepb.SearchEmbeddingsRequest{
			DatasetId: datasetID.String(),
			UserId:    userID.String(),
			OrgId:     orgID.String(),
			QueryText: " query ",
			TopK:      9,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(uc.userID).To(Equal(userID))
		Expect(uc.datasetID).To(Equal(datasetID))
		Expect(uc.queryText).To(Equal("query"))
		Expect(uc.topK).To(Equal(9))
		Expect(response.GetEmbeddingSnapshotId()).To(Equal(embeddingSnapshotID.String()))
		Expect(response.GetOrgId()).To(Equal(orgID.String()))
		Expect(response.GetFeatureSnapshotId()).To(Equal(featureSnapshotID.String()))
		Expect(response.GetEmbeddingDimensions()).To(Equal(int32(384)))
		Expect(response.GetMatches()).To(HaveLen(1))
		Expect(response.GetMatches()[0].GetEmbeddingRecordId()).To(Equal(recordID.String()))
		Expect(response.GetMatches()[0].GetOrgId()).To(Equal(orgID.String()))
		Expect(response.GetMatches()[0].GetDistance()).To(Equal(0.2))
		Expect(response.GetMatches()[0].GetSimilarity()).To(Equal(0.8))
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
