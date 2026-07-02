package grpc

import (
	"context"
	"testing"

	usecase "inference_service/pkg/app"
	"inference_service/pkg/domain/model"
	featurepb "lib/data_contracts_lib/feature_materializer"
	inferencepb "lib/data_contracts_lib/inference"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	stdgrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestInferenceGrpc(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Inference gRPC unit test suite")
}

type inferenceUsecaseStub struct {
	request model.GenerateRequest
	result  *model.GenerateResponse
	err     error
}

func (s *inferenceUsecaseStub) RecordModelUpdated(context.Context, *model.InferenceModel, uuid.UUID) (*model.InferenceModel, error) {
	return nil, nil
}

func (s *inferenceUsecaseStub) RecordDatasetUpdated(context.Context, *model.InferenceDataset, uuid.UUID) (*model.InferenceDataset, error) {
	return nil, nil
}

func (s *inferenceUsecaseStub) ReadModel(context.Context, uuid.UUID) (*model.InferenceModel, error) {
	return nil, nil
}

func (s *inferenceUsecaseStub) Generate(_ context.Context, request model.GenerateRequest) (*model.GenerateResponse, error) {
	s.request = request
	return s.result, s.err
}

type featureMaterializerServiceClientStub struct {
	request *featurepb.SearchEmbeddingsRequest
	resp    *featurepb.SearchEmbeddingsResponse
	err     error
}

func (s *featureMaterializerServiceClientStub) SearchEmbeddings(_ context.Context, request *featurepb.SearchEmbeddingsRequest, _ ...stdgrpc.CallOption) (*featurepb.SearchEmbeddingsResponse, error) {
	s.request = request
	return s.resp, s.err
}

var _ = Describe("InferenceServer", func() {
	It("maps generate requests and responses", func() {
		requestID := uuid.New()
		datasetID := uuid.New()
		modelID := uuid.New()
		recordID := uuid.New()
		snapshotID := uuid.New()
		uc := &inferenceUsecaseStub{
			result: &model.GenerateResponse{
				RequestID:             requestID,
				DatasetID:             datasetID,
				ModelID:               modelID,
				QueryText:             "what is relevant?",
				Answer:                "generated answer",
				PromptStrategyVersion: "rag-prompt-v1",
				GenerationProvider:    "ollama",
				GenerationModel:       "llama3.1:8b",
				Contexts: []model.RetrievedContext{{
					EmbeddingRecordID:   recordID,
					EmbeddingSnapshotID: snapshotID,
					ChunkIndex:          7,
					SourceText:          "context",
					Distance:            0.2,
					Similarity:          0.8,
				}},
			},
		}
		server := NewInferenceGrpcServer(uc)

		response, err := server.Generate(context.Background(), &inferencepb.GenerateRequest{
			RequestId:       requestID.String(),
			DatasetId:       datasetID.String(),
			ModelId:         modelID.String(),
			QueryText:       "what is relevant?",
			TopK:            4,
			MetadataFilters: map[string]string{"source": "manual"},
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(uc.request.RequestID).To(Equal(requestID))
		Expect(uc.request.DatasetID).To(Equal(datasetID))
		Expect(uc.request.ModelID).To(Equal(modelID))
		Expect(uc.request.TopK).To(Equal(4))
		Expect(uc.request.MetadataFilters).To(Equal(map[string]string{"source": "manual"}))
		Expect(response.GetAnswer()).To(Equal("generated answer"))
		Expect(response.GetRequestId()).To(Equal(requestID.String()))
		Expect(response.GetPromptStrategyVersion()).To(Equal("rag-prompt-v1"))
		Expect(response.GetGenerationProvider()).To(Equal("ollama"))
		Expect(response.GetGenerationModel()).To(Equal("llama3.1:8b"))
		Expect(response.GetContexts()).To(HaveLen(1))
		Expect(response.GetContexts()[0].GetEmbeddingRecordId()).To(Equal(recordID.String()))
	})

	It("rejects invalid generate requests", func() {
		server := NewInferenceGrpcServer(&inferenceUsecaseStub{})

		_, err := server.Generate(context.Background(), &inferencepb.GenerateRequest{
			DatasetId: "not-a-uuid",
			QueryText: "query",
		})

		Expect(status.Code(err)).To(Equal(codes.InvalidArgument))
	})

	It("requires request identity and top-k at the boundary", func() {
		server := NewInferenceGrpcServer(&inferenceUsecaseStub{})
		datasetID := uuid.New()
		modelID := uuid.New()

		_, err := server.Generate(context.Background(), &inferencepb.GenerateRequest{
			DatasetId: datasetID.String(),
			ModelId:   modelID.String(),
			QueryText: "query",
			TopK:      4,
		})
		Expect(status.Code(err)).To(Equal(codes.InvalidArgument))

		_, err = server.Generate(context.Background(), &inferencepb.GenerateRequest{
			RequestId: uuid.NewString(),
			DatasetId: datasetID.String(),
			ModelId:   modelID.String(),
			QueryText: "query",
		})
		Expect(status.Code(err)).To(Equal(codes.InvalidArgument))
	})
})

var _ = Describe("FeatureMaterializerClient", func() {
	It("maps search responses into retrieved contexts", func() {
		datasetID := uuid.New()
		recordID := uuid.New()
		snapshotID := uuid.New()
		clientStub := &featureMaterializerServiceClientStub{
			resp: &featurepb.SearchEmbeddingsResponse{
				Matches: []*featurepb.EmbeddingSearchMatch{{
					EmbeddingRecordId:   recordID.String(),
					EmbeddingSnapshotId: snapshotID.String(),
					DatasetId:           datasetID.String(),
					ChunkIndex:          3,
					SourceText:          "context",
					Distance:            0.1,
					Similarity:          0.9,
				}},
			},
		}
		client := &featureMaterializerClient{client: clientStub}

		contexts, err := client.SearchEmbeddings(context.Background(), datasetID, "query", 6, map[string]string{"source": "manual"})

		Expect(err).NotTo(HaveOccurred())
		Expect(clientStub.request.GetDatasetId()).To(Equal(datasetID.String()))
		Expect(clientStub.request.GetQueryText()).To(Equal("query"))
		Expect(clientStub.request.GetTopK()).To(Equal(int32(6)))
		Expect(clientStub.request.GetMetadataFilters()).To(Equal(map[string]string{"source": "manual"}))
		Expect(contexts).To(HaveLen(1))
		Expect(contexts[0].EmbeddingRecordID).To(Equal(recordID))
		Expect(contexts[0].EmbeddingSnapshotID).To(Equal(snapshotID))
		Expect(contexts[0].Similarity).To(Equal(0.9))
	})

	It("surfaces malformed materializer responses as errors", func() {
		client := &featureMaterializerClient{client: &featureMaterializerServiceClientStub{
			resp: &featurepb.SearchEmbeddingsResponse{
				Matches: []*featurepb.EmbeddingSearchMatch{{
					EmbeddingRecordId:   "not-a-uuid",
					EmbeddingSnapshotId: uuid.NewString(),
				}},
			},
		}}

		_, err := client.SearchEmbeddings(context.Background(), uuid.New(), "query", 5, nil)

		Expect(err).To(HaveOccurred())
	})
})

var _ usecase.RetrievalClient = (*featureMaterializerClient)(nil)
