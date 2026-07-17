package grpc

import (
	"context"
	"testing"
	"time"

	usecase "inference_service/pkg/app"
	"inference_service/pkg/domain/model"
	featurepb "lib/data_contracts_lib/feature_materializer"
	inferencepb "lib/data_contracts_lib/inference"
	"lib/shared_lib/ctxutil"

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
	request        model.GenerateRequest
	feedback       *model.InferenceFeedback
	feedbackKey    uuid.UUID
	result         *model.GenerateResponse
	feedbackResult *model.InferenceFeedback
	err            error
}

func (s *inferenceUsecaseStub) RecordModelUpdated(context.Context, *model.InferenceModel, uuid.UUID) (*model.InferenceModel, error) {
	return nil, nil
}

func (s *inferenceUsecaseStub) RecordDatasetUpdated(context.Context, *model.InferenceDataset, uuid.UUID) (*model.InferenceDataset, error) {
	return nil, nil
}

func (s *inferenceUsecaseStub) ReadModel(context.Context, uuid.UUID, uuid.UUID) (*model.InferenceModel, error) {
	return nil, nil
}

func (s *inferenceUsecaseStub) PublishAgentSpec(context.Context, model.AgentSpecPublication) (*model.AgentSpec, error) {
	return nil, nil
}

func (s *inferenceUsecaseStub) ListEndpoints(context.Context, uuid.UUID) ([]*model.PublishedEndpoint, error) {
	return nil, nil
}

func (s *inferenceUsecaseStub) PublishEndpoint(context.Context, model.EndpointPublication) (*model.PublishedEndpoint, error) {
	return nil, nil
}

func (s *inferenceUsecaseStub) SetEndpointDatasets(context.Context, model.EndpointDatasetBinding) (*model.PublishedEndpoint, error) {
	return nil, nil
}

func (s *inferenceUsecaseStub) SetEndpointMergeStrategy(context.Context, model.EndpointMergeConfiguration) (*model.PublishedEndpoint, error) {
	return nil, nil
}

func (s *inferenceUsecaseStub) GenerateForEndpoint(_ context.Context, _ uuid.UUID, request model.GenerateRequest) (*model.GenerateResponse, error) {
	s.request = request
	return s.result, s.err
}

func (s *inferenceUsecaseStub) Generate(_ context.Context, request model.GenerateRequest) (*model.GenerateResponse, error) {
	s.request = request
	return s.result, s.err
}

func (s *inferenceUsecaseStub) PrepareAgentRunActivity(context.Context, usecase.PrepareAgentRunActivityInput) (usecase.AgentRunWorkflowState, error) {
	return usecase.AgentRunWorkflowState{}, nil
}

func (s *inferenceUsecaseStub) GenerateAgentStepActivity(context.Context, usecase.GenerateAgentStepActivityInput) (usecase.GenerateAgentStepActivityOutput, error) {
	return usecase.GenerateAgentStepActivityOutput{}, nil
}

func (s *inferenceUsecaseStub) RecordAgentStepActivity(context.Context, usecase.RecordAgentStepActivityInput) (uuid.UUID, error) {
	return uuid.Nil, nil
}

func (s *inferenceUsecaseStub) InvokeAgentToolActivity(context.Context, usecase.InvokeAgentToolActivityInput) (usecase.InvokeAgentToolActivityOutput, error) {
	return usecase.InvokeAgentToolActivityOutput{}, nil
}

func (s *inferenceUsecaseStub) CompleteAgentRunActivity(context.Context, usecase.CompleteAgentRunActivityInput) error {
	return nil
}

func (s *inferenceUsecaseStub) FailAgentRunActivity(context.Context, usecase.FailAgentRunActivityInput) error {
	return nil
}

func (s *inferenceUsecaseStub) RecordFeedback(_ context.Context, feedback *model.InferenceFeedback, idempotencyKey uuid.UUID) (*model.InferenceFeedback, error) {
	s.feedback = feedback
	s.feedbackKey = idempotencyKey
	if s.feedbackResult != nil {
		return s.feedbackResult, s.err
	}
	return feedback, s.err
}

func (s *inferenceUsecaseStub) BuildPreferenceDatasetForEndpoint(context.Context, uuid.UUID, model.PreferenceDatasetBuildRequest) (*model.PreferenceDataset, error) {
	return nil, nil
}

func (s *inferenceUsecaseStub) ReadPreferenceDataset(context.Context, uuid.UUID, uuid.UUID) (*model.PreferenceDataset, error) {
	return nil, nil
}

func (s *inferenceUsecaseStub) ListPreferenceDatasets(context.Context, uuid.UUID, model.PreferenceDatasetFilter) ([]*model.PreferenceDataset, error) {
	return nil, nil
}

func (s *inferenceUsecaseStub) BuildPreferenceDataset(context.Context, model.PreferenceDatasetBuildRequest) (*model.PreferenceDataset, error) {
	return nil, nil
}

func (s *inferenceUsecaseStub) ReadAgentTrajectory(context.Context, uuid.UUID, uuid.UUID) (*model.AgentTrajectory, error) {
	return nil, nil
}

func (s *inferenceUsecaseStub) ReapExpiredAgentRuns(context.Context, time.Duration) (int64, error) {
	return 0, nil
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

func (s *featureMaterializerServiceClientStub) SearchGraph(context.Context, *featurepb.SearchGraphRequest, ...stdgrpc.CallOption) (*featurepb.SearchGraphResponse, error) {
	return &featurepb.SearchGraphResponse{}, nil
}

var _ = Describe("InferenceServer", func() {
	It("maps generate requests and responses", func() {
		requestID := uuid.New()
		userID := uuid.New()
		orgID := uuid.New()
		datasetID := uuid.New()
		modelID := uuid.New()
		recordID := uuid.New()
		snapshotID := uuid.New()
		uc := &inferenceUsecaseStub{
			result: &model.GenerateResponse{
				RequestID:             requestID,
				OrgID:                 orgID,
				DatasetID:             datasetID,
				ModelID:               modelID,
				QueryText:             "what is relevant?",
				Answer:                "generated answer",
				PromptStrategyVersion: "rag-prompt-v1",
				GenerationProtocol:    "OPENAI_CHAT_COMPLETIONS",
				GenerationModel:       "local-test-model:latest",
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
			UserId:          userID.String(),
			OrgId:           orgID.String(),
			DatasetId:       datasetID.String(),
			ModelId:         modelID.String(),
			QueryText:       "what is relevant?",
			TopK:            4,
			MetadataFilters: map[string]string{"source": "manual"},
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(uc.request.RequestID).To(Equal(requestID))
		Expect(uc.request.UserID).To(Equal(userID))
		Expect(uc.request.OrgID).To(Equal(orgID))
		Expect(uc.request.DatasetID).To(Equal(datasetID))
		Expect(response.GetOrgId()).To(Equal(orgID.String()))
		Expect(uc.request.ModelID).To(Equal(modelID))
		Expect(uc.request.TopK).To(Equal(4))
		Expect(uc.request.MetadataFilters).To(Equal(map[string]string{"source": "manual"}))
		Expect(response.GetAnswer()).To(Equal("generated answer"))
		Expect(response.GetRequestId()).To(Equal(requestID.String()))
		Expect(response.GetPromptStrategyVersion()).To(Equal("rag-prompt-v1"))
		Expect(response.GetGenerationProtocol()).To(Equal("OPENAI_CHAT_COMPLETIONS"))
		Expect(response.GetGenerationModel()).To(Equal("local-test-model:latest"))
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
		userID := uuid.New()
		orgID := uuid.New()
		modelID := uuid.New()

		_, err := server.Generate(context.Background(), &inferencepb.GenerateRequest{
			UserId:    userID.String(),
			DatasetId: datasetID.String(),
			ModelId:   modelID.String(),
			QueryText: "query",
			TopK:      4,
		})
		Expect(status.Code(err)).To(Equal(codes.InvalidArgument))

		_, err = server.Generate(context.Background(), &inferencepb.GenerateRequest{
			RequestId: uuid.NewString(),
			UserId:    userID.String(),
			OrgId:     orgID.String(),
			DatasetId: datasetID.String(),
			ModelId:   modelID.String(),
			QueryText: "query",
		})
		Expect(status.Code(err)).To(Equal(codes.InvalidArgument))
	})

	It("maps feedback requests into the usecase", func() {
		feedbackID := uuid.New()
		requestID := uuid.New()
		userID := uuid.New()
		orgID := uuid.New()
		uc := &inferenceUsecaseStub{}
		server := NewInferenceGrpcServer(uc)

		response, err := server.RecordFeedback(context.Background(), &inferencepb.RecordFeedbackRequest{
			FeedbackId:      feedbackID.String(),
			RequestId:       requestID.String(),
			UserId:          userID.String(),
			OrgId:           orgID.String(),
			Accepted:        false,
			Rating:          -1,
			Comment:         " not grounded ",
			PreferredAnswer: "grounded answer",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(response.GetFeedbackId()).To(Equal(feedbackID.String()))
		Expect(response.GetRequestId()).To(Equal(requestID.String()))
		Expect(uc.feedback.FeedbackID).To(Equal(feedbackID))
		Expect(uc.feedback.RequestID).To(Equal(requestID))
		Expect(uc.feedback.UserID).To(Equal(userID))
		Expect(uc.feedback.OrgID).To(Equal(orgID))
		Expect(uc.feedback.Accepted).To(BeFalse())
		Expect(uc.feedback.Rating).To(Equal(-1))
		Expect(uc.feedback.Comment).To(Equal("not grounded"))
		Expect(uc.feedback.PreferredAnswer).To(Equal("grounded answer"))
		Expect(uc.feedbackKey).To(Equal(feedbackID))
	})

	It("rejects invalid feedback requests", func() {
		server := NewInferenceGrpcServer(&inferenceUsecaseStub{})

		_, err := server.RecordFeedback(context.Background(), &inferencepb.RecordFeedbackRequest{
			FeedbackId: uuid.NewString(),
			RequestId:  uuid.NewString(),
			UserId:     uuid.NewString(),
			OrgId:      uuid.NewString(),
			Rating:     2,
		})

		Expect(status.Code(err)).To(Equal(codes.InvalidArgument))
	})
})

var _ = Describe("FeatureMaterializerClient", func() {
	It("accepts complete gRPC client configuration", func() {
		err := ValidateFeatureMaterializerClientConfig(FeatureMaterializerClientConfig{
			Address:       "feature-materializer:7072",
			DialTimeoutMs: 500,
			CallTimeoutMs: 15000,
			RetryCount:    3,
		})

		Expect(err).NotTo(HaveOccurred())
	})

	DescribeTable("rejects incomplete gRPC client configuration",
		func(config FeatureMaterializerClientConfig, expected string) {
			err := ValidateFeatureMaterializerClientConfig(config)

			Expect(err).To(MatchError(ContainSubstring(expected)))
		},
		Entry("missing address", FeatureMaterializerClientConfig{DialTimeoutMs: 1, CallTimeoutMs: 1, RetryCount: 1}, "address"),
		Entry("missing dial timeout", FeatureMaterializerClientConfig{Address: "feature-materializer:7072", CallTimeoutMs: 1, RetryCount: 1}, "dial timeout"),
		Entry("missing call timeout", FeatureMaterializerClientConfig{Address: "feature-materializer:7072", DialTimeoutMs: 1, RetryCount: 1}, "call timeout"),
		Entry("missing retry count", FeatureMaterializerClientConfig{Address: "feature-materializer:7072", DialTimeoutMs: 1, CallTimeoutMs: 1}, "retry count"),
	)

	It("maps search responses into retrieved contexts", func() {
		datasetID := uuid.New()
		userID := uuid.New()
		orgID := uuid.New()
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

		contexts, err := client.SearchEmbeddings(ctxutil.WithOrgID(context.Background(), orgID), userID, datasetID, "query", 6, map[string]string{"source": "manual"})

		Expect(err).NotTo(HaveOccurred())
		Expect(clientStub.request.GetUserId()).To(Equal(userID.String()))
		Expect(clientStub.request.GetOrgId()).To(Equal(orgID.String()))
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

		_, err := client.SearchEmbeddings(ctxutil.WithOrgID(context.Background(), uuid.New()), uuid.New(), uuid.New(), "query", 5, nil)

		Expect(err).To(HaveOccurred())
	})
})

var _ usecase.RetrievalClient = (*featureMaterializerClient)(nil)
