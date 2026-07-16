package tools

import (
	"context"
	"testing"

	"inference_service/pkg/app"
	"inference_service/pkg/domain"
	"inference_service/pkg/domain/model"
	toolspb "lib/data_contracts_lib/tools"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
)

func TestTools(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Inference tools unit test suite")
}

type retrievalClientStub struct {
	userID          uuid.UUID
	datasetID       uuid.UUID
	queryText       string
	topK            int
	metadataFilters map[string]string
	contexts        []model.RetrievedContext
	err             error
}

func (s *retrievalClientStub) SearchEmbeddings(_ context.Context, userID uuid.UUID, datasetID uuid.UUID, queryText string, topK int, metadataFilters map[string]string) ([]model.RetrievedContext, error) {
	s.userID = userID
	s.datasetID = datasetID
	s.queryText = queryText
	s.topK = topK
	s.metadataFilters = metadataFilters
	return s.contexts, s.err
}

func (s *retrievalClientStub) Close() error {
	return nil
}

var _ = Describe("SearchKnowledgeToolInvoker", func() {
	It("exposes search_knowledge only for explicit bindings", func() {
		invoker, err := NewSearchKnowledgeToolInvoker(&retrievalClientStub{})
		Expect(err).NotTo(HaveOccurred())

		specs, err := invoker.Available(context.Background(), app.ToolResolutionContext{OrgID: uuid.New(), UserID: uuid.New()}, []model.ToolBinding{{Name: "search_knowledge"}})

		Expect(err).NotTo(HaveOccurred())
		Expect(specs).To(HaveLen(1))
		Expect(specs[0].Name).To(Equal("search_knowledge"))
		Expect(specs[0].Parameters).NotTo(BeEmpty())
	})

	It("rejects unknown tool bindings", func() {
		invoker, err := NewSearchKnowledgeToolInvoker(&retrievalClientStub{})
		Expect(err).NotTo(HaveOccurred())

		_, err = invoker.Available(context.Background(), app.ToolResolutionContext{OrgID: uuid.New(), UserID: uuid.New()}, []model.ToolBinding{{Name: "http_get"}})

		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("unknown agent tool binding"))
	})

	It("validates tool-call arguments before retrieval", func() {
		invoker, err := NewSearchKnowledgeToolInvoker(&retrievalClientStub{})
		Expect(err).NotTo(HaveOccurred())
		invocation := app.ToolInvocationContext{UserID: uuid.New(), Datasets: []*model.InferenceDataset{{DatasetID: uuid.New()}}}

		result, err := invoker.Invoke(context.Background(), invocation, model.ToolCall{Name: "search_knowledge", Arguments: []byte(`{"top_k":1}`)})

		Expect(err).To(HaveOccurred())
		Expect(result.IsError).To(BeTrue())
		Expect(result.ErrorType).To(Equal(model.ToolErrorTypePolicyDenied))
	})

	It("searches tenant datasets with validated arguments", func() {
		datasetID := uuid.New()
		userID := uuid.New()
		retrieval := &retrievalClientStub{contexts: []model.RetrievedContext{{
			EmbeddingRecordID: uuid.New(),
			DatasetID:         datasetID,
			SourceText:        "grounded context",
		}}}
		invoker, err := NewSearchKnowledgeToolInvoker(retrieval)
		Expect(err).NotTo(HaveOccurred())
		invocation := app.ToolInvocationContext{UserID: userID, Datasets: []*model.InferenceDataset{{DatasetID: datasetID}}}

		result, err := invoker.Invoke(context.Background(), invocation, model.ToolCall{
			ID:        "call-1",
			Name:      "search_knowledge",
			Arguments: []byte(`{"query_text":"contract terms","top_k":4,"metadata_filters":{"section":"legal"}}`),
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(result.IsError).To(BeFalse())
		Expect(result.ToolImplVersion).To(Equal(searchKnowledgeToolImplVersion))
		Expect(result.Content).To(ContainSubstring("grounded context"))
		Expect(result.Contexts).To(HaveLen(1))
		Expect(result.Contexts[0].SourceText).To(Equal("grounded context"))
		Expect(retrieval.userID).To(Equal(userID))
		Expect(retrieval.datasetID).To(Equal(datasetID))
		Expect(retrieval.queryText).To(Equal("contract terms"))
		Expect(retrieval.topK).To(Equal(4))
		Expect(retrieval.metadataFilters).To(HaveKeyWithValue("section", "legal"))
	})

	It("rejects a nil retrieval dependency at construction", func() {
		invoker, err := NewSearchKnowledgeToolInvoker(nil)

		Expect(invoker).To(BeNil())
		Expect(err).To(MatchError(ContainSubstring(domain.ErrValidationFailed.Error())))
	})
})

var _ = Describe("RoutedToolInvoker", func() {
	It("routes search_knowledge locally and http_get through the remote tool service", func() {
		searchInvoker, err := NewSearchKnowledgeToolInvoker(&retrievalClientStub{})
		Expect(err).NotTo(HaveOccurred())
		remoteClient := &toolServiceClientStub{
			listResponse: &toolspb.ListAvailableToolsResponse{Tools: []*toolspb.ToolDefinition{{
				Name:                  "http_get",
				Description:           "Fetch HTTP content.",
				ParametersJson:        []byte(`{"type":"object"}`),
				ImplementationVersion: "http_get:test",
			}}},
			invokeResponse: &toolspb.InvokeToolResponse{
				ResultJson:            []byte(`{"status":200,"body":"ok"}`),
				ImplementationVersion: "http_get:test",
			},
		}
		remoteInvoker, err := NewRemoteToolInvokerWithClient(remoteClient, newToolServiceDTOAdapter(validator.New()))
		Expect(err).NotTo(HaveOccurred())
		routed, err := NewRoutedToolInvoker(searchInvoker, remoteInvoker, []string{SearchKnowledgeToolName})
		Expect(err).NotTo(HaveOccurred())
		resolution := app.ToolResolutionContext{OrgID: uuid.New(), UserID: uuid.New()}
		invocation := app.ToolInvocationContext{OrgID: resolution.OrgID, UserID: resolution.UserID, RunID: uuid.New()}

		specs, err := routed.Available(context.Background(), resolution, []model.ToolBinding{{Name: "search_knowledge"}, {Name: "http_get"}})

		Expect(err).NotTo(HaveOccurred())
		Expect(specs).To(HaveLen(2))
		Expect(remoteClient.listRequest.GetOrgId()).To(Equal(resolution.OrgID.String()))
		Expect(remoteClient.listRequest.GetUserId()).To(Equal(resolution.UserID.String()))

		result, err := routed.Invoke(context.Background(), invocation, model.ToolCall{
			ID:        "call-http",
			Name:      "http_get",
			Arguments: []byte(`{"url":"http://localhost/tool"}`),
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(result.IsError).To(BeFalse())
		Expect(result.ToolImplVersion).To(Equal("http_get:test"))
		Expect(result.InvocationID).NotTo(Equal(uuid.Nil))
		Expect(result.Content).To(ContainSubstring(`"body":"ok"`))
		Expect(remoteClient.invokeRequest.GetToolName()).To(Equal("http_get"))
		Expect(remoteClient.invokeRequest.GetInvocationId()).To(Equal(result.InvocationID.String()))
	})

	It("fails closed for remote tools when no remote tool service is configured", func() {
		searchInvoker, err := NewSearchKnowledgeToolInvoker(&retrievalClientStub{})
		Expect(err).NotTo(HaveOccurred())
		routed, err := NewRoutedToolInvoker(searchInvoker, nil, []string{SearchKnowledgeToolName})
		Expect(err).NotTo(HaveOccurred())

		_, err = routed.Available(context.Background(), app.ToolResolutionContext{OrgID: uuid.New(), UserID: uuid.New()}, []model.ToolBinding{{Name: "http_get"}})

		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("remote tool service is not configured"))
	})

	It("rejects duplicate requested tool names defensively", func() {
		searchInvoker, err := NewSearchKnowledgeToolInvoker(&retrievalClientStub{})
		Expect(err).NotTo(HaveOccurred())
		remoteClient := &toolServiceClientStub{
			listResponse: &toolspb.ListAvailableToolsResponse{Tools: []*toolspb.ToolDefinition{{
				Name:                  "http_get",
				Description:           "Fetch HTTP content.",
				ParametersJson:        []byte(`{"type":"object"}`),
				ImplementationVersion: "http_get:v1",
			}}},
		}
		remoteInvoker, err := NewRemoteToolInvokerWithClient(remoteClient, newToolServiceDTOAdapter(validator.New()))
		Expect(err).NotTo(HaveOccurred())
		routed, err := NewRoutedToolInvoker(searchInvoker, remoteInvoker, []string{SearchKnowledgeToolName})
		Expect(err).NotTo(HaveOccurred())

		_, err = routed.Available(context.Background(), app.ToolResolutionContext{OrgID: uuid.New(), UserID: uuid.New()}, []model.ToolBinding{{Name: "http_get"}, {Name: "HTTP_GET"}})

		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("requested tool names must be unique"))
	})
})

var _ = Describe("toolServiceDTOAdapter", func() {
	It("uses response error_type instead of treating HTTP failures as unknown successes", func() {
		adapter := newToolServiceDTOAdapter(validator.New())

		result, err := adapter.FromInvokeToolResponse(&toolspb.InvokeToolResponse{
			ResultJson:            []byte(`{"status":500}`),
			IsError:               true,
			ErrorCode:             "http_tool_request_failed",
			ErrorType:             model.ToolErrorTypeTransient.String(),
			ImplementationVersion: "http_get:test",
		}, model.ToolCall{ID: "call-http", Name: "http_get"})

		Expect(err).NotTo(HaveOccurred())
		Expect(result.IsError).To(BeTrue())
		Expect(result.ErrorType).To(Equal(model.ToolErrorTypeTransient))
		Expect(result.ToolImplVersion).To(Equal("http_get:test"))
	})

	It("maps legacy HTTP failure codes to an error type", func() {
		Expect(toolServiceErrorType("", "http_tool_request_failed")).To(Equal(model.ToolErrorTypeTransient))
	})
})

type toolServiceClientStub struct {
	listRequest    *toolspb.ListAvailableToolsRequest
	listResponse   *toolspb.ListAvailableToolsResponse
	listErr        error
	invokeRequest  *toolspb.InvokeToolRequest
	invokeResponse *toolspb.InvokeToolResponse
	invokeErr      error
}

func (s *toolServiceClientStub) ListAvailableTools(_ context.Context, req *toolspb.ListAvailableToolsRequest, _ ...grpc.CallOption) (*toolspb.ListAvailableToolsResponse, error) {
	s.listRequest = req
	return s.listResponse, s.listErr
}

func (s *toolServiceClientStub) Invoke(_ context.Context, req *toolspb.InvokeToolRequest, _ ...grpc.CallOption) (*toolspb.InvokeToolResponse, error) {
	s.invokeRequest = req
	return s.invokeResponse, s.invokeErr
}
