package grpc

import (
	"errors"
	"testing"

	"tool_execution_service/pkg/domain"
	"tool_execution_service/pkg/domain/model"

	toolspb "lib/data_contracts_lib/tools"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestToolDTOAdapter(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Tool execution gRPC suite")
}

var _ = Describe("ToolDTOAdapter", func() {
	var adapter *ToolDTOAdapter

	BeforeEach(func() {
		adapter = NewToolDTOAdapter(validator.New())
	})

	It("maps a valid invoke request to a domain command", func() {
		orgID := uuid.New()
		userID := uuid.New()
		invocationID := uuid.New()

		command, err := adapter.FromInvokeToolRequest(&toolspb.InvokeToolRequest{
			InvocationId:  invocationID.String(),
			ToolName:      " http_get ",
			ArgumentsJson: []byte(`{"url":"https://example.com"}`),
			OrgId:         orgID.String(),
			UserId:        userID.String(),
			TraceId:       "trace-1",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(command.InvocationID).To(Equal(invocationID))
		Expect(command.ToolName).To(Equal("http_get"))
		Expect(command.OrgID).To(Equal(orgID))
		Expect(command.UserID).To(Equal(userID))
		Expect(command.TraceID).To(Equal("trace-1"))
	})

	It("maps a valid list request to a domain command", func() {
		orgID := uuid.New()
		userID := uuid.New()

		command, err := adapter.FromListAvailableToolsRequest(&toolspb.ListAvailableToolsRequest{
			OrgId:  " " + orgID.String() + " ",
			UserId: userID.String(),
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(command.OrgID).To(Equal(orgID))
		Expect(command.UserID).To(Equal(userID))
	})

	It("rejects nil and malformed list requests at the gRPC boundary", func() {
		_, err := adapter.FromListAvailableToolsRequest(nil)
		Expect(err).To(MatchError(ContainSubstring("list tools request is required")))

		_, err = adapter.FromListAvailableToolsRequest(&toolspb.ListAvailableToolsRequest{
			OrgId:  "not-a-uuid",
			UserId: uuid.NewString(),
		})
		Expect(err).To(HaveOccurred())

		_, err = adapter.FromListAvailableToolsRequest(&toolspb.ListAvailableToolsRequest{
			OrgId:  uuid.NewString(),
			UserId: uuid.Nil.String(),
		})
		Expect(err).To(MatchError(ContainSubstring("user_id is invalid")))
	})

	It("rejects malformed invoke requests at the gRPC boundary", func() {
		_, err := adapter.FromInvokeToolRequest(nil)
		Expect(err).To(MatchError(ContainSubstring("invoke tool request is required")))

		_, err = adapter.FromInvokeToolRequest(&toolspb.InvokeToolRequest{
			ToolName:      "http_get",
			ArgumentsJson: []byte(`{"url":"https://example.com"}`),
			OrgId:         uuid.NewString(),
			UserId:        uuid.NewString(),
		})
		Expect(err).To(MatchError(ContainSubstring("InvocationID")))

		_, err = adapter.FromInvokeToolRequest(&toolspb.InvokeToolRequest{
			InvocationId:  uuid.NewString(),
			ToolName:      "http_get",
			ArgumentsJson: []byte(`not-json`),
			OrgId:         uuid.NewString(),
			UserId:        uuid.NewString(),
		})
		Expect(err).To(MatchError(ContainSubstring("arguments_json must contain valid JSON")))
	})

	It("rejects invoke requests with invalid actors or invocation ids at the gRPC boundary", func() {
		_, err := adapter.FromInvokeToolRequest(&toolspb.InvokeToolRequest{
			InvocationId:  "not-a-uuid",
			ToolName:      "http_get",
			ArgumentsJson: []byte(`{"url":"https://example.com"}`),
			OrgId:         uuid.NewString(),
			UserId:        uuid.NewString(),
		})
		Expect(err).To(HaveOccurred())

		_, err = adapter.FromInvokeToolRequest(&toolspb.InvokeToolRequest{
			InvocationId:  uuid.NewString(),
			ToolName:      "http_get",
			ArgumentsJson: []byte(`{"url":"https://example.com"}`),
			OrgId:         uuid.Nil.String(),
			UserId:        uuid.NewString(),
		})
		Expect(err).To(MatchError(ContainSubstring("org_id is invalid")))
	})

	It("maps available tools to protobuf DTOs", func() {
		response := adapter.ToListAvailableToolsResponse([]*model.ToolDefinition{
			{
				Name:                  "http_get",
				Description:           "Fetches data",
				ParametersJSON:        []byte(`{"type":"object"}`),
				ImplementationVersion: "v1",
			},
		})

		Expect(response.Tools).To(HaveLen(1))
		Expect(response.Tools[0].Name).To(Equal("http_get"))
		Expect(response.Tools[0].ParametersJson).To(MatchJSON(`{"type":"object"}`))
	})

	It("maps nil and error invocation results to response DTOs", func() {
		Expect(adapter.ToInvokeToolResponse(nil)).To(Equal(&toolspb.InvokeToolResponse{}))

		response := adapter.ToInvokeToolResponse(&model.ToolInvocationResult{
			ResultJSON:            []byte(`{"status":500}`),
			IsError:               true,
			ErrorCode:             "http_tool_request_failed",
			ErrorMessage:          "http tool returned status 500",
			ErrorType:             model.ToolErrorTypeTransient,
			ImplementationVersion: "http_get:v1",
			LatencyMs:             42,
		})

		Expect(response.ResultJson).To(MatchJSON(`{"status":500}`))
		Expect(response.IsError).To(BeTrue())
		Expect(response.ErrorCode).To(Equal("http_tool_request_failed"))
		Expect(response.ErrorMessage).To(Equal("http tool returned status 500"))
		Expect(response.ErrorType).To(Equal(model.ToolErrorTypeTransient.String()))
		Expect(response.ImplementationVersion).To(Equal("http_get:v1"))
		Expect(response.LatencyMs).To(Equal(int64(42)))
	})

	It("maps domain errors to stable gRPC status codes", func() {
		cases := []struct {
			err  error
			code codes.Code
		}{
			{domain.ErrValidationFailed.Extend("bad request"), codes.InvalidArgument},
			{domain.ErrToolNotFound.Extend("missing"), codes.NotFound},
			{domain.ErrToolDenied.Extend("denied"), codes.PermissionDenied},
			{domain.ErrToolPolicy.Extend("blocked"), codes.PermissionDenied},
			{domain.ErrToolExecution.Extend("unavailable"), codes.Unavailable},
			{errors.New("unexpected"), codes.Internal},
		}

		for _, tc := range cases {
			mapped := toolStatusError(tc.err)
			Expect(status.Code(mapped)).To(Equal(tc.code))
			Expect(mapped.Error()).To(ContainSubstring(tc.err.Error()))
		}
	})
})
