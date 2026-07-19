package app_test

import (
	"context"
	"testing"

	"tool_execution_service/pkg/app"
	"tool_execution_service/pkg/domain"
	"tool_execution_service/pkg/domain/model"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestToolUsecase(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Tool execution app suite")
}

var _ = Describe("ToolUsecase", func() {
	It("lists only the registry-authorized tools for the actor", func() {
		orgID := uuid.New()
		userID := uuid.New()
		expectedTools := []*model.ToolDefinition{{
			Name:           "http_get",
			ParametersJSON: []byte(`{"type":"object"}`),
			ExecutorKind:   model.ToolExecutorKindHTTPGet,
			Enabled:        true,
		}}
		registry := &registryStub{tools: expectedTools}
		usecase := app.NewToolUsecase(registry, map[model.ToolExecutorKind]app.ToolExecutor{
			model.ToolExecutorKindHTTPGet: &executorStub{},
		}, app.WithBoundaryPolicyResolver(&policyResolverStub{}), app.WithInvocationAuditRepository(&auditRepositoryStub{}))

		tools, err := usecase.ListAvailableTools(context.Background(), model.ListAvailableToolsCommand{
			OrgID:  orgID,
			UserID: userID,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(tools).To(Equal(expectedTools))
		Expect(registry.listedOrgID).To(Equal(orgID))
		Expect(registry.listedUserID).To(Equal(userID))
	})

	It("resolves an authorized tool and dispatches to the matching executor", func() {
		orgID := uuid.New()
		userID := uuid.New()
		registry := &registryStub{
			tool: &model.ToolDefinition{
				Name:                  "http_get",
				ParametersJSON:        []byte(`{"type":"object"}`),
				ExecutorKind:          model.ToolExecutorKindHTTPGet,
				ImplementationVersion: "v1",
				Enabled:               true,
			},
		}
		executor := &executorStub{
			result: &model.ToolInvocationResult{
				ResultJSON:            []byte(`{"ok":true}`),
				ImplementationVersion: "v1",
			},
		}
		auditor := &auditRepositoryStub{}
		usecase := app.NewToolUsecase(registry, map[model.ToolExecutorKind]app.ToolExecutor{
			model.ToolExecutorKindHTTPGet: executor,
		}, app.WithBoundaryPolicyResolver(&policyResolverStub{}), app.WithInvocationAuditRepository(auditor))

		result, err := usecase.Invoke(context.Background(), model.InvokeToolCommand{
			InvocationID:  uuid.New(),
			ToolName:      "http_get",
			ArgumentsJSON: []byte(`{"url":"http://example.com"}`),
			OrgID:         orgID,
			UserID:        userID,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(result.ResultJSON).To(MatchJSON(`{"ok":true}`))
		Expect(registry.resolvedOrgID).To(Equal(orgID))
		Expect(registry.resolvedUserID).To(Equal(userID))
		Expect(executor.command.ToolName).To(Equal("http_get"))
		Expect(auditor.records).To(HaveLen(1))
		Expect(auditor.records[0].Status).To(Equal(model.ToolInvocationAuditStatusCompleted))
	})

	It("fails closed when the resolved executor is not configured", func() {
		auditor := &auditRepositoryStub{}
		usecase := app.NewToolUsecase(&registryStub{
			tool: &model.ToolDefinition{Name: "calculator", ParametersJSON: []byte(`{"type":"object"}`), ExecutorKind: model.ToolExecutorKindCalculator, Enabled: true},
		}, map[model.ToolExecutorKind]app.ToolExecutor{
			model.ToolExecutorKindHTTPGet: &executorStub{},
		}, app.WithBoundaryPolicyResolver(&policyResolverStub{}), app.WithInvocationAuditRepository(auditor))

		_, err := usecase.Invoke(context.Background(), model.InvokeToolCommand{
			ToolName: "calculator",
			OrgID:    uuid.New(),
			UserID:   uuid.New(),
		})

		Expect(err).To(MatchError(ContainSubstring("executor CALCULATOR is not configured")))
		Expect(err).To(MatchError(MatchRegexp(domain.ErrToolPolicy.Error() + ".*")))
		Expect(auditor.records).To(HaveLen(1))
		Expect(auditor.records[0].Status).To(Equal(model.ToolInvocationAuditStatusDenied))
	})

	It("records a denied audit when the registry rejects the tool for the actor", func() {
		auditor := &auditRepositoryStub{}
		usecase := app.NewToolUsecase(&registryStub{resolveErr: domain.ErrToolDenied.Extend("tool is not allowlisted for tenant")}, map[model.ToolExecutorKind]app.ToolExecutor{
			model.ToolExecutorKindHTTPGet: &executorStub{},
		}, app.WithBoundaryPolicyResolver(&policyResolverStub{}), app.WithInvocationAuditRepository(auditor))

		_, err := usecase.Invoke(context.Background(), model.InvokeToolCommand{
			InvocationID: uuid.New(),
			ToolName:     "http_get",
			OrgID:        uuid.New(),
			UserID:       uuid.New(),
		})

		Expect(err).To(MatchError(MatchRegexp(domain.ErrToolDenied.Error() + ".*")))
		Expect(auditor.records).To(HaveLen(1))
		Expect(auditor.records[0].Status).To(Equal(model.ToolInvocationAuditStatusDenied))
		Expect(auditor.records[0].ErrorType).To(Equal(model.ToolErrorTypePolicyDenied))
	})

	It("records failed audit details when the executor returns an error result", func() {
		auditor := &auditRepositoryStub{}
		usecase := app.NewToolUsecase(&registryStub{
			tool: &model.ToolDefinition{
				Name:                  "http_get",
				ParametersJSON:        []byte(`{"type":"object"}`),
				ExecutorKind:          model.ToolExecutorKindHTTPGet,
				ImplementationVersion: "v1",
				Enabled:               true,
			},
		}, map[model.ToolExecutorKind]app.ToolExecutor{
			model.ToolExecutorKindHTTPGet: &executorStub{
				result: &model.ToolInvocationResult{
					IsError:               true,
					ErrorCode:             "http_tool_request_failed",
					ErrorType:             model.ToolErrorTypeTransient,
					ImplementationVersion: "v2",
					LatencyMs:             17,
				},
			},
		}, app.WithBoundaryPolicyResolver(&policyResolverStub{}), app.WithInvocationAuditRepository(auditor))

		result, err := usecase.Invoke(context.Background(), model.InvokeToolCommand{
			InvocationID:  uuid.New(),
			ToolName:      "http_get",
			ArgumentsJSON: []byte(`{"url":"http://example.com"}`),
			OrgID:         uuid.New(),
			UserID:        uuid.New(),
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(result.IsError).To(BeTrue())
		Expect(auditor.records).To(HaveLen(1))
		Expect(auditor.records[0].Status).To(Equal(model.ToolInvocationAuditStatusFailed))
		Expect(auditor.records[0].ErrorCode).To(Equal("http_tool_request_failed"))
		Expect(auditor.records[0].ErrorType).To(Equal(model.ToolErrorTypeTransient))
		Expect(auditor.records[0].ImplementationVersion).To(Equal("v2"))
		Expect(auditor.records[0].LatencyMs).To(Equal(int64(17)))
	})

	It("does not fail the tool invocation when audit persistence fails", func() {
		usecase := app.NewToolUsecase(&registryStub{
			tool: &model.ToolDefinition{
				Name:           "http_get",
				ParametersJSON: []byte(`{"type":"object"}`),
				ExecutorKind:   model.ToolExecutorKindHTTPGet,
				Enabled:        true,
			},
		}, map[model.ToolExecutorKind]app.ToolExecutor{
			model.ToolExecutorKindHTTPGet: &executorStub{result: &model.ToolInvocationResult{ResultJSON: []byte(`{"ok":true}`)}},
		}, app.WithBoundaryPolicyResolver(&policyResolverStub{}), app.WithInvocationAuditRepository(&auditRepositoryStub{err: domain.ErrToolExecution.Extend("audit unavailable")}))

		result, err := usecase.Invoke(context.Background(), model.InvokeToolCommand{
			InvocationID:  uuid.New(),
			ToolName:      "http_get",
			ArgumentsJSON: []byte(`{"url":"http://example.com"}`),
			OrgID:         uuid.New(),
			UserID:        uuid.New(),
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(result.ResultJSON).To(MatchJSON(`{"ok":true}`))
	})
})

type registryStub struct {
	tool           *model.ToolDefinition
	tools          []*model.ToolDefinition
	listErr        error
	resolveErr     error
	listedOrgID    uuid.UUID
	listedUserID   uuid.UUID
	resolvedOrgID  uuid.UUID
	resolvedUserID uuid.UUID
}

func (s *registryStub) ListAvailableTools(ctx context.Context, orgID uuid.UUID, userID uuid.UUID) ([]*model.ToolDefinition, error) {
	_ = ctx
	s.listedOrgID = orgID
	s.listedUserID = userID
	if s.listErr != nil {
		return nil, s.listErr
	}
	if s.tools != nil {
		return s.tools, nil
	}
	return []*model.ToolDefinition{s.tool}, nil
}

func (s *registryStub) ResolveTool(ctx context.Context, orgID uuid.UUID, userID uuid.UUID, toolName string) (*model.ToolDefinition, error) {
	_ = ctx
	s.resolvedOrgID = orgID
	s.resolvedUserID = userID
	if s.resolveErr != nil {
		return nil, s.resolveErr
	}
	if s.tool == nil || s.tool.Name != toolName {
		return nil, domain.ErrToolNotFound
	}
	return s.tool, nil
}

type executorStub struct {
	result  *model.ToolInvocationResult
	err     error
	command model.InvokeToolCommand
	policy  model.PolicySet
}

func (s *executorStub) Execute(ctx context.Context, tool *model.ToolDefinition, command model.InvokeToolCommand, policy model.PolicySet) (*model.ToolInvocationResult, error) {
	_ = ctx
	_ = tool
	s.command = command
	s.policy = policy
	return s.result, s.err
}

type auditRepositoryStub struct {
	records []model.ToolInvocationAudit
	err     error
}

func (s *auditRepositoryStub) RecordInvocation(ctx context.Context, audit model.ToolInvocationAudit) error {
	_ = ctx
	s.records = append(s.records, audit)
	return s.err
}

type policyResolverStub struct {
	policy model.PolicySet
	err    error
}

func (s *policyResolverStub) ResolvePolicy(tool *model.ToolDefinition) (model.PolicySet, error) {
	_ = tool
	if s.err != nil {
		return model.PolicySet{}, s.err
	}
	return s.policy, nil
}
