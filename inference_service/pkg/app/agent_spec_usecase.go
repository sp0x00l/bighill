package app

import (
	"context"
	"errors"
	"strings"

	"inference_service/pkg/domain"
	"inference_service/pkg/domain/model"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
)

func (u *inferenceUsecase) PublishAgentSpec(ctx context.Context, request model.AgentSpecPublication) (spec *model.AgentSpec, err error) {
	log.Trace("InferenceUsecase PublishAgentSpec")

	ctx = contextForActorOrg(ctx, request.UserID, request.OrgID)
	ctx, span := startInferenceSpan(ctx, "agent_spec.publish",
		attribute.String("org_id", request.OrgID.String()),
		attribute.String("user_id", request.UserID.String()),
	)
	defer endInferenceSpanOnReturn(ctx, span, &err)

	spec = request.Spec
	inferenceModel, err := u.modelRepository.ReadByID(ctx, request.OrgID, spec.ModelID)
	if err != nil {
		return nil, err
	}
	if inferenceModel.OrgID != request.OrgID {
		return nil, domain.ErrModelNotFound
	}
	if err := u.ensureAgentSpecPublishable(ctx, request.UserID, spec, inferenceModel); err != nil {
		return nil, err
	}
	return u.agentSpecRepository.UpsertAgentSpec(ctx, spec)
}

func (u *inferenceUsecase) ensureAgentSpecPublishable(ctx context.Context, userID uuid.UUID, spec *model.AgentSpec, inferenceModel *model.InferenceModel) error {
	log.Trace("InferenceUsecase ensureAgentSpecPublishable")

	if agentSpecRequiresToolCalls(spec) {
		if u.toolInvoker == nil {
			return domain.ErrModelNotReady.Extend("agent tool invoker is not configured")
		}
		if _, err := u.toolInvoker.Available(ctx, ToolResolutionContext{
			OrgID:  spec.OrgID,
			UserID: userID,
			Spec:   spec,
		}, spec.ToolBindings); err != nil {
			return err
		}
		if u.capabilityReportRepository == nil {
			return domain.ErrModelNotReady.Extend("model capability report repository is not configured")
		}
		effectiveBaseID := strings.TrimSpace(inferenceModel.EffectiveBaseID)
		if effectiveBaseID == "" {
			return domain.ErrModelNotReady.Extend("model effective base is required for tool-call capability")
		}
		report, err := u.capabilityReportRepository.ReadCapabilityReportForEffectiveBase(ctx, effectiveBaseID)
		if err != nil && !errors.Is(err, domain.ErrModelNotReady) {
			return err
		}
		if report == nil || !report.SupportsToolCalls {
			report, err = u.probeAndRecordCapabilityReport(ctx, inferenceModel)
			if err != nil {
				return err
			}
			if report == nil || !report.SupportsToolCalls {
				return domain.ErrModelNotReady.Extend("model cannot expose tool calls")
			}
		}
	}
	return nil
}

func agentSpecRequiresToolCalls(spec *model.AgentSpec) bool {
	log.Trace("agentSpecRequiresToolCalls")

	for _, binding := range spec.ToolBindings {
		if binding.Name != "" || binding.Required || binding.ToolChoice != "" {
			return true
		}
	}
	return false
}
