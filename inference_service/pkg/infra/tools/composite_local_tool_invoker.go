package tools

import (
	"context"

	"inference_service/pkg/app"
	"inference_service/pkg/domain"
	"inference_service/pkg/domain/model"

	log "github.com/sirupsen/logrus"
)

type CompositeLocalToolInvoker struct {
	invokers map[string]app.ToolInvoker
}

func NewCompositeLocalToolInvoker(invokers map[string]app.ToolInvoker) (*CompositeLocalToolInvoker, error) {
	log.Trace("NewCompositeLocalToolInvoker")

	if len(invokers) == 0 {
		return nil, domain.ErrValidationFailed.Extend("local tool invokers are required")
	}
	indexed := make(map[string]app.ToolInvoker, len(invokers))
	for name, invoker := range invokers {
		key := toolNameKey(name)
		if key == "" {
			return nil, domain.ErrValidationFailed.Extend("local tool name is required")
		}
		if invoker == nil {
			return nil, domain.ErrValidationFailed.Extend("local tool invoker is required")
		}
		if _, exists := indexed[key]; exists {
			return nil, domain.ErrValidationFailed.Extend("local tool names must be unique")
		}
		indexed[key] = invoker
	}
	return &CompositeLocalToolInvoker{invokers: indexed}, nil
}

func (i *CompositeLocalToolInvoker) Available(ctx context.Context, resolution app.ToolResolutionContext, bindings []model.ToolBinding) ([]model.ToolSpec, error) {
	log.Trace("CompositeLocalToolInvoker Available")

	specs := make([]model.ToolSpec, 0, len(bindings))
	for _, binding := range bindings {
		invoker, ok := i.invokers[toolNameKey(binding.Name)]
		if !ok {
			return nil, domain.ErrValidationFailed.Extend("unknown local tool binding")
		}
		toolSpecs, err := invoker.Available(ctx, resolution, []model.ToolBinding{binding})
		if err != nil {
			return nil, err
		}
		specs = append(specs, toolSpecs...)
	}
	return specs, nil
}

func (i *CompositeLocalToolInvoker) Invoke(ctx context.Context, invocation app.ToolInvocationContext, call model.ToolCall) (model.ToolResult, error) {
	log.Trace("CompositeLocalToolInvoker Invoke")

	invoker, ok := i.invokers[toolNameKey(call.Name)]
	if !ok {
		err := domain.ErrValidationFailed.Extend("local tool is not allowed")
		return model.ToolResult{
			CallID:    call.ID,
			Name:      call.Name,
			Content:   err.Error(),
			IsError:   true,
			ErrorType: model.ToolErrorTypePolicyDenied,
		}, err
	}
	return invoker.Invoke(ctx, invocation, call)
}

func (i *CompositeLocalToolInvoker) ToolNames() []string {
	log.Trace("CompositeLocalToolInvoker ToolNames")

	names := make([]string, 0, len(i.invokers))
	for name := range i.invokers {
		names = append(names, name)
	}
	return names
}

var _ app.ToolInvoker = (*CompositeLocalToolInvoker)(nil)
