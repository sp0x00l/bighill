package tools

import (
	"context"
	"strings"

	"inference_service/pkg/app"
	"inference_service/pkg/domain"
	"inference_service/pkg/domain/model"

	log "github.com/sirupsen/logrus"
)

type RoutedToolInvoker struct {
	local      app.ToolInvoker
	remote     app.ToolInvoker
	localTools map[string]struct{}
}

func NewRoutedToolInvoker(local app.ToolInvoker, remote app.ToolInvoker, localToolNames []string) (*RoutedToolInvoker, error) {
	log.Trace("NewRoutedToolInvoker")

	if local == nil {
		return nil, domain.ErrValidationFailed.Extend("local tool invoker is required")
	}
	indexed := make(map[string]struct{}, len(localToolNames))
	for _, name := range localToolNames {
		name = toolNameKey(name)
		if name != "" {
			indexed[name] = struct{}{}
		}
	}
	if len(indexed) == 0 {
		return nil, domain.ErrValidationFailed.Extend("local tool names are required")
	}
	return &RoutedToolInvoker{
		local:      local,
		remote:     remote,
		localTools: indexed,
	}, nil
}

func (i *RoutedToolInvoker) Available(ctx context.Context, resolution app.ToolResolutionContext, bindings []model.ToolBinding) ([]model.ToolSpec, error) {
	log.Trace("RoutedToolInvoker Available")

	if err := rejectDuplicateToolBindings(bindings); err != nil {
		return nil, err
	}
	localBindings, remoteBindings := i.partitionBindings(bindings)
	specs := make([]model.ToolSpec, 0, len(bindings))
	if len(localBindings) > 0 {
		localSpecs, err := i.local.Available(ctx, resolution, localBindings)
		if err != nil {
			return nil, err
		}
		for idx := range localSpecs {
			if strings.TrimSpace(localSpecs[idx].Locality) == "" {
				localSpecs[idx].Locality = "local"
			}
		}
		specs = append(specs, localSpecs...)
	}
	if len(remoteBindings) > 0 {
		if i.remote == nil {
			return nil, domain.ErrValidationFailed.Extend("remote tool service is not configured")
		}
		remoteSpecs, err := i.remote.Available(ctx, resolution, remoteBindings)
		if err != nil {
			return nil, err
		}
		for idx := range remoteSpecs {
			if strings.TrimSpace(remoteSpecs[idx].Locality) == "" {
				remoteSpecs[idx].Locality = "remote"
			}
		}
		specs = append(specs, remoteSpecs...)
	}
	if err := rejectDuplicateToolSpecs(specs); err != nil {
		return nil, err
	}
	return specs, nil
}

func (i *RoutedToolInvoker) Invoke(ctx context.Context, invocation app.ToolInvocationContext, call model.ToolCall) (model.ToolResult, error) {
	log.Trace("RoutedToolInvoker Invoke")

	if i.isLocal(call.Name) {
		return i.local.Invoke(ctx, invocation, call)
	}
	if i.remote == nil {
		err := domain.ErrValidationFailed.Extend("remote tool service is not configured")
		return model.ToolResult{
			CallID:    call.ID,
			Name:      call.Name,
			Content:   err.Error(),
			IsError:   true,
			ErrorType: model.ToolErrorTypePolicyDenied,
		}, err
	}
	return i.remote.Invoke(ctx, invocation, call)
}

func (i *RoutedToolInvoker) partitionBindings(bindings []model.ToolBinding) ([]model.ToolBinding, []model.ToolBinding) {
	log.Trace("RoutedToolInvoker partitionBindings")

	localBindings := []model.ToolBinding{}
	remoteBindings := []model.ToolBinding{}
	for _, binding := range bindings {
		if i.isLocal(binding.Name) {
			localBindings = append(localBindings, binding)
			continue
		}
		remoteBindings = append(remoteBindings, binding)
	}
	return localBindings, remoteBindings
}

func (i *RoutedToolInvoker) isLocal(name string) bool {
	log.Trace("RoutedToolInvoker isLocal")

	_, ok := i.localTools[toolNameKey(strings.TrimSpace(name))]
	return ok
}

func rejectDuplicateToolSpecs(specs []model.ToolSpec) error {
	log.Trace("rejectDuplicateToolSpecs")

	seen := make(map[string]struct{}, len(specs))
	for _, spec := range specs {
		key := toolNameKey(spec.Name)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			return domain.ErrValidationFailed.Extend("resolved tool names must be unique")
		}
		seen[key] = struct{}{}
	}
	return nil
}

func rejectDuplicateToolBindings(bindings []model.ToolBinding) error {
	log.Trace("rejectDuplicateToolBindings")

	seen := make(map[string]struct{}, len(bindings))
	for _, binding := range bindings {
		key := toolNameKey(binding.Name)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			return domain.ErrValidationFailed.Extend("requested tool names must be unique")
		}
		seen[key] = struct{}{}
	}
	return nil
}

var _ app.ToolInvoker = (*RoutedToolInvoker)(nil)
