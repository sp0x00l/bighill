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

func (i *RoutedToolInvoker) Available(ctx context.Context, session *model.AgentSession, bindings []model.ToolBinding) ([]model.ToolSpec, error) {
	log.Trace("RoutedToolInvoker Available")

	localBindings, remoteBindings := i.partitionBindings(bindings)
	specs := make([]model.ToolSpec, 0, len(bindings))
	if len(localBindings) > 0 {
		localSpecs, err := i.local.Available(ctx, session, localBindings)
		if err != nil {
			return nil, err
		}
		specs = append(specs, localSpecs...)
	}
	if len(remoteBindings) > 0 {
		if i.remote == nil {
			return nil, domain.ErrValidationFailed.Extend("remote tool service is not configured")
		}
		remoteSpecs, err := i.remote.Available(ctx, session, remoteBindings)
		if err != nil {
			return nil, err
		}
		specs = append(specs, remoteSpecs...)
	}
	return specs, nil
}

func (i *RoutedToolInvoker) Invoke(ctx context.Context, session *model.AgentSession, call model.ToolCall) (model.ToolResult, error) {
	log.Trace("RoutedToolInvoker Invoke")

	if i.isLocal(call.Name) {
		return i.local.Invoke(ctx, session, call)
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
	return i.remote.Invoke(ctx, session, call)
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

var _ app.ToolInvoker = (*RoutedToolInvoker)(nil)
