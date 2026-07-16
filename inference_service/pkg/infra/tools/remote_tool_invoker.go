package tools

import (
	"context"
	"fmt"
	"strings"
	"time"

	"inference_service/pkg/app"
	"inference_service/pkg/domain"
	"inference_service/pkg/domain/model"

	toolspb "lib/data_contracts_lib/tools"
	rpcLib "lib/shared_lib/rpc"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ToolServiceClientConfig struct {
	Address       string
	DialTimeoutMs int
	CallTimeoutMs int
	RetryCount    int
}

func ValidateToolServiceClientConfig(config ToolServiceClientConfig) error {
	log.Trace("ValidateToolServiceClientConfig")

	if strings.TrimSpace(config.Address) == "" {
		return fmt.Errorf("tool service grpc address is required")
	}
	if config.DialTimeoutMs <= 0 {
		return fmt.Errorf("tool service grpc dial timeout must be greater than zero")
	}
	if config.CallTimeoutMs <= 0 {
		return fmt.Errorf("tool service grpc call timeout must be greater than zero")
	}
	if config.RetryCount <= 0 {
		return fmt.Errorf("tool service grpc retry count must be greater than zero")
	}
	return nil
}

type RemoteToolInvoker struct {
	conn    *grpc.ClientConn
	client  toolspb.ToolServiceClient
	adapter *toolServiceDTOAdapter
}

func NewRemoteToolInvoker(ctx context.Context, config ToolServiceClientConfig, opts ...grpc.DialOption) (*RemoteToolInvoker, error) {
	log.Trace("NewRemoteToolInvoker")

	if err := ValidateToolServiceClientConfig(config); err != nil {
		return nil, err
	}
	conn, err := rpcLib.NewClient(ctx, rpcLib.Config{
		Address:          strings.TrimSpace(config.Address),
		Insecure:         true,
		DialTimeout:      time.Duration(config.DialTimeoutMs) * time.Millisecond,
		PerCallTimeout:   time.Duration(config.CallTimeoutMs) * time.Millisecond,
		MaxRetryAttempts: config.RetryCount,
	}, opts...)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("tool service grpc connection instantiation failed")
		return nil, err
	}
	return &RemoteToolInvoker{
		conn:    conn,
		client:  toolspb.NewToolServiceClient(conn),
		adapter: newToolServiceDTOAdapter(validator.New()),
	}, nil
}

func NewRemoteToolInvokerWithClient(client toolspb.ToolServiceClient, adapter *toolServiceDTOAdapter) (*RemoteToolInvoker, error) {
	log.Trace("NewRemoteToolInvokerWithClient")

	if client == nil {
		return nil, domain.ErrValidationFailed.Extend("tool service client is required")
	}
	if adapter == nil {
		return nil, domain.ErrValidationFailed.Extend("tool service dto adapter is required")
	}
	return &RemoteToolInvoker{client: client, adapter: adapter}, nil
}

func (i *RemoteToolInvoker) Close() error {
	log.Trace("RemoteToolInvoker Close")

	if i.conn == nil {
		return nil
	}
	return i.conn.Close()
}

func (i *RemoteToolInvoker) Available(ctx context.Context, resolution app.ToolResolutionContext, bindings []model.ToolBinding) ([]model.ToolSpec, error) {
	log.Trace("RemoteToolInvoker Available")

	if len(bindings) == 0 {
		return nil, nil
	}
	req, err := i.adapter.ToListAvailableToolsRequest(resolution)
	if err != nil {
		return nil, err
	}
	resp, err := i.client.ListAvailableTools(ctx, req)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("tool service list available tools failed")
		return nil, remoteToolError(err)
	}
	return i.adapter.FromListAvailableToolsResponse(resp, bindings)
}

func (i *RemoteToolInvoker) Invoke(ctx context.Context, invocation app.ToolInvocationContext, call model.ToolCall) (model.ToolResult, error) {
	log.Trace("RemoteToolInvoker Invoke")

	invocationID := uuid.New()
	req, err := i.adapter.ToInvokeToolRequest(invocation, call, invocationID)
	if err != nil {
		return model.ToolResult{
			InvocationID: invocationID,
			CallID:       call.ID,
			Name:         call.Name,
			Content:      err.Error(),
			IsError:      true,
			ErrorType:    model.ToolErrorTypePolicyDenied,
		}, err
	}
	resp, err := i.client.Invoke(ctx, req)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("tool service invoke failed")
		mappedErr := remoteToolError(err)
		return model.ToolResult{
			InvocationID: invocationID,
			CallID:       call.ID,
			Name:         call.Name,
			Content:      mappedErr.Error(),
			IsError:      true,
			ErrorType:    remoteToolErrorType(err),
		}, mappedErr
	}
	result, err := i.adapter.FromInvokeToolResponse(resp, call)
	if err != nil {
		return model.ToolResult{
			InvocationID: invocationID,
			CallID:       call.ID,
			Name:         call.Name,
			Content:      err.Error(),
			IsError:      true,
			ErrorType:    model.ToolErrorTypePermanent,
		}, err
	}
	result.InvocationID = invocationID
	return result, nil
}

var _ app.ToolInvoker = (*RemoteToolInvoker)(nil)

func remoteToolError(err error) error {
	log.Trace("remoteToolError")

	switch status.Code(err) {
	case codes.InvalidArgument:
		return domain.ErrValidationFailed.Extend(rpcLib.ExtractGRPCErrMsg(err).Error())
	case codes.NotFound:
		return domain.ErrValidationFailed.Extend(rpcLib.ExtractGRPCErrMsg(err).Error())
	case codes.PermissionDenied:
		return domain.ErrValidationFailed.Extend(rpcLib.ExtractGRPCErrMsg(err).Error())
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted:
		return domain.ErrModelNotReady.Extend(rpcLib.ExtractGRPCErrMsg(err).Error())
	default:
		return fmt.Errorf("tool service request failed: %w", rpcLib.ExtractGRPCErrMsg(err))
	}
}

func remoteToolErrorType(err error) model.ToolErrorType {
	log.Trace("remoteToolErrorType")

	switch status.Code(err) {
	case codes.InvalidArgument, codes.NotFound, codes.PermissionDenied:
		return model.ToolErrorTypePolicyDenied
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted:
		return model.ToolErrorTypeTransient
	default:
		return model.ToolErrorTypePermanent
	}
}
