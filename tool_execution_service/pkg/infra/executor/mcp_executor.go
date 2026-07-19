package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"tool_execution_service/pkg/domain"
	"tool_execution_service/pkg/domain/model"

	"lib/shared_lib/serializer"
	"lib/shared_lib/userevents"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

const (
	mcpJSONRPCVersion      = "2.0"
	mcpMethodToolsList     = "tools/list"
	mcpMethodToolsCall     = "tools/call"
	mcpImplementationLabel = "mcp"
)

type CredentialResolver interface {
	ResolveCredential(ctx context.Context, ref string) (string, error)
}

type boundaryHTTPClientFactory func(egress model.EgressPolicy, timeout model.TimeoutPolicy) HTTPClient

type MCPExecutor struct {
	endpoint           string
	client             HTTPClient
	clientFactory      boundaryHTTPClientFactory
	credentialResolver CredentialResolver
}

func NewMCPExecutor(endpoint string, credentialResolver CredentialResolver) *MCPExecutor {
	log.Trace("NewMCPExecutor")

	return &MCPExecutor{
		endpoint: strings.TrimSpace(endpoint),
		clientFactory: func(egress model.EgressPolicy, timeout model.TimeoutPolicy) HTTPClient {
			return NewBoundaryHTTPClient(egress, timeout)
		},
		credentialResolver: credentialResolver,
	}
}

func NewMCPExecutorWithClient(endpoint string, client HTTPClient, credentialResolver CredentialResolver) *MCPExecutor {
	log.Trace("NewMCPExecutorWithClient")

	executor := NewMCPExecutor(endpoint, credentialResolver)
	executor.client = client
	return executor
}

func (e *MCPExecutor) Execute(ctx context.Context, tool *model.ToolDefinition, command model.InvokeToolCommand, policy model.PolicySet) (*model.ToolInvocationResult, error) {
	log.Trace("MCPExecutor Execute")

	endpoint := e.endpointForTool(tool)
	if err := validateSchemaPolicy(policy.Schema, command.ArgumentsJSON); err != nil {
		return nil, err
	}
	args, err := mcpArguments(command.ArgumentsJSON)
	if err != nil {
		return nil, err
	}
	response, latencyMs, err := e.call(ctx, endpoint, policy, mcpJSONRPCRequest{
		JSONRPC: mcpJSONRPCVersion,
		ID:      command.InvocationID.String(),
		Method:  mcpMethodToolsCall,
		Params: map[string]any{
			"name":      command.ToolName,
			"arguments": args,
		},
	})
	if err != nil {
		errorType := classifyMCPError(err)
		if errorType == model.ToolErrorTypePolicyDenied {
			return nil, err
		}
		return &model.ToolInvocationResult{
			ResultJSON:            []byte(`{}`),
			IsError:               true,
			ErrorCode:             domain.ErrToolExecution.Code,
			ErrorMessage:          "mcp tool request failed",
			ErrorType:             errorType,
			ImplementationVersion: tool.ImplementationVersion,
			LatencyMs:             latencyMs,
			EgressHost:            endpointHost(endpoint),
		}, nil
	}
	if response.Error != nil {
		return &model.ToolInvocationResult{
			ResultJSON:            []byte(`{}`),
			IsError:               true,
			ErrorCode:             "mcp_tool_error",
			ErrorMessage:          response.Error.Message,
			ErrorType:             model.ToolErrorTypePermanent,
			ImplementationVersion: tool.ImplementationVersion,
			LatencyMs:             latencyMs,
			EgressHost:            endpointHost(endpoint),
		}, nil
	}
	resultJSON, err := normalizeJSON(response.Result)
	if err != nil {
		return nil, domain.ErrToolExecution.Extend("mcp result is not valid JSON")
	}
	return &model.ToolInvocationResult{
		ResultJSON:            resultJSON,
		IsError:               mcpResultIsError(response.Result),
		ErrorCode:             mcpResultErrorCode(response.Result),
		ErrorMessage:          mcpResultErrorMessage(response.Result),
		ErrorType:             mcpResultErrorType(response.Result),
		ImplementationVersion: tool.ImplementationVersion,
		LatencyMs:             latencyMs,
		EgressHost:            endpointHost(endpoint),
	}, nil
}

type MCPDiscoveryConfig struct {
	Endpoint      string
	DeclaredTools []string
	AllowedOrgIDs []uuid.UUID
}

func DiscoverMCPTools(ctx context.Context, config MCPDiscoveryConfig, policy model.PolicySet, credentialResolver CredentialResolver) ([]*model.ToolDefinition, error) {
	log.Trace("DiscoverMCPTools")

	return discoverMCPToolsWithClient(ctx, config, policy, credentialResolver, nil)
}

func discoverMCPToolsWithClient(ctx context.Context, config MCPDiscoveryConfig, policy model.PolicySet, credentialResolver CredentialResolver, client HTTPClient) ([]*model.ToolDefinition, error) {
	log.Trace("discoverMCPToolsWithClient")

	executor := NewMCPExecutor(config.Endpoint, credentialResolver)
	executor.client = client
	response, _, err := executor.call(ctx, strings.TrimSpace(config.Endpoint), policy, mcpJSONRPCRequest{
		JSONRPC: mcpJSONRPCVersion,
		ID:      "tools-list",
		Method:  mcpMethodToolsList,
		Params:  map[string]any{},
	})
	if err != nil {
		return nil, err
	}
	if response.Error != nil {
		return nil, domain.ErrToolExecution.Extend(response.Error.Message)
	}
	var list mcpListToolsResult
	if err := json.Unmarshal(response.Result, &list); err != nil {
		return nil, domain.ErrToolExecution.Extend("mcp tools/list result is invalid")
	}
	declared := declaredToolSet(config.DeclaredTools)
	discovered := make([]*model.ToolDefinition, 0, len(config.DeclaredTools))
	for _, tool := range list.Tools {
		name := strings.TrimSpace(tool.Name)
		if name == "" || !declared[strings.ToLower(name)] {
			continue
		}
		schema := canonicalInputSchema(tool.InputSchema)
		if len(schema) == 0 {
			return nil, domain.ErrToolExecution.Extend("mcp tool input schema is missing")
		}
		discovered = append(discovered, &model.ToolDefinition{
			Name:                  name,
			Description:           strings.TrimSpace(tool.Description),
			ParametersJSON:        schema,
			ImplementationVersion: mcpImplementationVersion(config.Endpoint, name, schema),
			ExecutorKind:          model.ToolExecutorKindMCP,
			MCPServerEndpoint:     strings.TrimSpace(config.Endpoint),
			EgressHosts:           []string{endpointHost(config.Endpoint)},
			AllowedOrgIDs:         append([]uuid.UUID(nil), config.AllowedOrgIDs...),
			Enabled:               true,
		})
	}
	sort.Slice(discovered, func(i, j int) bool {
		return discovered[i].Name < discovered[j].Name
	})
	if len(discovered) != len(declared) {
		return nil, domain.ErrToolExecution.Extend("declared mcp tool is unavailable")
	}
	return discovered, nil
}

func (e *MCPExecutor) call(ctx context.Context, endpoint string, policy model.PolicySet, rpcRequest mcpJSONRPCRequest) (*mcpJSONRPCResponse, int64, error) {
	log.Trace("MCPExecutor call")

	target, err := url.Parse(strings.TrimSpace(endpoint))
	if err != nil {
		return nil, 0, domain.ErrValidationFailed.Extend("mcp endpoint is invalid")
	}
	if err := validateHTTPGetTarget(target, policy.Egress); err != nil {
		return nil, 0, err
	}
	payload, err := json.Marshal(rpcRequest)
	if err != nil {
		return nil, 0, domain.ErrToolExecution.Extend("marshal mcp request")
	}
	requestCtx, cancel := context.WithTimeout(ctx, policy.Timeout.CallTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(requestCtx, http.MethodPost, target.String(), bytes.NewReader(payload))
	if err != nil {
		return nil, 0, domain.ErrToolExecution.Extend(err.Error())
	}
	req.Header.Set("Content-Type", "application/json")
	if err := e.injectCredential(requestCtx, req, policy.Credential); err != nil {
		return nil, 0, err
	}
	startedAt := time.Now()
	client := e.client
	if client == nil {
		client = e.clientFactory(policy.Egress, policy.Timeout)
	}
	resp, err := client.Do(req)
	latencyMs := time.Since(startedAt).Milliseconds()
	if err != nil {
		return nil, latencyMs, err
	}
	defer resp.Body.Close()
	body, err := readCappedBody(resp.Body, policy.ResponseCap)
	if err != nil {
		return nil, latencyMs, err
	}
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return nil, latencyMs, domain.ErrToolDenied.Extend("mcp endpoint denied request")
	}
	if resp.StatusCode >= http.StatusBadRequest {
		return nil, latencyMs, domain.ErrToolExecution.Extend(fmt.Sprintf("mcp endpoint returned status %d", resp.StatusCode))
	}
	var response mcpJSONRPCResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, latencyMs, domain.ErrToolExecution.Extend("mcp response is invalid JSON")
	}
	return &response, latencyMs, nil
}

func (e *MCPExecutor) endpointForTool(tool *model.ToolDefinition) string {
	log.Trace("MCPExecutor endpointForTool")

	if tool != nil {
		if endpoint := strings.TrimSpace(tool.MCPServerEndpoint); endpoint != "" {
			return endpoint
		}
	}
	return strings.TrimSpace(e.endpoint)
}

func (e *MCPExecutor) injectCredential(ctx context.Context, req *http.Request, credential model.CredentialPolicy) error {
	log.Trace("MCPExecutor injectCredential")

	if strings.TrimSpace(credential.SecretRef) == "" {
		return domain.ErrToolPolicy.Extend("mcp credential ref is not configured")
	}
	value, err := e.credentialResolver.ResolveCredential(ctx, credential.SecretRef)
	if err != nil {
		return err
	}
	headerName := strings.TrimSpace(credential.HeaderName)
	if headerName == "" {
		return domain.ErrToolPolicy.Extend("mcp credential header is not configured")
	}
	req.Header.Set(headerName, credential.Prefix+value)
	return nil
}

type mcpJSONRPCRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      string `json:"id"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
}

type mcpJSONRPCResponse struct {
	JSONRPC string           `json:"jsonrpc"`
	ID      any              `json:"id,omitempty"`
	Result  json.RawMessage  `json:"result,omitempty"`
	Error   *mcpJSONRPCError `json:"error,omitempty"`
}

type mcpJSONRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type mcpListToolsResult struct {
	Tools []mcpToolDefinitionDTO `json:"tools"`
}

type mcpToolDefinitionDTO struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	InputSchema json.RawMessage `json:"inputSchema"`
}

func mcpArguments(payload []byte) (map[string]any, error) {
	log.Trace("mcpArguments")

	var args map[string]any
	if err := json.Unmarshal(payload, &args); err != nil {
		return nil, domain.ErrValidationFailed.Extend("tool arguments must be a JSON object")
	}
	return args, nil
}

func normalizeJSON(raw json.RawMessage) ([]byte, error) {
	log.Trace("normalizeJSON")

	if len(raw) == 0 {
		return []byte(`{}`), nil
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	return json.Marshal(value)
}

func mcpResultIsError(raw json.RawMessage) bool {
	log.Trace("mcpResultIsError")

	var result struct {
		IsError bool `json:"isError"`
	}
	return json.Unmarshal(raw, &result) == nil && result.IsError
}

func mcpResultErrorCode(raw json.RawMessage) string {
	log.Trace("mcpResultErrorCode")

	if mcpResultIsError(raw) {
		return "mcp_tool_error"
	}
	return ""
}

func mcpResultErrorMessage(raw json.RawMessage) string {
	log.Trace("mcpResultErrorMessage")

	if !mcpResultIsError(raw) {
		return ""
	}
	var result struct {
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
	}
	if json.Unmarshal(raw, &result) == nil && len(result.Content) > 0 {
		return result.Content[0].Text
	}
	return "mcp tool returned an error"
}

func mcpResultErrorType(raw json.RawMessage) model.ToolErrorType {
	log.Trace("mcpResultErrorType")

	if mcpResultIsError(raw) {
		return model.ToolErrorTypePermanent
	}
	return model.ToolErrorTypeUnknown
}

func canonicalInputSchema(raw json.RawMessage) []byte {
	log.Trace("canonicalInputSchema")

	if len(raw) == 0 {
		return nil
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil
	}
	canonical, err := serializer.NewJSONSerializer().Serialize(value)
	if err != nil {
		return nil
	}
	return canonical
}

func mcpImplementationVersion(endpoint string, toolName string, schema []byte) string {
	log.Trace("mcpImplementationVersion")

	hash := userevents.SHA256String(toolName + ":" + string(schema))
	return fmt.Sprintf("%s:%s:%s", mcpImplementationLabel, endpointHost(endpoint), hash)
}

func endpointHost(endpoint string) string {
	log.Trace("endpointHost")

	parsed, err := url.Parse(strings.TrimSpace(endpoint))
	if err != nil {
		return ""
	}
	return strings.ToLower(parsed.Hostname())
}

func declaredToolSet(names []string) map[string]bool {
	log.Trace("declaredToolSet")

	result := make(map[string]bool, len(names))
	for _, name := range names {
		trimmed := strings.ToLower(strings.TrimSpace(name))
		if trimmed != "" {
			result[trimmed] = true
		}
	}
	return result
}

func classifyMCPError(err error) model.ToolErrorType {
	log.Trace("classifyMCPError")

	if errors.Is(err, domain.ErrToolPolicy) || errors.Is(err, domain.ErrToolDenied) {
		return model.ToolErrorTypePolicyDenied
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return model.ToolErrorTypeTransient
	}
	var netErr net.Error
	if errors.As(err, &netErr) && (netErr.Timeout() || netErr.Temporary()) {
		return model.ToolErrorTypeTransient
	}
	return model.ToolErrorTypeTransient
}
