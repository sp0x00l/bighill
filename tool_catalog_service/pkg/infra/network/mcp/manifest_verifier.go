package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"lib/shared_lib/serializer"
	"tool_catalog_service/pkg/domain"
	"tool_catalog_service/pkg/domain/model"

	"github.com/santhosh-tekuri/jsonschema/v6"
	log "github.com/sirupsen/logrus"
)

const (
	jsonRPCVersion        = "2.0"
	toolsListMethod       = "tools/list"
	contentTypeHeader     = "Content-Type"
	jsonContentType       = "application/json"
	maxListToolsBodyBytes = 1 << 20
)

type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type ManifestVerifier struct {
	client  HTTPClient
	timeout time.Duration
}

func NewManifestVerifier(client HTTPClient, timeout time.Duration) *ManifestVerifier {
	log.Trace("NewManifestVerifier")

	if client == nil {
		client = http.DefaultClient
	}
	return &ManifestVerifier{client: client, timeout: timeout}
}

func (v *ManifestVerifier) VerifyCapabilityManifest(ctx context.Context, command model.PublishCapabilityCommand) error {
	log.Trace("ManifestVerifier VerifyCapabilityManifest")

	if err := validateToolInputSchema(command.ParametersJSON); err != nil {
		return err
	}
	if command.Kind != model.CapabilityKindMCP {
		return nil
	}
	liveSchema, err := v.liveInputSchema(ctx, command.MCPServerEndpoint, command.ToolName)
	if err != nil {
		return err
	}
	if !bytes.Equal(liveSchema, command.ParametersJSON) {
		return domain.ErrToolCatalogValidation.Extend("mcp tool schema does not match live tool")
	}
	return nil
}

func (v *ManifestVerifier) liveInputSchema(ctx context.Context, endpoint string, toolName string) ([]byte, error) {
	log.Trace("ManifestVerifier liveInputSchema")

	endpoint = strings.TrimSpace(endpoint)
	toolName = strings.TrimSpace(toolName)
	if endpoint == "" || toolName == "" {
		return nil, domain.ErrToolCatalogValidation.Extend("mcp endpoint and tool name are required")
	}
	timeout := v.timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	requestCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	payload, err := json.Marshal(jsonRPCRequest{
		JSONRPC: jsonRPCVersion,
		ID:      "tools-list",
		Method:  toolsListMethod,
		Params:  map[string]any{},
	})
	if err != nil {
		return nil, domain.ErrToolCatalogValidation.Extend("mcp tools/list request is not serializable")
	}
	req, err := http.NewRequestWithContext(requestCtx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, domain.ErrToolCatalogValidation.Extend("mcp endpoint is invalid")
	}
	req.Header.Set(contentTypeHeader, jsonContentType)
	resp, err := v.client.Do(req)
	if err != nil {
		return nil, domain.ErrToolCatalogValidation.Extend(fmt.Sprintf("mcp tools/list failed: %s", err.Error()))
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxListToolsBodyBytes+1))
	if err != nil {
		return nil, domain.ErrToolCatalogValidation.Extend("mcp tools/list response cannot be read")
	}
	if len(body) > maxListToolsBodyBytes {
		return nil, domain.ErrToolCatalogValidation.Extend("mcp tools/list response is too large")
	}
	if resp.StatusCode >= http.StatusBadRequest {
		return nil, domain.ErrToolCatalogValidation.Extend(fmt.Sprintf("mcp tools/list returned status %d", resp.StatusCode))
	}
	var parsed jsonRPCResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, domain.ErrToolCatalogValidation.Extend("mcp tools/list response is invalid JSON")
	}
	if parsed.Error != nil {
		return nil, domain.ErrToolCatalogValidation.Extend(parsed.Error.Message)
	}
	var list listToolsResult
	if err := json.Unmarshal(parsed.Result, &list); err != nil {
		return nil, domain.ErrToolCatalogValidation.Extend("mcp tools/list result is invalid")
	}
	for _, tool := range list.Tools {
		if strings.TrimSpace(tool.Name) != toolName {
			continue
		}
		schema, err := canonicalJSON(tool.InputSchema)
		if err != nil {
			return nil, domain.ErrToolCatalogValidation.Extend("mcp tool input schema is invalid")
		}
		if err := validateToolInputSchema(schema); err != nil {
			return nil, err
		}
		return schema, nil
	}
	return nil, domain.ErrToolCatalogValidation.Extend("mcp tool is not available")
}

type jsonRPCRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      string `json:"id"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
}

type jsonRPCResponse struct {
	Result json.RawMessage `json:"result,omitempty"`
	Error  *jsonRPCError   `json:"error,omitempty"`
}

type jsonRPCError struct {
	Message string `json:"message"`
}

type listToolsResult struct {
	Tools []toolDefinition `json:"tools"`
}

type toolDefinition struct {
	Name        string          `json:"name"`
	InputSchema json.RawMessage `json:"inputSchema"`
}

func canonicalJSON(raw json.RawMessage) ([]byte, error) {
	log.Trace("canonicalJSON")

	if len(raw) == 0 {
		return nil, nil
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	return serializer.NewJSONSerializer().Serialize(value)
}

func validateToolInputSchema(raw []byte) error {
	log.Trace("validateToolInputSchema")

	if len(bytes.TrimSpace(raw)) == 0 {
		return domain.ErrToolCatalogValidation.Extend("tool input schema is required")
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return domain.ErrToolCatalogValidation.Extend("tool input schema is invalid JSON")
	}
	schemaDocument, ok := value.(map[string]any)
	if !ok {
		return domain.ErrToolCatalogValidation.Extend("tool input schema must be a JSON object")
	}
	if len(schemaDocument) == 0 {
		return domain.ErrToolCatalogValidation.Extend("tool input schema must not be empty")
	}
	compiler := jsonschema.NewCompiler()
	if err := compiler.AddResource("tool_input_schema.json", schemaDocument); err != nil {
		return domain.ErrToolCatalogValidation.Extend("tool input schema is invalid")
	}
	if _, err := compiler.Compile("tool_input_schema.json"); err != nil {
		return domain.ErrToolCatalogValidation.Extend("tool input schema is invalid")
	}
	return nil
}
