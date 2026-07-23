package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"inference_service/pkg/app"
	"inference_service/pkg/domain"
	"inference_service/pkg/domain/model"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

const (
	GraphSearchToolName        = "graph_search"
	graphSearchToolName        = GraphSearchToolName
	graphSearchToolImplVersion = "graph_search_v1"
	graphSearchModeLocal       = "local"
	graphSearchModeGlobal      = "global"
)

type graphSearchArgsDTO struct {
	QueryText string `json:"query_text" validate:"required"`
	TopK      int    `json:"top_k" validate:"required,min=1,max=50"`
	MaxHops   int    `json:"max_hops" validate:"required,min=1,max=5"`
	Mode      string `json:"mode,omitempty" validate:"omitempty,oneof=local global"`
}

type graphSearchArgs struct {
	QueryText string
	TopK      int
	MaxHops   int
	Mode      string
}

type graphSearchArgsDTOAdapter struct {
	validator *validator.Validate
}

type graphSearchModeRetrievalClient interface {
	SearchGraphWithMode(ctx context.Context, userID uuid.UUID, datasetID uuid.UUID, queryText string, topK int, maxHops int, mode string) ([]model.RetrievedContext, error)
}

func newGraphSearchArgsDTOAdapter(validator *validator.Validate) *graphSearchArgsDTOAdapter {
	log.Trace("newGraphSearchArgsDTOAdapter")

	return &graphSearchArgsDTOAdapter{validator: validator}
}

func (a *graphSearchArgsDTOAdapter) FromDTO(arguments []byte) (graphSearchArgs, error) {
	log.Trace("graphSearchArgsDTOAdapter FromDTO")

	dto := graphSearchArgsDTO{}
	if err := json.Unmarshal(arguments, &dto); err != nil {
		return graphSearchArgs{}, domain.ErrValidationFailed.Extend("graph_search arguments must be valid JSON")
	}
	if err := a.validator.Struct(dto); err != nil {
		return graphSearchArgs{}, domain.ErrValidationFailed.Extend(fmt.Sprintf("graph_search arguments are invalid: %v", err))
	}
	mode := strings.ToLower(strings.TrimSpace(dto.Mode))
	if mode == "" {
		mode = graphSearchModeLocal
	}
	return graphSearchArgs{
		QueryText: dto.QueryText,
		TopK:      dto.TopK,
		MaxHops:   dto.MaxHops,
		Mode:      mode,
	}, nil
}

type GraphSearchToolInvoker struct {
	retrievalClient app.RetrievalClient
	adapter         *graphSearchArgsDTOAdapter
}

func NewGraphSearchToolInvoker(retrievalClient app.RetrievalClient) (*GraphSearchToolInvoker, error) {
	log.Trace("NewGraphSearchToolInvoker")

	if retrievalClient == nil {
		return nil, domain.ErrValidationFailed.Extend("retrieval client is required")
	}
	return &GraphSearchToolInvoker{
		retrievalClient: retrievalClient,
		adapter:         newGraphSearchArgsDTOAdapter(validator.New()),
	}, nil
}

func (i *GraphSearchToolInvoker) Available(ctx context.Context, resolution app.ToolResolutionContext, bindings []model.ToolBinding) ([]model.ToolSpec, error) {
	log.Trace("GraphSearchToolInvoker Available")

	_ = ctx
	specs := make([]model.ToolSpec, 0, len(bindings))
	for _, binding := range bindings {
		if binding.Name != graphSearchToolName {
			return nil, domain.ErrValidationFailed.Extend("unknown agent tool binding")
		}
		if resolution.Datasets != nil && len(graphReadyDatasets(resolution.Datasets)) == 0 {
			continue
		}
		specs = append(specs, model.ToolSpec{
			Name:                  graphSearchToolName,
			Description:           "Search the endpoint's materialized entity graph. Use local mode for multi-hop grounded chunks and global mode for connected-component groupings with extractive summaries.",
			Parameters:            json.RawMessage(graphSearchParametersSchema),
			ImplementationVersion: graphSearchToolImplVersion,
			Locality:              "local",
		})
	}
	return specs, nil
}

func (i *GraphSearchToolInvoker) Invoke(ctx context.Context, invocation app.ToolInvocationContext, call model.ToolCall) (model.ToolResult, error) {
	log.Trace("GraphSearchToolInvoker Invoke")

	if call.Name != graphSearchToolName {
		return model.ToolResult{
			CallID:          call.ID,
			Name:            call.Name,
			Content:         "tool is not allowed",
			IsError:         true,
			ErrorType:       model.ToolErrorTypePolicyDenied,
			ToolImplVersion: graphSearchToolImplVersion,
		}, domain.ErrValidationFailed.Extend("tool is not allowed")
	}
	args, err := i.adapter.FromDTO(call.Arguments)
	if err != nil {
		return model.ToolResult{
			CallID:          call.ID,
			Name:            call.Name,
			Content:         err.Error(),
			IsError:         true,
			ErrorType:       model.ToolErrorTypePolicyDenied,
			ToolImplVersion: graphSearchToolImplVersion,
		}, err
	}
	datasets := graphReadyDatasets(invocation.Datasets)
	if len(datasets) == 0 {
		err := domain.ErrValidationFailed.Extend("graph_search is not available for this endpoint")
		return model.ToolResult{
			CallID:          call.ID,
			Name:            call.Name,
			Content:         err.Error(),
			IsError:         true,
			ErrorType:       model.ToolErrorTypePolicyDenied,
			ToolImplVersion: graphSearchToolImplVersion,
		}, err
	}
	contexts := make([]model.RetrievedContext, 0, len(datasets)*args.TopK)
	for _, dataset := range datasets {
		matches, err := i.searchGraph(ctx, invocation.UserID, dataset.DatasetID, args)
		if err != nil {
			return model.ToolResult{
				CallID:          call.ID,
				Name:            call.Name,
				Content:         err.Error(),
				IsError:         true,
				ErrorType:       model.ToolErrorTypeTransient,
				ToolImplVersion: graphSearchToolImplVersion,
			}, err
		}
		contexts = append(contexts, matches...)
	}
	payload, err := json.Marshal(struct {
		Mode     string                   `json:"mode"`
		Contexts []model.RetrievedContext `json:"contexts"`
	}{
		Mode:     args.Mode,
		Contexts: contexts,
	})
	if err != nil {
		return model.ToolResult{}, fmt.Errorf("marshal graph_search result: %w", err)
	}
	return model.ToolResult{
		CallID:          call.ID,
		Name:            call.Name,
		Content:         string(payload),
		Contexts:        contexts,
		ToolImplVersion: graphSearchToolImplVersion,
	}, nil
}

func (i *GraphSearchToolInvoker) searchGraph(ctx context.Context, userID uuid.UUID, datasetID uuid.UUID, args graphSearchArgs) ([]model.RetrievedContext, error) {
	log.Trace("GraphSearchToolInvoker searchGraph")

	if args.Mode == graphSearchModeLocal {
		return i.retrievalClient.SearchGraph(ctx, userID, datasetID, args.QueryText, args.TopK, args.MaxHops)
	}
	modeClient, ok := i.retrievalClient.(graphSearchModeRetrievalClient)
	if !ok {
		return nil, domain.ErrValidationFailed.Extend("graph_search global mode is not supported by retrieval client")
	}
	return modeClient.SearchGraphWithMode(ctx, userID, datasetID, args.QueryText, args.TopK, args.MaxHops, args.Mode)
}

func graphReadyDatasets(datasets []*model.InferenceDataset) []*model.InferenceDataset {
	log.Trace("graphReadyDatasets")

	out := make([]*model.InferenceDataset, 0, len(datasets))
	for _, dataset := range datasets {
		if dataset == nil || !dataset.IsGraphReady() {
			continue
		}
		out = append(out, dataset)
	}
	return out
}

const graphSearchParametersSchema = `{
  "type": "object",
  "additionalProperties": false,
  "properties": {
    "query_text": { "type": "string", "minLength": 1 },
    "top_k": { "type": "integer", "minimum": 1, "maximum": 50 },
    "max_hops": { "type": "integer", "minimum": 1, "maximum": 5 },
    "mode": { "type": "string", "enum": ["local", "global"], "default": "local" }
  },
  "required": ["query_text", "top_k", "max_hops"]
}`
