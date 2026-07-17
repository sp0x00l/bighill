package tools

import (
	"context"
	"encoding/json"
	"fmt"

	"inference_service/pkg/app"
	"inference_service/pkg/domain"
	"inference_service/pkg/domain/model"

	"github.com/go-playground/validator/v10"
	log "github.com/sirupsen/logrus"
)

const (
	GraphSearchToolName        = "graph_search"
	graphSearchToolName        = GraphSearchToolName
	graphSearchToolImplVersion = "graph_search_v1"
)

type graphSearchArgsDTO struct {
	QueryText string `json:"query_text" validate:"required"`
	TopK      int    `json:"top_k" validate:"required,min=1,max=50"`
	MaxHops   int    `json:"max_hops" validate:"required,min=1,max=5"`
}

type graphSearchArgs struct {
	QueryText string
	TopK      int
	MaxHops   int
}

type graphSearchArgsDTOAdapter struct {
	validator *validator.Validate
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
	return graphSearchArgs{
		QueryText: dto.QueryText,
		TopK:      dto.TopK,
		MaxHops:   dto.MaxHops,
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
			Description:           "Search the endpoint's materialized entity graph and return multi-hop grounded context chunks.",
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
		matches, err := i.retrievalClient.SearchGraph(ctx, invocation.UserID, dataset.DatasetID, args.QueryText, args.TopK, args.MaxHops)
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
		Contexts []model.RetrievedContext `json:"contexts"`
	}{
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
    "max_hops": { "type": "integer", "minimum": 1, "maximum": 5 }
  },
  "required": ["query_text", "top_k", "max_hops"]
}`
