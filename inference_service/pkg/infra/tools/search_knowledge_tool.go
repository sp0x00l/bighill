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
	SearchKnowledgeToolName        = "search_knowledge"
	searchKnowledgeToolName        = SearchKnowledgeToolName
	searchKnowledgeToolImplVersion = "search_knowledge_v1"
)

type searchKnowledgeArgsDTO struct {
	QueryText       string            `json:"query_text" validate:"required"`
	TopK            int               `json:"top_k" validate:"required,min=1,max=50"`
	MetadataFilters map[string]string `json:"metadata_filters" validate:"omitempty"`
}

type searchKnowledgeArgs struct {
	QueryText       string
	TopK            int
	MetadataFilters map[string]string
}

type searchKnowledgeArgsDTOAdapter struct {
	validator *validator.Validate
}

func newSearchKnowledgeArgsDTOAdapter(validator *validator.Validate) *searchKnowledgeArgsDTOAdapter {
	log.Trace("newSearchKnowledgeArgsDTOAdapter")

	return &searchKnowledgeArgsDTOAdapter{validator: validator}
}

func (a *searchKnowledgeArgsDTOAdapter) FromDTO(arguments []byte) (searchKnowledgeArgs, error) {
	log.Trace("searchKnowledgeArgsDTOAdapter FromDTO")

	dto := searchKnowledgeArgsDTO{}
	if err := json.Unmarshal(arguments, &dto); err != nil {
		return searchKnowledgeArgs{}, domain.ErrValidationFailed.Extend("search_knowledge arguments must be valid JSON")
	}
	if err := a.validator.Struct(dto); err != nil {
		return searchKnowledgeArgs{}, domain.ErrValidationFailed.Extend(fmt.Sprintf("search_knowledge arguments are invalid: %v", err))
	}
	return searchKnowledgeArgs{
		QueryText:       dto.QueryText,
		TopK:            dto.TopK,
		MetadataFilters: dto.MetadataFilters,
	}, nil
}

type SearchKnowledgeToolInvoker struct {
	retrievalClient app.RetrievalClient
	adapter         *searchKnowledgeArgsDTOAdapter
}

func NewSearchKnowledgeToolInvoker(retrievalClient app.RetrievalClient) (*SearchKnowledgeToolInvoker, error) {
	log.Trace("NewSearchKnowledgeToolInvoker")

	if retrievalClient == nil {
		return nil, domain.ErrValidationFailed.Extend("retrieval client is required")
	}
	return &SearchKnowledgeToolInvoker{
		retrievalClient: retrievalClient,
		adapter:         newSearchKnowledgeArgsDTOAdapter(validator.New()),
	}, nil
}

func (i *SearchKnowledgeToolInvoker) Available(ctx context.Context, session *model.AgentSession, bindings []model.ToolBinding) ([]model.ToolSpec, error) {
	log.Trace("SearchKnowledgeToolInvoker Available")

	_ = ctx
	_ = session
	specs := make([]model.ToolSpec, 0, len(bindings))
	for _, binding := range bindings {
		if binding.Name != searchKnowledgeToolName {
			return nil, domain.ErrValidationFailed.Extend("unknown agent tool binding")
		}
		specs = append(specs, model.ToolSpec{
			Name:        searchKnowledgeToolName,
			Description: "Search the endpoint's materialized knowledge datasets and return grounded context chunks.",
			Parameters:  json.RawMessage(searchKnowledgeParametersSchema),
		})
	}
	return specs, nil
}

func (i *SearchKnowledgeToolInvoker) Invoke(ctx context.Context, session *model.AgentSession, call model.ToolCall) (model.ToolResult, error) {
	log.Trace("SearchKnowledgeToolInvoker Invoke")

	if call.Name != searchKnowledgeToolName {
		return model.ToolResult{
			CallID:          call.ID,
			Name:            call.Name,
			Content:         "tool is not allowed",
			IsError:         true,
			ErrorType:       model.ToolErrorTypePolicyDenied,
			ToolImplVersion: searchKnowledgeToolImplVersion,
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
			ToolImplVersion: searchKnowledgeToolImplVersion,
		}, err
	}
	contexts := make([]model.RetrievedContext, 0, len(session.Datasets)*args.TopK)
	for _, dataset := range session.Datasets {
		matches, err := i.retrievalClient.SearchEmbeddings(ctx, session.UserID, dataset.DatasetID, args.QueryText, args.TopK, args.MetadataFilters)
		if err != nil {
			return model.ToolResult{
				CallID:          call.ID,
				Name:            call.Name,
				Content:         err.Error(),
				IsError:         true,
				ErrorType:       model.ToolErrorTypeTransient,
				ToolImplVersion: searchKnowledgeToolImplVersion,
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
		return model.ToolResult{}, fmt.Errorf("marshal search_knowledge result: %w", err)
	}
	return model.ToolResult{
		CallID:          call.ID,
		Name:            call.Name,
		Content:         string(payload),
		ToolImplVersion: searchKnowledgeToolImplVersion,
	}, nil
}

const searchKnowledgeParametersSchema = `{
  "type": "object",
  "additionalProperties": false,
  "properties": {
    "query_text": { "type": "string", "minLength": 1 },
    "top_k": { "type": "integer", "minimum": 1, "maximum": 50 },
    "metadata_filters": {
      "type": "object",
      "additionalProperties": { "type": "string" }
    }
  },
  "required": ["query_text", "top_k"]
}`
