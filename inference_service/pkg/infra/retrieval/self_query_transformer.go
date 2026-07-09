package retrieval

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"inference_service/pkg/domain/model"

	log "github.com/sirupsen/logrus"
)

type QueryGenerator interface {
	Generate(ctx context.Context, request model.GenerationRequest) (string, error)
}

type SelfQueryTransformer struct {
	generators    map[string]QueryGenerator
	allowedFilter map[string]struct{}
}

func NewSelfQueryTransformer(generators map[string]QueryGenerator) *SelfQueryTransformer {
	log.Trace("NewSelfQueryTransformer")

	return &SelfQueryTransformer{
		generators:    generators,
		allowedFilter: defaultSelfQueryFilterAllowList(),
	}
}

func (t *SelfQueryTransformer) TransformQuery(ctx context.Context, request model.QueryTransformRequest) (*model.QueryTransformResult, error) {
	log.Trace("SelfQueryTransformer TransformQuery")

	if t == nil || len(t.generators) == 0 {
		return nil, fmt.Errorf("query transformer generators are required")
	}
	if request.Model == nil {
		return nil, fmt.Errorf("query transformer model is required")
	}
	protocol := strings.TrimSpace(request.Model.ServingProtocol.String())
	generator := t.generators[protocol]
	if generator == nil {
		return nil, fmt.Errorf("query transformer serving protocol %q is not supported", protocol)
	}
	prompt := selfQueryPrompt(request)
	raw, err := generator.Generate(ctx, model.GenerationRequest{
		RequestID: request.RequestID,
		Query:     request.QueryText,
		Prompt:    prompt,
		Model:     request.Model,
	})
	if err != nil {
		return nil, err
	}
	var parsed selfQueryResponse
	if err := json.Unmarshal([]byte(extractJSONObject(raw)), &parsed); err != nil {
		return nil, fmt.Errorf("decode self-query response: %w", err)
	}
	query := strings.TrimSpace(parsed.Query)
	if query == "" {
		return nil, fmt.Errorf("self-query response query is required")
	}
	filters := make(map[string]string, len(parsed.Filters))
	for key, value := range parsed.Filters {
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" || value == "" {
			return nil, fmt.Errorf("self-query response filters must be non-empty strings")
		}
		if _, ok := t.allowedFilter[key]; !ok {
			continue
		}
		filters[key] = value
	}
	return &model.QueryTransformResult{
		QueryText:       query,
		MetadataFilters: filters,
	}, nil
}

type selfQueryResponse struct {
	Query   string            `json:"query"`
	Filters map[string]string `json:"filters"`
}

func defaultSelfQueryFilterAllowList() map[string]struct{} {
	return map[string]struct{}{
		"source":          {},
		"source_format":   {},
		"section":         {},
		"section_path":    {},
		"heading":         {},
		"table_namespace": {},
		"table_name":      {},
	}
}

var fencedJSONPattern = regexp.MustCompile("(?s)```(?:json)?\\s*(\\{.*?\\})\\s*```")

func extractJSONObject(raw string) string {
	raw = strings.TrimSpace(raw)
	if match := fencedJSONPattern.FindStringSubmatch(raw); len(match) == 2 {
		return strings.TrimSpace(match[1])
	}
	start := strings.Index(raw, "{")
	end := strings.LastIndex(raw, "}")
	if start >= 0 && end > start {
		return strings.TrimSpace(raw[start : end+1])
	}
	return raw
}

func selfQueryPrompt(request model.QueryTransformRequest) string {
	log.Trace("selfQueryPrompt")

	return fmt.Sprintf(`Return only a JSON object for retrieval planning.
Schema: {"query":"semantic search query","filters":{"metadata_key":"metadata_value"}}
Allowed filter keys: source, source_format, section, section_path, heading, table_namespace, table_name.
Use filters only when the question explicitly names metadata constraints. Existing filters are already applied by the caller.
Question:
<query>
%s
</query>`, strings.TrimSpace(request.QueryText))
}
