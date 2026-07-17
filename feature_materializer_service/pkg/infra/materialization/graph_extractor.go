package materialization

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"feature_materializer_service/pkg/domain"
	"feature_materializer_service/pkg/domain/model"
	contractschemas "lib/data_contracts_lib/schemas"

	log "github.com/sirupsen/logrus"
)

var entityTokenPattern = regexp.MustCompile(`\b[A-Z][A-Za-z0-9]*(?:\s+[A-Z][A-Za-z0-9]*){0,3}\b`)

type LocalGraphExtractor struct {
}

type DisabledGraphExtractor struct {
}

type graphExtractionDocument struct {
	Entities  []graphExtractionEntityDTO   `json:"entities"`
	Relations []graphExtractionRelationDTO `json:"relations"`
}

type graphExtractionEntityDTO struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Type        string `json:"type"`
	Description string `json:"description"`
}

type graphExtractionRelationDTO struct {
	Source      string  `json:"source"`
	Target      string  `json:"target"`
	Type        string  `json:"type"`
	Description string  `json:"description"`
	Weight      float64 `json:"weight"`
}

func NewLocalGraphExtractor() (*LocalGraphExtractor, error) {
	log.Trace("NewLocalGraphExtractor")

	if !json.Valid(contractschemas.GraphExtractionV1Schema()) {
		return nil, fmt.Errorf("graph extraction schema is invalid JSON")
	}
	return &LocalGraphExtractor{}, nil
}

func NewDisabledGraphExtractor() *DisabledGraphExtractor {
	log.Trace("NewDisabledGraphExtractor")

	return &DisabledGraphExtractor{}
}

func (e *DisabledGraphExtractor) ExtractGraph(context.Context, []model.GraphChunk, model.GraphExtractionStrategy) (*model.GraphExtraction, error) {
	log.Trace("DisabledGraphExtractor ExtractGraph")

	return nil, domain.ErrGraphMaterialize.Extend("graph extraction is disabled")
}

func (e *LocalGraphExtractor) ExtractGraph(ctx context.Context, chunks []model.GraphChunk, strategy model.GraphExtractionStrategy) (*model.GraphExtraction, error) {
	log.Trace("LocalGraphExtractor ExtractGraph")

	_ = ctx
	_ = strategy
	extraction := &model.GraphExtraction{}
	for _, chunk := range chunks {
		names := orderedUniqueEntityNames(chunk.SourceText)
		for _, name := range names {
			extraction.Entities = append(extraction.Entities, model.GraphExtractionEntity{
				ID:          graphExtractionEntityID(name),
				Name:        name,
				Type:        graphEntityType(name),
				Description: "Mentioned in source chunk.",
				ChunkIndex:  chunk.ChunkIndex,
			})
		}
		for i := 0; i+1 < len(names); i++ {
			extraction.Relations = append(extraction.Relations, model.GraphExtractionRelation{
				Source:      graphExtractionEntityID(names[i]),
				Target:      graphExtractionEntityID(names[i+1]),
				Type:        "RELATED_TO",
				Description: "Co-mentioned in source chunk.",
				Weight:      1,
			})
		}
	}
	if err := e.validate(extraction); err != nil {
		return nil, err
	}
	return extraction, nil
}

func (e *LocalGraphExtractor) validate(extraction *model.GraphExtraction) error {
	log.Trace("LocalGraphExtractor validate")

	document := graphExtractionDocument{
		Entities:  make([]graphExtractionEntityDTO, 0, len(extraction.Entities)),
		Relations: make([]graphExtractionRelationDTO, 0, len(extraction.Relations)),
	}
	for _, entity := range extraction.Entities {
		document.Entities = append(document.Entities, graphExtractionEntityDTO{
			ID:          strings.TrimSpace(entity.ID),
			Name:        strings.TrimSpace(entity.Name),
			Type:        strings.TrimSpace(entity.Type),
			Description: strings.TrimSpace(entity.Description),
		})
	}
	for _, relation := range extraction.Relations {
		document.Relations = append(document.Relations, graphExtractionRelationDTO{
			Source:      strings.TrimSpace(relation.Source),
			Target:      strings.TrimSpace(relation.Target),
			Type:        strings.TrimSpace(relation.Type),
			Description: strings.TrimSpace(relation.Description),
			Weight:      relation.Weight,
		})
	}
	if err := validateGraphExtractionDocument(document); err != nil {
		return domain.ErrGraphMaterialize.Extend(err.Error())
	}
	return nil
}

func orderedUniqueEntityNames(text string) []string {
	log.Trace("orderedUniqueEntityNames")

	matches := entityTokenPattern.FindAllString(text, -1)
	out := make([]string, 0, len(matches))
	seen := map[string]struct{}{}
	for _, match := range matches {
		name := strings.TrimSpace(match)
		if len(name) <= 1 {
			continue
		}
		lower := strings.ToLower(name)
		switch lower {
		case "the", "this", "that", "it", "a", "an":
			continue
		}
		if _, ok := seen[lower]; ok {
			continue
		}
		seen[lower] = struct{}{}
		out = append(out, name)
	}
	return out
}

func graphExtractionEntityID(name string) string {
	log.Trace("graphExtractionEntityID")

	return strings.ToLower(strings.Join(strings.Fields(strings.TrimSpace(name)), "_"))
}

func graphEntityType(name string) string {
	log.Trace("graphEntityType")

	if strings.Contains(strings.ToLower(name), "inc") || strings.Contains(strings.ToLower(name), "corp") {
		return "organization"
	}
	return "entity"
}

func validateGraphExtractionDocument(document graphExtractionDocument) error {
	log.Trace("validateGraphExtractionDocument")

	for idx, entity := range document.Entities {
		if strings.TrimSpace(entity.ID) == "" {
			return fmt.Errorf("entities[%d].id is required", idx)
		}
		if strings.TrimSpace(entity.Name) == "" {
			return fmt.Errorf("entities[%d].name is required", idx)
		}
		if strings.TrimSpace(entity.Type) == "" {
			return fmt.Errorf("entities[%d].type is required", idx)
		}
		if strings.TrimSpace(entity.Description) == "" {
			return fmt.Errorf("entities[%d].description is required", idx)
		}
	}
	for idx, relation := range document.Relations {
		if strings.TrimSpace(relation.Source) == "" {
			return fmt.Errorf("relations[%d].source is required", idx)
		}
		if strings.TrimSpace(relation.Target) == "" {
			return fmt.Errorf("relations[%d].target is required", idx)
		}
		if strings.TrimSpace(relation.Type) == "" {
			return fmt.Errorf("relations[%d].type is required", idx)
		}
		if strings.TrimSpace(relation.Description) == "" {
			return fmt.Errorf("relations[%d].description is required", idx)
		}
		if relation.Weight < 0 {
			return fmt.Errorf("relations[%d].weight must be non-negative", idx)
		}
	}
	return nil
}
