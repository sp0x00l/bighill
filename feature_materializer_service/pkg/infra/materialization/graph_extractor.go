package materialization

import (
	"context"
	"fmt"
	"strings"

	"feature_materializer_service/pkg/domain"
	"feature_materializer_service/pkg/domain/model"

	log "github.com/sirupsen/logrus"
)

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

func NewDisabledGraphExtractor() *DisabledGraphExtractor {
	log.Trace("NewDisabledGraphExtractor")

	return &DisabledGraphExtractor{}
}

func (e *DisabledGraphExtractor) ExtractGraph(context.Context, []model.GraphChunk, model.GraphExtractionStrategy) (*model.GraphExtraction, error) {
	log.Trace("DisabledGraphExtractor ExtractGraph")

	return nil, domain.ErrGraphMaterialize.Extend("graph extraction is disabled")
}

func validateGraphExtractionDocument(document graphExtractionDocument) error {
	log.Trace("validateGraphExtractionDocument")

	entityIDs := make(map[string]struct{}, len(document.Entities))
	entityIDList := make([]string, 0, len(document.Entities))
	for idx, entity := range document.Entities {
		entityID := strings.TrimSpace(entity.ID)
		if entityID == "" {
			return graphExtractionDocumentError("entities[%d].id is required", idx)
		}
		entityIDs[entityID] = struct{}{}
		entityIDList = append(entityIDList, entityID)
		if strings.TrimSpace(entity.Name) == "" {
			return graphExtractionDocumentError("entities[%d].name is required", idx)
		}
		if strings.TrimSpace(entity.Type) == "" {
			return graphExtractionDocumentError("entities[%d].type is required", idx)
		}
		if strings.TrimSpace(entity.Description) == "" {
			return graphExtractionDocumentError("entities[%d].description is required", idx)
		}
	}
	for idx, relation := range document.Relations {
		if strings.TrimSpace(relation.Source) == "" {
			return graphExtractionDocumentError("relations[%d].source is required", idx)
		}
		if _, ok := entityIDs[strings.TrimSpace(relation.Source)]; !ok {
			return graphExtractionDocumentError("relations[%d].source must reference an entity id: %q available=%q", idx, strings.TrimSpace(relation.Source), strings.Join(entityIDList, ","))
		}
		if strings.TrimSpace(relation.Target) == "" {
			return graphExtractionDocumentError("relations[%d].target is required", idx)
		}
		if _, ok := entityIDs[strings.TrimSpace(relation.Target)]; !ok {
			return graphExtractionDocumentError("relations[%d].target must reference an entity id: %q available=%q", idx, strings.TrimSpace(relation.Target), strings.Join(entityIDList, ","))
		}
		if strings.TrimSpace(relation.Type) == "" {
			return graphExtractionDocumentError("relations[%d].type is required", idx)
		}
		if strings.TrimSpace(relation.Description) == "" {
			return graphExtractionDocumentError("relations[%d].description is required", idx)
		}
		if relation.Weight < 0 {
			return graphExtractionDocumentError("relations[%d].weight must be non-negative", idx)
		}
	}
	return nil
}

func graphExtractionDocumentError(format string, args ...any) error {
	log.Trace("graphExtractionDocumentError")

	return domain.ErrGraphExtractionInvalid.Extend(fmt.Sprintf(format, args...))
}

func canonicalizeGraphExtractionDocument(document graphExtractionDocument, sourceText string) graphExtractionDocument {
	log.Trace("canonicalizeGraphExtractionDocument")

	lookup, ambiguous := graphExtractionEntityLookup(document.Entities)
	for idx := range document.Relations {
		document.Relations[idx].Source = canonicalGraphEndpoint(document.Relations[idx].Source, &document, lookup, ambiguous, sourceText)
		document.Relations[idx].Target = canonicalGraphEndpoint(document.Relations[idx].Target, &document, lookup, ambiguous, sourceText)
	}
	return document
}

func graphExtractionEntityLookup(entities []graphExtractionEntityDTO) (map[string]string, map[string]struct{}) {
	log.Trace("graphExtractionEntityLookup")

	lookup := make(map[string]string, len(entities)*2)
	ambiguous := make(map[string]struct{})
	for _, entity := range entities {
		entityID := strings.TrimSpace(entity.ID)
		if entityID == "" {
			continue
		}
		addGraphEndpointAlias(lookup, ambiguous, entityID, entityID)
		addGraphEndpointAlias(lookup, ambiguous, entity.Name, entityID)
	}
	return lookup, ambiguous
}

func addGraphEndpointAlias(lookup map[string]string, ambiguous map[string]struct{}, alias string, entityID string) {
	log.Trace("addGraphEndpointAlias")

	key := normalizeGraphEndpoint(alias)
	if key == "" {
		return
	}
	if existing, ok := lookup[key]; ok && existing != entityID {
		delete(lookup, key)
		ambiguous[key] = struct{}{}
		return
	}
	if _, ok := ambiguous[key]; ok {
		return
	}
	lookup[key] = entityID
}

func canonicalGraphEndpoint(value string, document *graphExtractionDocument, lookup map[string]string, ambiguous map[string]struct{}, sourceText string) string {
	log.Trace("canonicalGraphEndpoint")

	trimmed := strings.TrimSpace(value)
	key := normalizeGraphEndpoint(trimmed)
	if _, ok := ambiguous[key]; ok {
		return trimmed
	}
	if entityID, ok := lookup[key]; ok {
		return entityID
	}
	if endpointMentionedInSource(trimmed, sourceText) {
		entityID := graphEndpointEntityID(trimmed)
		document.Entities = append(document.Entities, graphExtractionEntityDTO{
			ID:          entityID,
			Name:        trimmed,
			Type:        "other",
			Description: "Mentioned in source chunk.",
		})
		addGraphEndpointAlias(lookup, ambiguous, entityID, entityID)
		addGraphEndpointAlias(lookup, ambiguous, trimmed, entityID)
		return entityID
	}
	return trimmed
}

func endpointMentionedInSource(endpoint string, sourceText string) bool {
	log.Trace("endpointMentionedInSource")

	endpointKey := normalizeGraphEndpoint(endpoint)
	if endpointKey == "" {
		return false
	}
	return strings.Contains(normalizeGraphEndpoint(sourceText), endpointKey)
}

func graphEndpointEntityID(value string) string {
	log.Trace("graphEndpointEntityID")

	parts := make([]string, 0)
	var current strings.Builder
	for _, r := range strings.ToLower(strings.TrimSpace(value)) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			current.WriteRune(r)
			continue
		}
		if current.Len() > 0 {
			parts = append(parts, current.String())
			current.Reset()
		}
	}
	if current.Len() > 0 {
		parts = append(parts, current.String())
	}
	if len(parts) == 0 {
		return "entity"
	}
	return strings.Join(parts, "_")
}

func normalizeGraphEndpoint(value string) string {
	log.Trace("normalizeGraphEndpoint")

	var out strings.Builder
	for _, r := range strings.ToLower(strings.TrimSpace(value)) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			out.WriteRune(r)
		}
	}
	return out.String()
}
