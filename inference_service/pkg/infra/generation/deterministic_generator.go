package generation

import (
	"context"
	"fmt"
	"strings"

	"inference_service/pkg/domain/model"

	log "github.com/sirupsen/logrus"
)

type DeterministicGenerator struct{}

func NewDeterministicGenerator() *DeterministicGenerator {
	log.Trace("NewDeterministicGenerator")

	return &DeterministicGenerator{}
}

func (g *DeterministicGenerator) Generate(_ context.Context, request model.GenerationRequest) (string, error) {
	log.Trace("DeterministicGenerator Generate")

	query := strings.TrimSpace(request.Query)
	if query == "" {
		return "", fmt.Errorf("query is required")
	}
	if len(request.Contexts) == 0 {
		return "No relevant context was found for the query.", nil
	}

	best := strings.TrimSpace(request.Contexts[0].SourceText)
	if best == "" {
		return fmt.Sprintf("Retrieved %d context chunks, but the highest ranked chunk was empty.", len(request.Contexts)), nil
	}
	return fmt.Sprintf("Based on the retrieved context: %s", best), nil
}
