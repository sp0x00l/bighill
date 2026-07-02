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
		return "", fmt.Errorf("retrieved context is required")
	}

	best := strings.TrimSpace(request.Contexts[0].SourceText)
	if best == "" {
		return "", fmt.Errorf("highest ranked retrieved context is empty")
	}
	return fmt.Sprintf("Based on the retrieved context: %s", best), nil
}

func (g *DeterministicGenerator) Provider() string {
	log.Trace("DeterministicGenerator Provider")

	return "deterministic"
}

func (g *DeterministicGenerator) Model() string {
	log.Trace("DeterministicGenerator Model")

	return "deterministic"
}
