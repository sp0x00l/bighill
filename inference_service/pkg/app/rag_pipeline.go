package app

import (
	"context"
	"fmt"
	"strings"

	"inference_service/pkg/domain/model"

	log "github.com/sirupsen/logrus"
)

type ContextWindowPacker struct {
	strategy model.PromptStrategy
}

func NewContextWindowPacker(strategy model.PromptStrategy) *ContextWindowPacker {
	log.Trace("NewContextWindowPacker")

	return &ContextWindowPacker{
		strategy: strategy,
	}
}

func (p *ContextWindowPacker) Pack(_ context.Context, request model.ContextPackRequest) ([]model.RetrievedContext, error) {
	log.Trace("ContextWindowPacker Pack")

	strategy := p.strategy
	packed := make([]model.RetrievedContext, 0, minInt(len(request.Contexts), strategy.MaxContextChunks))
	remainingChars := strategy.MaxContextChars
	for _, retrieved := range request.Contexts {
		if len(packed) >= strategy.MaxContextChunks || remainingChars <= 0 {
			break
		}
		sourceText := strings.TrimSpace(retrieved.SourceText)
		if sourceText == "" {
			continue
		}
		if len(sourceText) > remainingChars {
			sourceText = strings.TrimSpace(sourceText[:remainingChars])
		}
		if sourceText == "" {
			continue
		}
		retrieved.SourceText = sourceText
		packed = append(packed, retrieved)
		remainingChars -= len(sourceText)
	}
	return packed, nil
}

type DefaultPromptBuilder struct {
	strategy model.PromptStrategy
}

func NewDefaultPromptBuilder(strategy model.PromptStrategy) *DefaultPromptBuilder {
	log.Trace("NewDefaultPromptBuilder")

	return &DefaultPromptBuilder{
		strategy: strategy,
	}
}

func (b *DefaultPromptBuilder) BuildPrompt(_ context.Context, request model.PromptBuildRequest) (*model.PromptPackage, error) {
	log.Trace("DefaultPromptBuilder BuildPrompt")

	query := strings.TrimSpace(request.Query)
	strategy := b.strategy
	var prompt strings.Builder
	prompt.WriteString(strategy.SystemPrompt)
	prompt.WriteString("\n\nDataset:\n")
	prompt.WriteString(fmt.Sprintf("- dataset_id: %s\n", request.Dataset.DatasetID))
	prompt.WriteString(fmt.Sprintf("- version: %d\n", request.Dataset.DatasetVersion))
	prompt.WriteString(fmt.Sprintf("- embedding_snapshot_id: %s\n", request.Dataset.EmbeddingSnapshotID))
	prompt.WriteString("\nModel:\n")
	prompt.WriteString(fmt.Sprintf("- model_id: %s\n", request.Model.ModelID))
	prompt.WriteString(fmt.Sprintf("- model_version: %d\n", request.Model.ModelVersion))
	prompt.WriteString("\nRetrieved context:\n")
	for i, retrieved := range request.Contexts {
		prompt.WriteString(fmt.Sprintf("[context:%d record_id:%s snapshot_id:%s chunk:%d similarity:%.6f]\n",
			i+1,
			retrieved.EmbeddingRecordID,
			retrieved.EmbeddingSnapshotID,
			retrieved.ChunkIndex,
			retrieved.Similarity,
		))
		prompt.WriteString(strings.TrimSpace(retrieved.SourceText))
		prompt.WriteString("\n\n")
	}
	prompt.WriteString("Question:\n")
	prompt.WriteString(query)
	prompt.WriteString("\n\nAnswer:")

	return &model.PromptPackage{
		Prompt:   prompt.String(),
		Strategy: strategy,
		Contexts: request.Contexts,
	}, nil
}

func minInt(a, b int) int {
	log.Trace("minInt")

	if a < b {
		return a
	}
	return b
}
