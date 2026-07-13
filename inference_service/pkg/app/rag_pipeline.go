package app

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"inference_service/pkg/domain/model"

	tiktoken "github.com/pkoukk/tiktoken-go"
	tiktokenloader "github.com/pkoukk/tiktoken-go-loader"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
)

const (
	promptEncodingName            = "cl100k_base"
	promptSectionDataset          = "\n\nDataset:\n"
	promptSectionModel            = "\nModel:\n"
	promptSectionRetrievedContext = "\nRetrieved context:\n"
	promptContextHeaderFormat     = "[context:%d record_id:%s snapshot_id:%s chunk:%d similarity:%.6f]\n"
	promptSectionQuestion         = "Question:\n"
	promptSectionAnswer           = "\n\nAnswer:"
)

var (
	promptEncodingOnce sync.Once
	promptEncoding     *tiktoken.Tiktoken
	promptEncodingErr  error
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

func (p *ContextWindowPacker) Pack(ctx context.Context, request model.ContextPackRequest) (packed []model.RetrievedContext, err error) {
	log.Trace("ContextWindowPacker Pack")

	ctx, span := startInferenceSpan(ctx, "generate.pack_context_window",
		attribute.Int("candidate_count", len(request.Contexts)),
		attribute.Int("max_context_chunks", p.strategy.MaxContextChunks),
		attribute.Int("max_context_tokens", p.strategy.MaxContextTokens),
	)
	defer endInferenceSpanOnReturn(ctx, span, &err)

	strategy := p.strategy
	encoding, err := loadPromptEncoding()
	if err != nil {
		return nil, err
	}
	packed = make([]model.RetrievedContext, 0, min(len(request.Contexts), strategy.MaxContextChunks))
	remainingTokens := strategy.MaxContextTokens
	for _, retrieved := range request.Contexts {
		if len(packed) >= strategy.MaxContextChunks || remainingTokens <= 0 {
			break
		}
		sourceText := strings.TrimSpace(retrieved.SourceText)
		if sourceText == "" {
			continue
		}
		tokens := encoding.Encode(sourceText, nil, nil)
		if len(tokens) > remainingTokens {
			tokens = tokens[:remainingTokens]
			sourceText = strings.TrimSpace(encoding.Decode(tokens))
		}
		if sourceText == "" {
			continue
		}
		retrieved.SourceText = sourceText
		packed = append(packed, retrieved)
		remainingTokens -= len(tokens)
	}
	return packed, nil
}

func loadPromptEncoding() (*tiktoken.Tiktoken, error) {
	log.Trace("loadPromptEncoding")

	promptEncodingOnce.Do(func() {
		tiktoken.SetBpeLoader(tiktokenloader.NewOfflineLoader())
		promptEncoding, promptEncodingErr = tiktoken.GetEncoding(promptEncodingName)
		if promptEncodingErr != nil {
			promptEncodingErr = fmt.Errorf("get prompt tokenizer encoding %s: %w", promptEncodingName, promptEncodingErr)
		}
	})
	return promptEncoding, promptEncodingErr
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

func (b *DefaultPromptBuilder) BuildPrompt(ctx context.Context, request model.PromptBuildRequest) (promptPackage *model.PromptPackage, err error) {
	log.Trace("DefaultPromptBuilder BuildPrompt")

	ctx, span := startInferenceSpan(ctx, "generate.build_prompt",
		attribute.String("dataset_id", request.Dataset.DatasetID.String()),
		attribute.String("model_id", request.Model.ModelID.String()),
		attribute.Int("context_count", len(request.Contexts)),
		attribute.String("prompt_strategy_version", b.strategy.Version),
	)
	defer endInferenceSpanOnReturn(ctx, span, &err)

	query := strings.TrimSpace(request.Query)
	strategy := b.strategy
	var prompt strings.Builder
	prompt.WriteString(strategy.SystemPrompt)
	prompt.WriteString(promptSectionDataset)
	prompt.WriteString(fmt.Sprintf("- dataset_id: %s\n", request.Dataset.DatasetID))
	prompt.WriteString(fmt.Sprintf("- version: %d\n", request.Dataset.DatasetVersion))
	prompt.WriteString(fmt.Sprintf("- embedding_snapshot_id: %s\n", request.Dataset.EmbeddingSnapshotID))
	prompt.WriteString(promptSectionModel)
	prompt.WriteString(fmt.Sprintf("- model_id: %s\n", request.Model.ModelID))
	prompt.WriteString(fmt.Sprintf("- model_version: %d\n", request.Model.ModelVersion))
	prompt.WriteString(promptSectionRetrievedContext)
	for i, retrieved := range request.Contexts {
		prompt.WriteString(fmt.Sprintf(promptContextHeaderFormat,
			i+1,
			retrieved.EmbeddingRecordID,
			retrieved.EmbeddingSnapshotID,
			retrieved.ChunkIndex,
			retrieved.Similarity,
		))
		prompt.WriteString(strings.TrimSpace(retrieved.SourceText))
		prompt.WriteString("\n\n")
	}
	prompt.WriteString(promptSectionQuestion)
	prompt.WriteString(query)
	prompt.WriteString(promptSectionAnswer)

	return &model.PromptPackage{
		Prompt:   prompt.String(),
		Strategy: strategy,
		Contexts: request.Contexts,
	}, nil
}
