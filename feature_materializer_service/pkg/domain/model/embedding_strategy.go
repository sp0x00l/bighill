package model

import (
	"fmt"
	"strings"
)

const (
	DefaultEmbeddingStrategyVersion = "rag-v1"
	DefaultExtractorName            = "go-document-extractor-suite"
	DefaultExtractorVersion         = "v1"
	DefaultCleanerName              = "go-basic-text-cleaner"
	DefaultCleanerVersion           = "v1"
	DefaultChunkerName              = "go-token-window"
	DefaultChunkerVersion           = "v1"
	DefaultChunkSize                = 384
	DefaultChunkOverlap             = 64
	DefaultEmbeddingProvider        = "deterministic"
	DefaultEmbeddingModel           = "bge-small-en-v1.5"
	DefaultEmbeddingDimensions      = 384
)

type EmbeddingStrategy struct {
	StrategyVersion     string
	ExtractorName       string
	ExtractorVersion    string
	CleanerName         string
	CleanerVersion      string
	ChunkerName         string
	ChunkerVersion      string
	ChunkSize           int
	ChunkOverlap        int
	EmbeddingProvider   string
	EmbeddingModel      string
	EmbeddingDimensions int
}

func NormalizeEmbeddingStrategy(strategy EmbeddingStrategy) EmbeddingStrategy {
	strategy.StrategyVersion = strings.TrimSpace(strategy.StrategyVersion)
	strategy.ExtractorName = strings.TrimSpace(strategy.ExtractorName)
	strategy.ExtractorVersion = strings.TrimSpace(strategy.ExtractorVersion)
	strategy.CleanerName = strings.TrimSpace(strategy.CleanerName)
	strategy.CleanerVersion = strings.TrimSpace(strategy.CleanerVersion)
	strategy.ChunkerName = strings.TrimSpace(strategy.ChunkerName)
	strategy.ChunkerVersion = strings.TrimSpace(strategy.ChunkerVersion)
	strategy.EmbeddingProvider = strings.ToLower(strings.TrimSpace(strategy.EmbeddingProvider))
	strategy.EmbeddingModel = strings.TrimSpace(strategy.EmbeddingModel)

	if strategy.StrategyVersion == "" {
		strategy.StrategyVersion = DefaultEmbeddingStrategyVersion
	}
	if strategy.ExtractorName == "" {
		strategy.ExtractorName = DefaultExtractorName
	}
	if strategy.ExtractorVersion == "" {
		strategy.ExtractorVersion = DefaultExtractorVersion
	}
	if strategy.CleanerName == "" {
		strategy.CleanerName = DefaultCleanerName
	}
	if strategy.CleanerVersion == "" {
		strategy.CleanerVersion = DefaultCleanerVersion
	}
	if strategy.ChunkerName == "" {
		strategy.ChunkerName = DefaultChunkerName
	}
	if strategy.ChunkerVersion == "" {
		strategy.ChunkerVersion = DefaultChunkerVersion
	}
	if strategy.ChunkSize <= 0 {
		strategy.ChunkSize = DefaultChunkSize
	}
	if strategy.ChunkOverlap < 0 {
		strategy.ChunkOverlap = 0
	}
	if strategy.ChunkOverlap >= strategy.ChunkSize {
		strategy.ChunkOverlap = strategy.ChunkSize / 4
	}
	if strategy.EmbeddingProvider == "" {
		strategy.EmbeddingProvider = DefaultEmbeddingProvider
	}
	if strategy.EmbeddingModel == "" {
		strategy.EmbeddingModel = DefaultEmbeddingModel
	}
	if strategy.EmbeddingDimensions <= 0 {
		strategy.EmbeddingDimensions = DefaultEmbeddingDimensions
	}
	return strategy
}

func (s EmbeddingStrategy) CanonicalKey() string {
	s = NormalizeEmbeddingStrategy(s)
	return fmt.Sprintf(
		"strategy=%s|extractor=%s|extractor_version=%s|cleaner=%s|cleaner_version=%s|chunker=%s|chunker_version=%s|chunk_size=%d|chunk_overlap=%d|embedding_provider=%s|embedding_model=%s|embedding_dimensions=%d",
		s.StrategyVersion,
		s.ExtractorName,
		s.ExtractorVersion,
		s.CleanerName,
		s.CleanerVersion,
		s.ChunkerName,
		s.ChunkerVersion,
		s.ChunkSize,
		s.ChunkOverlap,
		s.EmbeddingProvider,
		s.EmbeddingModel,
		s.EmbeddingDimensions,
	)
}
