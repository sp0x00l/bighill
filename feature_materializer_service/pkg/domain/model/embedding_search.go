package model

type EmbeddingSearchResult struct {
	EmbeddingSnapshot *EmbeddingSnapshot
	Matches           []EmbeddingRecord
}
