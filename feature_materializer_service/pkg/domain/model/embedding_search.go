package model

type EmbeddingSearchResult struct {
	EmbeddingSnapshot *EmbeddingSnapshot
	Matches           []EmbeddingRecord
	Disclosure        RetrievalDisclosure
}

type EmbeddingRecordSearchResult struct {
	Records    []EmbeddingRecord
	Disclosure RetrievalDisclosure
}
