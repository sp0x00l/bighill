package model

import "github.com/google/uuid"

type GenerateRequest struct {
	DatasetID uuid.UUID
	ModelID   uuid.UUID
	QueryText string
	TopK      int
}

type GenerateResponse struct {
	DatasetID uuid.UUID
	ModelID   uuid.UUID
	QueryText string
	Answer    string
	Contexts  []RetrievedContext
}

type RetrievedContext struct {
	EmbeddingRecordID   uuid.UUID
	EmbeddingSnapshotID uuid.UUID
	ChunkIndex          int
	SourceText          string
	Distance            float64
	Similarity          float64
}

type GenerationRequest struct {
	Dataset  *InferenceDataset
	Model    *InferenceModel
	Query    string
	Contexts []RetrievedContext
}
