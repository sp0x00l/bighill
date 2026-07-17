package model

import "errors"

type ProcessingState int

const (
	DatasetProcessingPending ProcessingState = iota
	DatasetProcessingRawMaterialized
	DatasetProcessingFeatureMaterialized
	DatasetProcessingEmbeddingsMaterialized
	DatasetProcessingGraphMaterialized
	DatasetProcessingFailed
)

func (s ProcessingState) String() string {
	switch s {
	case DatasetProcessingPending:
		return "PENDING"
	case DatasetProcessingRawMaterialized:
		return "RAW_MATERIALIZED"
	case DatasetProcessingFeatureMaterialized:
		return "FEATURE_MATERIALIZED"
	case DatasetProcessingEmbeddingsMaterialized:
		return "EMBEDDINGS_MATERIALIZED"
	case DatasetProcessingGraphMaterialized:
		return "GRAPH_MATERIALIZED"
	case DatasetProcessingFailed:
		return "FAILED"
	default:
		return "PENDING"
	}
}

func ToProcessingState(s string) (ProcessingState, error) {
	switch s {
	case "", "PENDING":
		return DatasetProcessingPending, nil
	case "RAW_MATERIALIZED":
		return DatasetProcessingRawMaterialized, nil
	case "FEATURE_MATERIALIZED":
		return DatasetProcessingFeatureMaterialized, nil
	case "EMBEDDINGS_MATERIALIZED":
		return DatasetProcessingEmbeddingsMaterialized, nil
	case "GRAPH_MATERIALIZED":
		return DatasetProcessingGraphMaterialized, nil
	case "FAILED":
		return DatasetProcessingFailed, nil
	default:
		return 0, errors.New("invalid ProcessingState")
	}
}

func AdvanceProcessingState(current, requested ProcessingState) ProcessingState {
	if requested > current {
		return requested
	}
	return current
}
