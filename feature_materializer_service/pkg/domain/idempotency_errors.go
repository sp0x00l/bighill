package domain

import (
	"errors"

	"feature_materializer_service/pkg/domain/model"
)

type RawSnapshotAlreadyMaterializedError struct {
	Record *model.RawSnapshot
}

func (e *RawSnapshotAlreadyMaterializedError) Error() string {
	return "raw snapshot already materialized"
}

func IsRawSnapshotAlreadyMaterialized(err error) (*model.RawSnapshot, bool) {
	var alreadyMaterialized *RawSnapshotAlreadyMaterializedError
	if !errors.As(err, &alreadyMaterialized) {
		return nil, false
	}
	return alreadyMaterialized.Record, true
}

type FeatureSnapshotAlreadyBuiltError struct {
	Record *model.FeatureSnapshot
}

func (e *FeatureSnapshotAlreadyBuiltError) Error() string {
	return "feature snapshot already built"
}

func IsFeatureSnapshotAlreadyBuilt(err error) (*model.FeatureSnapshot, bool) {
	var alreadyBuilt *FeatureSnapshotAlreadyBuiltError
	if !errors.As(err, &alreadyBuilt) {
		return nil, false
	}
	return alreadyBuilt.Record, true
}

type EmbeddingsAlreadyMaterializedError struct {
	Record *model.EmbeddingSnapshot
}

func (e *EmbeddingsAlreadyMaterializedError) Error() string {
	return "embeddings already materialized"
}

func IsEmbeddingsAlreadyMaterialized(err error) (*model.EmbeddingSnapshot, bool) {
	var alreadyMaterialized *EmbeddingsAlreadyMaterializedError
	if !errors.As(err, &alreadyMaterialized) {
		return nil, false
	}
	return alreadyMaterialized.Record, true
}

type GraphAlreadyMaterializedError struct {
	Record *model.GraphSnapshot
}

func (e *GraphAlreadyMaterializedError) Error() string {
	return "graph already materialized"
}

func IsGraphAlreadyMaterialized(err error) (*model.GraphSnapshot, bool) {
	var alreadyMaterialized *GraphAlreadyMaterializedError
	if !errors.As(err, &alreadyMaterialized) {
		return nil, false
	}
	return alreadyMaterialized.Record, true
}
