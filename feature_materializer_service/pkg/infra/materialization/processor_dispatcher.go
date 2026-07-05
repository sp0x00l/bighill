package materialization

import (
	"context"
	"fmt"

	"feature_materializer_service/pkg/domain"
	"feature_materializer_service/pkg/domain/model"

	log "github.com/sirupsen/logrus"
)

type RawSnapshotProcessor interface {
	SupportsRawSnapshot(*model.DatasetFile) bool
	WriteRawSnapshot(context.Context, *model.DatasetFile, *model.RawSnapshot) (*model.RawSnapshot, error)
}

type RawSnapshotWriterDispatcher struct {
	processors []RawSnapshotProcessor
}

func NewRawSnapshotWriterDispatcher(processors ...RawSnapshotProcessor) *RawSnapshotWriterDispatcher {
	log.Trace("NewRawSnapshotWriterDispatcher")

	return &RawSnapshotWriterDispatcher{processors: processors}
}

func (d *RawSnapshotWriterDispatcher) WriteRawSnapshot(ctx context.Context, datasetFile *model.DatasetFile, rawSnapshot *model.RawSnapshot) (*model.RawSnapshot, error) {
	log.Trace("RawSnapshotWriterDispatcher WriteRawSnapshot")

	for _, processor := range d.processors {
		if processor != nil && processor.SupportsRawSnapshot(datasetFile) {
			return processor.WriteRawSnapshot(ctx, datasetFile, rawSnapshot)
		}
	}
	return nil, fmt.Errorf("%w: raw snapshot processing profile %s is not supported", domain.ErrRawSnapshotMaterialize, datasetFileProfile(datasetFile))
}

type FeatureSnapshotProcessor interface {
	SupportsFeatureSnapshot(*model.RawSnapshot) bool
	BuildFeatureSnapshot(context.Context, *model.RawSnapshot, *model.FeatureSnapshot) (*model.FeatureSnapshot, error)
}

type FeatureSnapshotBuilderDispatcher struct {
	processors []FeatureSnapshotProcessor
}

func NewFeatureSnapshotBuilderDispatcher(processors ...FeatureSnapshotProcessor) *FeatureSnapshotBuilderDispatcher {
	log.Trace("NewFeatureSnapshotBuilderDispatcher")

	return &FeatureSnapshotBuilderDispatcher{processors: processors}
}

func (d *FeatureSnapshotBuilderDispatcher) BuildFeatureSnapshot(ctx context.Context, rawSnapshot *model.RawSnapshot, featureSnapshot *model.FeatureSnapshot) (*model.FeatureSnapshot, error) {
	log.Trace("FeatureSnapshotBuilderDispatcher BuildFeatureSnapshot")

	for _, processor := range d.processors {
		if processor != nil && processor.SupportsFeatureSnapshot(rawSnapshot) {
			return processor.BuildFeatureSnapshot(ctx, rawSnapshot, featureSnapshot)
		}
	}
	return nil, fmt.Errorf("%w: feature snapshot processing profile %s is not supported", domain.ErrFeatureSnapshotBuild, rawSnapshotProfile(rawSnapshot))
}

type EmbeddingProcessor interface {
	SupportsEmbeddings(*model.FeatureSnapshot) bool
	MaterializeEmbeddings(context.Context, *model.FeatureSnapshot, *model.EmbeddingSnapshot) (*model.EmbeddingSnapshot, []model.EmbeddingRecord, error)
}

type EmbeddingWriterDispatcher struct {
	processors []EmbeddingProcessor
}

func NewEmbeddingWriterDispatcher(processors ...EmbeddingProcessor) *EmbeddingWriterDispatcher {
	log.Trace("NewEmbeddingWriterDispatcher")

	return &EmbeddingWriterDispatcher{processors: processors}
}

func (d *EmbeddingWriterDispatcher) MaterializeEmbeddings(ctx context.Context, featureSnapshot *model.FeatureSnapshot, embeddingSnapshot *model.EmbeddingSnapshot) (*model.EmbeddingSnapshot, []model.EmbeddingRecord, error) {
	log.Trace("EmbeddingWriterDispatcher MaterializeEmbeddings")

	for _, processor := range d.processors {
		if processor != nil && processor.SupportsEmbeddings(featureSnapshot) {
			return processor.MaterializeEmbeddings(ctx, featureSnapshot, embeddingSnapshot)
		}
	}
	return nil, nil, fmt.Errorf("%w: embedding processing profile %s is not supported", domain.ErrEmbeddingMaterialize, featureSnapshotProfile(featureSnapshot))
}

func datasetFileProfile(datasetFile *model.DatasetFile) string {
	log.Trace("datasetFileProfile")

	if datasetFile == nil {
		return "<nil>"
	}
	return datasetFile.ProcessingProfile.String()
}

func rawSnapshotProfile(rawSnapshot *model.RawSnapshot) string {
	log.Trace("rawSnapshotProfile")

	if rawSnapshot == nil {
		return "<nil>"
	}
	return rawSnapshot.ProcessingProfile.String()
}

func featureSnapshotProfile(featureSnapshot *model.FeatureSnapshot) string {
	log.Trace("featureSnapshotProfile")

	if featureSnapshot == nil {
		return "<nil>"
	}
	return featureSnapshot.ProcessingProfile.String()
}
