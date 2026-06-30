package materialization

import (
	"context"
	"fmt"

	"feature_materializer_service/pkg/domain/model"

	log "github.com/sirupsen/logrus"
)

type FeatureSnapshotBuilder struct {
	store ArtifactStore
}

func NewFeatureSnapshotBuilder(store ArtifactStore) *FeatureSnapshotBuilder {
	log.Trace("NewFeatureSnapshotBuilder")

	return &FeatureSnapshotBuilder{
		store: store,
	}
}

func (b *FeatureSnapshotBuilder) BuildFeatureSnapshot(ctx context.Context, rawSnapshot *model.RawSnapshot, featureSnapshot *model.FeatureSnapshot) (*model.FeatureSnapshot, error) {
	log.Trace("FeatureSnapshotBuilder BuildFeatureSnapshot")

	data, err := b.store.Read(ctx, rawSnapshot.StorageLocation)
	if err != nil {
		return nil, err
	}

	artifact, err := NormalizeArtifactToParquet(ctx, data, parquetContentType, "parquet")
	if err != nil {
		return nil, err
	}

	key := fmt.Sprintf("lakehouse/features/%s/%s/data.parquet", featureSnapshot.DatasetID.String(), featureSnapshot.FeatureSnapshotID.String())
	location, err := b.store.Write(ctx, key, parquetContentType, artifact.Data)
	if err != nil {
		return nil, err
	}

	out := *featureSnapshot
	out.UserID = rawSnapshot.UserID
	out.StorageLocation = location
	out.TableFormat = "PARQUET"
	out.SchemaVersion = artifact.SchemaVersion
	out.SchemaMetadata = artifact.SchemaMetadata
	out.Status = model.SnapshotStatusReady

	return &out, nil
}
