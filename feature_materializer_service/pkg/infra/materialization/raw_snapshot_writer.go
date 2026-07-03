package materialization

import (
	"context"
	"fmt"

	"feature_materializer_service/pkg/domain"
	"feature_materializer_service/pkg/domain/model"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type RawSnapshotWriter struct {
	store ArtifactStore
}

func NewRawSnapshotWriter(store ArtifactStore) *RawSnapshotWriter {
	log.Trace("NewRawSnapshotWriter")

	return &RawSnapshotWriter{store: store}
}

func (w *RawSnapshotWriter) SupportsRawSnapshot(datasetFile *model.DatasetFile) bool {
	log.Trace("RawSnapshotWriter SupportsRawSnapshot")

	return datasetFile != nil && datasetFile.SourceConnectorID == uuid.Nil
}

func (w *RawSnapshotWriter) WriteRawSnapshot(ctx context.Context, datasetFile *model.DatasetFile, rawSnapshot *model.RawSnapshot) (*model.RawSnapshot, error) {
	log.Trace("RawSnapshotWriter WriteRawSnapshot")

	data, err := w.store.Read(ctx, datasetFile.StorageLocation)
	if err != nil {
		return nil, err
	}

	artifact, err := NormalizeArtifactToParquet(ctx, data, datasetFile.ContentType, datasetFile.FileExtension)
	if err != nil {
		return nil, err
	}

	key := fmt.Sprintf("lakehouse/raw/%s/%s/data.parquet", rawSnapshot.DatasetID.String(), rawSnapshot.RawSnapshotID.String())
	location, err := w.store.Write(ctx, key, parquetContentType, artifact.Data)
	if err != nil {
		return nil, err
	}

	out := *rawSnapshot
	out.StorageLocation = location
	out.TableFormat = "PARQUET"
	out.ProcessingProfile = datasetFile.ProcessingProfile
	out.SchemaVersion = artifact.SchemaVersion
	out.SchemaMetadata = artifact.SchemaMetadata
	out.Status = model.SnapshotStatusReady
	if out.TableNamespace == "" || out.TableName == "" {
		return nil, domain.ErrValidationFailed.Extend("raw snapshot table reference is required")
	}
	return &out, nil
}
