package materialization

import (
	"context"
	"fmt"
	"strings"

	"feature_materializer_service/pkg/domain"
	"feature_materializer_service/pkg/domain/model"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type DataStreamRawSnapshotWriter struct {
	store  ArtifactStore
	reader DataStreamReader
}

func NewDataStreamRawSnapshotWriter(store ArtifactStore, reader DataStreamReader) *DataStreamRawSnapshotWriter {
	log.Trace("NewDataStreamRawSnapshotWriter")

	return &DataStreamRawSnapshotWriter{
		store:  store,
		reader: reader,
	}
}

func (w *DataStreamRawSnapshotWriter) SupportsRawSnapshot(datasetFile *model.DatasetFile) bool {
	log.Trace("DataStreamRawSnapshotWriter SupportsRawSnapshot")

	return datasetFile != nil && datasetFile.SourceConnectorID != uuid.Nil
}

func (w *DataStreamRawSnapshotWriter) WriteRawSnapshot(ctx context.Context, datasetFile *model.DatasetFile, rawSnapshot *model.RawSnapshot) (*model.RawSnapshot, error) {
	log.Trace("DataStreamRawSnapshotWriter WriteRawSnapshot")

	if w.reader == nil {
		return nil, domain.ErrRawSnapshotMaterialize.Extend("data stream reader is required")
	}
	if rawSnapshot == nil {
		return nil, domain.ErrValidationFailed.Extend("raw snapshot is required")
	}
	if strings.TrimSpace(datasetFile.TableNamespace) == "" || strings.TrimSpace(datasetFile.TableName) == "" {
		return nil, domain.ErrValidationFailed.Extend("raw snapshot table reference is required")
	}

	artifact, err := w.reader.ReadParquet(ctx, DataStreamReadRequest{
		UserID:            datasetFile.UserID.String(),
		SourceType:        datasetFile.SourceType,
		SourceConnectorID: datasetFile.SourceConnectorID.String(),
		SQL:               datasetFile.SourceQuery,
		Database:          datasetFile.SourceDatabase,
		Collection:        datasetFile.SourceCollection,
	})
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
	out.ContentType = parquetContentType
	out.FileExtension = "parquet"
	out.TableFormat = "PARQUET"
	out.ProcessingProfile = datasetFile.ProcessingProfile
	out.SchemaVersion = artifact.SchemaVersion
	out.SchemaMetadata = artifact.SchemaMetadata
	out.Status = model.SnapshotStatusReady
	return &out, nil
}
