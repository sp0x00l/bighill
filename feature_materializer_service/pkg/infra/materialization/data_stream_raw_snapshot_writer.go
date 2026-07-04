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
	store         ArtifactStore
	reader        DataStreamReader
	icebergWriter IcebergTableWriter
}

type DataStreamRawSnapshotWriterOption func(*DataStreamRawSnapshotWriter)

func WithDataStreamRawIcebergTableWriter(writer IcebergTableWriter) DataStreamRawSnapshotWriterOption {
	log.Trace("WithDataStreamRawIcebergTableWriter")

	return func(rawWriter *DataStreamRawSnapshotWriter) {
		rawWriter.icebergWriter = writer
	}
}

func NewDataStreamRawSnapshotWriter(store ArtifactStore, reader DataStreamReader, opts ...DataStreamRawSnapshotWriterOption) *DataStreamRawSnapshotWriter {
	log.Trace("NewDataStreamRawSnapshotWriter")

	writer := &DataStreamRawSnapshotWriter{
		store:  store,
		reader: reader,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(writer)
		}
	}
	return writer
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
	out.TableFormat = datasetFile.TableFormat
	out.CatalogProvider = datasetFile.CatalogProvider
	out.ProcessingProfile = datasetFile.ProcessingProfile
	out.SchemaVersion = artifact.SchemaVersion
	out.SchemaMetadata = artifact.SchemaMetadata
	out.Status = model.SnapshotStatusReady
	if isPolarisIceberg(out.CatalogProvider, out.TableFormat) {
		if w.icebergWriter == nil {
			return nil, domain.ErrCatalogRegister.Extend("iceberg table writer is required")
		}
		result, err := w.icebergWriter.WriteTable(ctx, IcebergTableWriteRequest{
			Namespace:   out.TableNamespace,
			Table:       out.TableName,
			ParquetData: artifact.Data,
		})
		if err != nil {
			return nil, fmt.Errorf("%w: write iceberg raw table: %w", domain.ErrCatalogRegister, err)
		}
		out.SchemaMetadata = mergeIcebergWriteMetadata(out.SchemaMetadata, result)
	}
	return &out, nil
}
