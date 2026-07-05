package materialization

import (
	"context"
	"fmt"

	"feature_materializer_service/pkg/domain"
	"feature_materializer_service/pkg/domain/model"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

const rawSnapshotLakehouseKeyFormat = "lakehouse/raw/%s/%s/data.parquet"

type RawSnapshotWriter struct {
	store         ArtifactStore
	icebergWriter IcebergTableWriter
}

type RawSnapshotWriterOption func(*RawSnapshotWriter)

func WithRawIcebergTableWriter(writer IcebergTableWriter) RawSnapshotWriterOption {
	log.Trace("WithRawIcebergTableWriter")

	return func(rawWriter *RawSnapshotWriter) {
		rawWriter.icebergWriter = writer
	}
}

func NewRawSnapshotWriter(store ArtifactStore, opts ...RawSnapshotWriterOption) *RawSnapshotWriter {
	log.Trace("NewRawSnapshotWriter")

	writer := &RawSnapshotWriter{store: store}
	for _, opt := range opts {
		if opt != nil {
			opt(writer)
		}
	}
	return writer
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

	key := fmt.Sprintf(rawSnapshotLakehouseKeyFormat, rawSnapshot.DatasetID.String(), rawSnapshot.RawSnapshotID.String())
	location, err := w.store.Write(ctx, key, parquetContentType, artifact.Data)
	if err != nil {
		return nil, err
	}

	out := *rawSnapshot
	out.StorageLocation = location
	out.TableFormat = datasetFile.TableFormat
	out.CatalogProvider = datasetFile.CatalogProvider
	out.ProcessingProfile = datasetFile.ProcessingProfile
	out.SchemaVersion = artifact.SchemaVersion
	out.SchemaMetadata = artifact.SchemaMetadata
	out.Status = model.SnapshotStatusReady
	if out.TableNamespace == "" || out.TableName == "" {
		return nil, domain.ErrValidationFailed.Extend("raw snapshot table reference is required")
	}
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
