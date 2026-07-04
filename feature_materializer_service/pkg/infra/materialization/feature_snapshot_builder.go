package materialization

import (
	"context"
	"encoding/json"
	"fmt"

	"feature_materializer_service/pkg/domain"
	"feature_materializer_service/pkg/domain/model"

	log "github.com/sirupsen/logrus"
)

type FeatureSnapshotBuilder struct {
	store         ArtifactStore
	icebergWriter IcebergTableWriter
}

type FeatureSnapshotBuilderOption func(*FeatureSnapshotBuilder)

func WithFeatureIcebergTableWriter(writer IcebergTableWriter) FeatureSnapshotBuilderOption {
	log.Trace("WithFeatureIcebergTableWriter")

	return func(builder *FeatureSnapshotBuilder) {
		builder.icebergWriter = writer
	}
}

func NewFeatureSnapshotBuilder(store ArtifactStore, opts ...FeatureSnapshotBuilderOption) *FeatureSnapshotBuilder {
	log.Trace("NewFeatureSnapshotBuilder")

	builder := &FeatureSnapshotBuilder{
		store: store,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(builder)
		}
	}
	return builder
}

func (b *FeatureSnapshotBuilder) SupportsFeatureSnapshot(rawSnapshot *model.RawSnapshot) bool {
	log.Trace("FeatureSnapshotBuilder SupportsFeatureSnapshot")

	return rawSnapshot != nil
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
	schemaMetadata, err := MergeSourceSchemaMetadata(artifact.SchemaMetadata, rawSnapshot.SchemaMetadata)
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
	out.TableFormat = rawSnapshot.TableFormat
	out.CatalogProvider = rawSnapshot.CatalogProvider
	out.ProcessingProfile = rawSnapshot.ProcessingProfile
	out.SchemaVersion = artifact.SchemaVersion
	out.SchemaMetadata = schemaMetadata
	out.Status = model.SnapshotStatusReady

	if isPolarisIceberg(rawSnapshot.CatalogProvider, rawSnapshot.TableFormat) {
		if b.icebergWriter == nil {
			return nil, domain.ErrCatalogRegister.Extend("iceberg table writer is required")
		}
		result, err := b.icebergWriter.WriteTable(ctx, IcebergTableWriteRequest{
			Namespace:   featureSnapshot.TableNamespace,
			Table:       featureSnapshot.TableName,
			ParquetData: artifact.Data,
		})
		if err != nil {
			return nil, fmt.Errorf("%w: write iceberg feature table: %w", domain.ErrCatalogRegister, err)
		}
		out.SchemaMetadata = mergeIcebergWriteMetadata(out.SchemaMetadata, result)
	}

	return &out, nil
}

func mergeIcebergWriteMetadata(schemaMetadata string, result *IcebergTableWriteResult) string {
	log.Trace("mergeIcebergWriteMetadata")

	if result == nil {
		return schemaMetadata
	}
	metadata := map[string]any{}
	if schemaMetadata != "" {
		_ = json.Unmarshal([]byte(schemaMetadata), &metadata)
	}
	metadata["iceberg_catalog"] = result.Catalog
	metadata["iceberg_namespace"] = result.Namespace
	metadata["iceberg_table"] = result.Table
	metadata["iceberg_warehouse"] = result.Warehouse
	metadata["iceberg_source_rows"] = result.SourceRows
	metadata["iceberg_table_rows"] = result.TableRows
	encoded, err := json.Marshal(metadata)
	if err != nil {
		return schemaMetadata
	}
	return string(encoded)
}
