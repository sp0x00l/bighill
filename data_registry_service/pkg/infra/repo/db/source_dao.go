package db

import (
	"context"
	domainErrors "data_registry_service/pkg/domain"
	"data_registry_service/pkg/domain/model"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	log "github.com/sirupsen/logrus"
)

type DatasetDAO struct {
	ID                  pgtype.UUID
	UserID              pgtype.UUID
	Title               pgtype.Text
	Description         pgtype.Text
	Origin              pgtype.Text
	Location            pgtype.Text
	Status              pgtype.Text
	Category            pgtype.Text
	TableNamespace      pgtype.Text
	TableName           pgtype.Text
	TableFormat         pgtype.Text
	CatalogProvider     pgtype.Text
	ProcessingProfile   pgtype.Text
	SchemaVersion       pgtype.Int4
	SchemaMetadata      pgtype.Text
	ProcessingState     pgtype.Text
	DatasetVersion      pgtype.Int4
	RawSnapshotID       pgtype.UUID
	FeatureSnapshotID   pgtype.UUID
	EmbeddingSnapshotID pgtype.UUID
	VectorStore         pgtype.Text
	CollectionName      pgtype.Text
	EmbeddingDimensions pgtype.Int4
	EmbeddingCount      pgtype.Int8
}

type Dataset struct {
	IdempotencyKey pgtype.UUID `db:"idempotency_key"`
}

func (d *Dataset) toDAO(dataset *model.Dataset) pgx.NamedArgs {
	log.Trace("DatasetDAO toDAO")

	dao := pgx.NamedArgs{
		"id":              pgtype.UUID{Bytes: dataset.ID, Valid: true},
		"user_id":         pgtype.UUID{Bytes: dataset.UserID, Valid: true},
		"title":           pgtype.Text{String: dataset.Title, Valid: true},
		"description":     pgtype.Text{String: dataset.Description, Valid: dataset.Description != ""},
		"origin":          pgtype.Text{String: dataset.Origin.String(), Valid: true},
		"location":        pgtype.Text{String: dataset.Location, Valid: dataset.Location != ""},
		"status":          pgtype.Text{String: dataset.Status.String(), Valid: true},
		"category":        pgtype.Text{String: dataset.Category, Valid: dataset.Category != ""},
		"idempotency_key": pgtype.UUID{Bytes: d.IdempotencyKey.Bytes, Valid: true},
		"table_namespace": pgtype.Text{String: dataset.TableNamespace, Valid: true},
		"table_name":      pgtype.Text{String: dataset.TableName, Valid: true},
		"table_format":    pgtype.Text{String: dataset.TableFormat.String(), Valid: true},
		"catalog_provider": pgtype.Text{
			String: dataset.CatalogProvider.String(),
			Valid:  true,
		},
		"processing_profile": pgtype.Text{String: dataset.ProcessingProfile.String(), Valid: true},
		"schema_version":     pgtype.Int4{Int32: int32(dataset.SchemaVersion), Valid: true},
		"schema_metadata":    pgtype.Text{String: dataset.SchemaMetadata, Valid: true},
		"processing_state": pgtype.Text{
			String: dataset.ProcessingState.String(),
			Valid:  true,
		},
		"dataset_version":       pgtype.Int4{Int32: int32(dataset.DatasetVersion), Valid: true},
		"raw_snapshot_id":       pgtype.UUID{Bytes: dataset.RawSnapshotID, Valid: dataset.RawSnapshotID != uuid.Nil},
		"feature_snapshot_id":   pgtype.UUID{Bytes: dataset.FeatureSnapshotID, Valid: dataset.FeatureSnapshotID != uuid.Nil},
		"embedding_snapshot_id": pgtype.UUID{Bytes: dataset.EmbeddingSnapshotID, Valid: dataset.EmbeddingSnapshotID != uuid.Nil},
		"vector_store":          pgtype.Text{String: dataset.VectorStore, Valid: true},
		"collection_name":       pgtype.Text{String: dataset.CollectionName, Valid: true},
		"embedding_dimensions":  pgtype.Int4{Int32: int32(dataset.EmbeddingDimensions), Valid: true},
		"embedding_count":       pgtype.Int8{Int64: dataset.EmbeddingCount, Valid: true},
	}

	return dao
}

func fromDAO(ctx context.Context, dao *DatasetDAO) (*model.Dataset, error) {
	log.Trace("DatasetDAO fromDAO")

	origin, err := model.ToOriginType(dao.Origin.String)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("Failed to convert origin type")
		return nil, domainErrors.ErrValidationFailed.Extend("failed to convert origin type")
	}

	status, err := model.ToStatusType(dao.Status.String)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("Failed to convert status type")
		return nil, domainErrors.ErrValidationFailed.Extend("failed to convert status type")
	}
	tableFormat, err := model.ToTableFormat(dao.TableFormat.String)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("Failed to convert table format")
		return nil, domainErrors.ErrValidationFailed.Extend("failed to convert table format")
	}
	catalogProvider, err := model.ToCatalogProvider(dao.CatalogProvider.String)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("Failed to convert catalog provider")
		return nil, domainErrors.ErrValidationFailed.Extend("failed to convert catalog provider")
	}
	processingProfile, err := model.ToProcessingProfile(dao.ProcessingProfile.String)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("Failed to convert processing profile")
		return nil, domainErrors.ErrValidationFailed.Extend("failed to convert processing profile")
	}
	processingState, err := model.ToProcessingState(dao.ProcessingState.String)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("Failed to convert processing state")
		return nil, domainErrors.ErrValidationFailed.Extend("failed to convert processing state")
	}

	return &model.Dataset{
		ID:                  dao.ID.Bytes,
		UserID:              dao.UserID.Bytes,
		Title:               dao.Title.String,
		Description:         dao.Description.String,
		Origin:              origin,
		Location:            dao.Location.String,
		Status:              status,
		Category:            dao.Category.String,
		TableNamespace:      dao.TableNamespace.String,
		TableName:           dao.TableName.String,
		TableFormat:         tableFormat,
		CatalogProvider:     catalogProvider,
		ProcessingProfile:   processingProfile,
		SchemaVersion:       int(dao.SchemaVersion.Int32),
		SchemaMetadata:      dao.SchemaMetadata.String,
		ProcessingState:     processingState,
		DatasetVersion:      int(dao.DatasetVersion.Int32),
		RawSnapshotID:       dao.RawSnapshotID.Bytes,
		FeatureSnapshotID:   dao.FeatureSnapshotID.Bytes,
		EmbeddingSnapshotID: dao.EmbeddingSnapshotID.Bytes,
		VectorStore:         dao.VectorStore.String,
		CollectionName:      dao.CollectionName.String,
		EmbeddingDimensions: int(dao.EmbeddingDimensions.Int32),
		EmbeddingCount:      dao.EmbeddingCount.Int64,
	}, nil
}
