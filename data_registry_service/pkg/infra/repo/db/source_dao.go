package db

import (
	"context"
	domainErrors "data_registry_service/pkg/domain"
	"data_registry_service/pkg/domain/model"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	log "github.com/sirupsen/logrus"
)

type DatasetDAO struct {
	ID              pgtype.UUID
	UserID          pgtype.UUID
	Title           pgtype.Text
	Description     pgtype.Text
	Origin          pgtype.Text
	Location        pgtype.Text
	Status          pgtype.Text
	Category        pgtype.Text
	TableNamespace  pgtype.Text
	TableName       pgtype.Text
	TableFormat     pgtype.Text
	CatalogProvider pgtype.Text
	SchemaVersion   pgtype.Int4
	SchemaMetadata  pgtype.Text
	ProcessingState pgtype.Text
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
		"origin":          pgtype.Text{String: dataset.Origin.String(), Valid: true},
		"status":          pgtype.Text{String: dataset.Status.String(), Valid: true},
		"idempotency_key": pgtype.UUID{Bytes: d.IdempotencyKey.Bytes, Valid: true},
		"table_namespace": pgtype.Text{String: dataset.TableNamespace, Valid: true},
		"table_name":      pgtype.Text{String: dataset.TableName, Valid: true},
		"table_format":    pgtype.Text{String: dataset.TableFormat.String(), Valid: true},
		"catalog_provider": pgtype.Text{
			String: dataset.CatalogProvider.String(),
			Valid:  true,
		},
		"schema_version":  pgtype.Int4{Int32: int32(dataset.SchemaVersion), Valid: true},
		"schema_metadata": pgtype.Text{String: dataset.SchemaMetadata, Valid: true},
		"processing_state": pgtype.Text{
			String: dataset.ProcessingState.String(),
			Valid:  true,
		},
	}

	if dataset.Description != "" {
		dao["description"] = pgtype.Text{String: dataset.Description, Valid: true}
	}
	if dataset.Location != "" {
		dao["location"] = pgtype.Text{String: dataset.Location, Valid: true}
	}

	if dataset.Category != "" {
		dao["category"] = pgtype.Text{String: dataset.Category, Valid: true}
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
	processingState, err := model.ToProcessingState(dao.ProcessingState.String)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("Failed to convert processing state")
		return nil, domainErrors.ErrValidationFailed.Extend("failed to convert processing state")
	}

	return &model.Dataset{
		ID:              dao.ID.Bytes,
		UserID:          dao.UserID.Bytes,
		Title:           dao.Title.String,
		Description:     dao.Description.String,
		Origin:          origin,
		Location:        dao.Location.String,
		Status:          status,
		Category:        dao.Category.String,
		TableNamespace:  dao.TableNamespace.String,
		TableName:       dao.TableName.String,
		TableFormat:     tableFormat,
		CatalogProvider: catalogProvider,
		SchemaVersion:   int(dao.SchemaVersion.Int32),
		SchemaMetadata:  dao.SchemaMetadata.String,
		ProcessingState: processingState,
	}, nil
}
