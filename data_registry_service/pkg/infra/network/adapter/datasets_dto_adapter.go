package adapter

import (
	"context"
	serializers "data_registry_service/pkg/common/serializer"
	domainErrors "data_registry_service/pkg/domain"
	"data_registry_service/pkg/domain/model"
	"encoding/json"
	"fmt"
	coreRest "lib/shared_lib/transport"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"

	"github.com/go-playground/validator/v10"
)

type DatasetDTO struct {
	ID              string                 `json:"id,omitempty"`
	UserID          string                 `json:"userId,omitempty"`
	Title           string                 `json:"title"                       validate:"required,max=250"`
	Description     string                 `json:"description,omitempty"`
	Origin          string                 `json:"origin,omitempty"`
	Location        string                 `json:"location,omitempty"          validate:"max=250"`
	StorageLocation string                 `json:"storageLocation,omitempty"   validate:"max=1024"`
	Status          string                 `json:"status"`
	Category        string                 `json:"category,omitempty"          validate:"max=250"`
	TableNamespace  string                 `json:"tableNamespace,omitempty"    validate:"max=250"`
	TableName       string                 `json:"tableName,omitempty"         validate:"max=250"`
	TableFormat     string                 `json:"tableFormat,omitempty"`
	CatalogProvider string                 `json:"catalogProvider,omitempty"`
	SchemaVersion   int                    `json:"schemaVersion,omitempty"`
	SchemaMetadata  json.RawMessage        `json:"schemaMetadata,omitempty"`
	ProcessingState string                 `json:"processingState,omitempty"`
	Links           coreRest.ResourceLinks `json:"links"`
}

type dtoAdapter struct {
	validator *validator.Validate
	encoder   *serializers.Encoder
}

type DatasetDTOAdapter interface {
	ToDTO(ctx context.Context, datasetsModel *model.Dataset, baseURL string) ([]byte, error)
	ToDTOs(ctx context.Context, datasetsModels []*model.Dataset, baseURL string) []any
	FromDTO(ctx context.Context, datasetBytes []byte) (*model.Dataset, error)
}

func NewDatasetDTOAdapter(encoder *serializers.Encoder) *dtoAdapter {
	log.Trace("NewDatasetDTOAdapter")

	return &dtoAdapter{
		validator: validator.New(),
		encoder:   encoder,
	}
}

func (a *dtoAdapter) ToDTOs(ctx context.Context, datasetsModels []*model.Dataset, baseURL string) []any {
	log.Trace("dtoAdapter ToDTOs")

	var resources []any
	for _, datasetModel := range datasetsModels {
		datasetDTO := a.toDTO(datasetModel, baseURL)
		resources = append(resources, datasetDTO)
	}

	return resources
}

func (a *dtoAdapter) ToDTO(ctx context.Context, datasetModel *model.Dataset, baseURL string) ([]byte, error) {
	log.Trace("dtoAdapter ToDTO")

	datasetDTO := a.toDTO(datasetModel, baseURL)
	encoded, err := a.encoder.EncodeDataToString(datasetDTO)
	if err != nil {
		return nil, err
	}
	return []byte(encoded), nil
}

func (a *dtoAdapter) FromDTO(ctx context.Context, datasetBytes []byte) (*model.Dataset, error) {
	log.Trace("dtoAdapter FromDTO")

	var datasetDTO DatasetDTO
	err := a.encoder.DecodeStringToData(string(datasetBytes), &datasetDTO)
	if err != nil {
		return nil, domainErrors.ErrValidationFailed.Extend(err.Error())
	}

	datasetModel, err := a.fromDTO(ctx, &datasetDTO)
	if err != nil {
		return nil, err
	}
	return datasetModel, nil
}

func (a *dtoAdapter) fromDTO(ctx context.Context, datasetDTO *DatasetDTO) (*model.Dataset, error) {
	log.Trace("dtoAdapter fromDTO")

	if err := a.validator.Struct(datasetDTO); err != nil {
		log.WithContext(ctx).WithError(err).Error("datasetDTO validation failed")
		return nil, domainErrors.ErrValidationFailed.Extend(err.Error())
	}

	var err error
	var datasetID uuid.UUID
	if datasetDTO.ID != "" {
		datasetID, err = uuid.Parse(datasetDTO.ID)
		if err != nil {
			log.WithContext(ctx).WithError(err).Error("dataset ID is invalid")
			return nil, domainErrors.ErrValidationFailed.Extend(err.Error())
		}
	}

	var userID uuid.UUID
	if datasetDTO.UserID != "" {
		userID, err = uuid.Parse(datasetDTO.UserID)
		if err != nil {
			log.WithContext(ctx).WithError(err).Error("user ID is invalid")
			return nil, domainErrors.ErrValidationFailed.Extend(err.Error())
		}
	}

	var origin model.OriginType
	if datasetDTO.Origin != "" {
		origin, err = model.ToOriginType(datasetDTO.Origin)
		if err != nil {
			log.WithContext(ctx).WithError(err).Error("dataset origin is invalid")
			return nil, domainErrors.ErrValidationFailed.Extend(err.Error())
		}
	}

	var status model.StatusType
	if datasetDTO.Status != "" {
		status, err = model.ToStatusType(datasetDTO.Status)
		if err != nil {
			log.WithContext(ctx).WithError(err).Error("dataset status is invalid")
			return nil, domainErrors.ErrValidationFailed.Extend(err.Error())
		}
	}
	var tableFormat model.TableFormat
	if datasetDTO.TableFormat != "" {
		tableFormat, err = model.ToTableFormat(datasetDTO.TableFormat)
		if err != nil {
			log.WithContext(ctx).WithError(err).Error("dataset table format is invalid")
			return nil, domainErrors.ErrValidationFailed.Extend(err.Error())
		}
	}

	var catalogProvider model.CatalogProvider
	if datasetDTO.CatalogProvider != "" {
		catalogProvider, err = model.ToCatalogProvider(datasetDTO.CatalogProvider)
		if err != nil {
			log.WithContext(ctx).WithError(err).Error("dataset catalog provider is invalid")
			return nil, domainErrors.ErrValidationFailed.Extend(err.Error())
		}
	}

	location := datasetDTO.StorageLocation
	if location == "" {
		location = datasetDTO.Location
	}
	schemaMetadata := string(datasetDTO.SchemaMetadata)

	modelDataset := &model.Dataset{
		ID:              datasetID,
		UserID:          userID,
		Title:           datasetDTO.Title,
		Description:     datasetDTO.Description,
		Origin:          origin,
		Location:        location,
		Status:          status,
		Category:        datasetDTO.Category,
		TableNamespace:  datasetDTO.TableNamespace,
		TableName:       datasetDTO.TableName,
		TableFormat:     tableFormat,
		CatalogProvider: catalogProvider,
		SchemaVersion:   datasetDTO.SchemaVersion,
		SchemaMetadata:  schemaMetadata,
	}
	model.NormalizeDatasetMetadata(modelDataset)

	return modelDataset, nil
}

func (a *dtoAdapter) toDTO(datasetModel *model.Dataset, baseURL string) *DatasetDTO {
	log.Trace("dtoAdapter toDTO")

	schemaMetadata := datasetModel.SchemaMetadata
	if schemaMetadata == "" {
		schemaMetadata = "{}"
	}
	dtoDataset := DatasetDTO{
		ID:              datasetModel.ID.String(),
		UserID:          datasetModel.UserID.String(),
		Title:           datasetModel.Title,
		Description:     datasetModel.Description,
		Origin:          datasetModel.Origin.String(),
		Location:        datasetModel.Location,
		StorageLocation: datasetModel.Location,
		Status:          datasetModel.Status.String(),
		Category:        datasetModel.Category,
		TableNamespace:  datasetModel.TableNamespace,
		TableName:       datasetModel.TableName,
		TableFormat:     datasetModel.TableFormat.String(),
		CatalogProvider: datasetModel.CatalogProvider.String(),
		SchemaVersion:   datasetModel.SchemaVersion,
		SchemaMetadata:  json.RawMessage(schemaMetadata),
		ProcessingState: datasetModel.ProcessingState.String(),
		Links: coreRest.ResourceLinks{
			Self: coreRest.Self{
				Href: fmt.Sprintf("%s/%s", baseURL, datasetModel.ID.String()),
			},
		},
	}
	return &dtoDataset
}
