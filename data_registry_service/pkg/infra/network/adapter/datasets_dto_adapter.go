package adapter

import (
	"context"
	domainErrors "data_registry_service/pkg/domain"
	"data_registry_service/pkg/domain/model"
	"encoding/json"
	"fmt"
	serializers "lib/shared_lib/serializer"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"

	"github.com/go-playground/validator/v10"
)

type DatasetDTO struct {
	ID                  string          `json:"id,omitempty"`
	UserID              string          `json:"userId,omitempty"`
	OrgID               string          `json:"orgId,omitempty"`
	Title               string          `json:"title"                       validate:"required,max=250"`
	Description         string          `json:"description,omitempty"`
	Origin              string          `json:"origin,omitempty"`
	Location            string          `json:"location,omitempty"          validate:"max=250"`
	StorageLocation     string          `json:"storageLocation,omitempty"   validate:"max=1024"`
	SourceType          string          `json:"sourceType,omitempty"`
	SourceConnectorID   string          `json:"sourceConnectorId,omitempty"`
	SourceQuery         string          `json:"sourceQuery,omitempty"`
	SourceDatabase      string          `json:"sourceDatabase,omitempty"`
	SourceCollection    string          `json:"sourceCollection,omitempty"`
	Status              string          `json:"status"`
	Category            string          `json:"category,omitempty"          validate:"max=250"`
	TableNamespace      string          `json:"tableNamespace,omitempty"    validate:"max=250"`
	TableName           string          `json:"tableName,omitempty"         validate:"max=250"`
	TableFormat         string          `json:"tableFormat,omitempty"`
	CatalogProvider     string          `json:"catalogProvider,omitempty"`
	ProcessingProfile   string          `json:"processingProfile,omitempty"`
	SchemaVersion       int             `json:"schemaVersion,omitempty"`
	SchemaMetadata      json.RawMessage `json:"schemaMetadata,omitempty"`
	ProcessingState     string          `json:"processingState,omitempty"`
	DatasetVersion      int             `json:"datasetVersion,omitempty"`
	RawSnapshotID       string          `json:"rawSnapshotId,omitempty"`
	FeatureSnapshotID   string          `json:"featureSnapshotId,omitempty"`
	EmbeddingSnapshotID string          `json:"embeddingSnapshotId,omitempty"`
	Links               ResourceLinks   `json:"links"`
}

type ResourceLinks struct {
	Self Self `json:"self"`
}

type Self struct {
	Href string `json:"href"`
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

	var orgID uuid.UUID
	if datasetDTO.OrgID != "" {
		orgID, err = uuid.Parse(datasetDTO.OrgID)
		if err != nil {
			log.WithContext(ctx).WithError(err).Error("org ID is invalid")
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

	var sourceType model.StorageType
	if datasetDTO.SourceType != "" {
		sourceType, err = model.ToStorageType(datasetDTO.SourceType)
		if err != nil {
			log.WithContext(ctx).WithError(err).Error("dataset source type is invalid")
			return nil, domainErrors.ErrValidationFailed.Extend(err.Error())
		}
	}

	var sourceConnectorID uuid.UUID
	if datasetDTO.SourceConnectorID != "" {
		if datasetDTO.SourceType == "" {
			return nil, domainErrors.ErrValidationFailed.Extend("dataset source type is required when source connector id is set")
		}
		sourceConnectorID, err = uuid.Parse(datasetDTO.SourceConnectorID)
		if err != nil {
			log.WithContext(ctx).WithError(err).Error("dataset source connector ID is invalid")
			return nil, domainErrors.ErrValidationFailed.Extend(err.Error())
		}
	}

	processingProfile, err := model.ToProcessingProfile(datasetDTO.ProcessingProfile)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("dataset processing profile is invalid")
		return nil, domainErrors.ErrValidationFailed.Extend(err.Error())
	}

	location := datasetDTO.StorageLocation
	if location == "" {
		location = datasetDTO.Location
	}
	schemaMetadata := string(datasetDTO.SchemaMetadata)

	modelDataset := &model.Dataset{
		ID:                datasetID,
		UserID:            userID,
		OrgID:             orgID,
		Title:             datasetDTO.Title,
		Description:       datasetDTO.Description,
		Origin:            origin,
		Location:          location,
		SourceType:        sourceType,
		SourceConnectorID: sourceConnectorID,
		SourceQuery:       datasetDTO.SourceQuery,
		SourceDatabase:    datasetDTO.SourceDatabase,
		SourceCollection:  datasetDTO.SourceCollection,
		Status:            status,
		Category:          datasetDTO.Category,
		TableNamespace:    datasetDTO.TableNamespace,
		TableName:         datasetDTO.TableName,
		TableFormat:       tableFormat,
		CatalogProvider:   catalogProvider,
		ProcessingProfile: processingProfile,
		SchemaVersion:     datasetDTO.SchemaVersion,
		SchemaMetadata:    schemaMetadata,
	}
	model.NormalizeDatasetMetadata(modelDataset)

	return modelDataset, nil
}

func (a *dtoAdapter) toDTO(datasetModel *model.Dataset, baseURL string) *DatasetDTO {
	log.Trace("dtoAdapter toDTO")

	schemaMetadata := datasetModel.SchemaMetadata
	dtoDataset := DatasetDTO{
		ID:                  datasetModel.ID.String(),
		UserID:              datasetModel.UserID.String(),
		OrgID:               datasetModel.OrgID.String(),
		Title:               datasetModel.Title,
		Description:         datasetModel.Description,
		Origin:              datasetModel.Origin.String(),
		Location:            datasetModel.Location,
		StorageLocation:     datasetModel.Location,
		SourceType:          datasetSourceType(datasetModel),
		SourceConnectorID:   uuidToString(datasetModel.SourceConnectorID),
		SourceQuery:         datasetModel.SourceQuery,
		SourceDatabase:      datasetModel.SourceDatabase,
		SourceCollection:    datasetModel.SourceCollection,
		Status:              datasetModel.Status.String(),
		Category:            datasetModel.Category,
		TableNamespace:      datasetModel.TableNamespace,
		TableName:           datasetModel.TableName,
		TableFormat:         datasetModel.TableFormat.String(),
		CatalogProvider:     datasetModel.CatalogProvider.String(),
		ProcessingProfile:   datasetModel.ProcessingProfile.String(),
		SchemaVersion:       datasetModel.SchemaVersion,
		SchemaMetadata:      json.RawMessage(schemaMetadata),
		ProcessingState:     datasetModel.ProcessingState.String(),
		DatasetVersion:      datasetModel.DatasetVersion,
		RawSnapshotID:       uuidToString(datasetModel.RawSnapshotID),
		FeatureSnapshotID:   uuidToString(datasetModel.FeatureSnapshotID),
		EmbeddingSnapshotID: uuidToString(datasetModel.EmbeddingSnapshotID),
		Links: ResourceLinks{
			Self: Self{
				Href: fmt.Sprintf("%s/%s", baseURL, datasetModel.ID.String()),
			},
		},
	}
	return &dtoDataset
}

func datasetSourceType(dataset *model.Dataset) string {
	log.Trace("datasetSourceType")

	if dataset == nil || dataset.SourceConnectorID == uuid.Nil {
		return ""
	}
	return dataset.SourceType.String()
}

func uuidToString(id uuid.UUID) string {
	log.Trace("uuidToString")

	if id == uuid.Nil {
		return ""
	}
	return id.String()
}
