package rest

import (
	"context"
	app "data_registry_service/pkg/app"
	"data_registry_service/pkg/domain/model"
	"data_registry_service/pkg/infra/network/adapter"
	"fmt"

	domainErrors "data_registry_service/pkg/domain"
	rest "data_registry_service/pkg/infra/network/restsupport"
	core "lib/shared_lib/transport"
	"net/http"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
)

const (
	pathPublicPublishedDatasets = "/v1/public/data/registry"
	pathAuthUserDatasets        = "/v1/data/registry"
)

type SourceConnectorAdapter interface {
	ToDTO(ctx context.Context, conn *model.SourceConnector) ([]byte, error)
	FromDTO(ctx context.Context, storageType model.StorageType, body []byte) (*model.SourceConnector, error)
}

type DataRegistryHandlers struct {
	datasetsUsecase      app.DatasetUsecase
	sourceUsecase        app.SourceUsecase
	datasetDTOAdapter    adapter.DatasetDTOAdapter
	filterAdapter        adapter.FilterDTOAdapter
	sourceConnDTOAdapter SourceConnectorAdapter
}

func NewDataRegistryHandlers(datasetsUsecase app.DatasetUsecase, sourceUsecase app.SourceUsecase,
	datasetDTOAdapter adapter.DatasetDTOAdapter, sourceConnDTOAdapter SourceConnectorAdapter,
	filterAdapter adapter.FilterDTOAdapter) *DataRegistryHandlers {
	log.Trace("NewDataRegistryHandlers")

	return &DataRegistryHandlers{
		datasetsUsecase:      datasetsUsecase,
		sourceUsecase:        sourceUsecase,
		datasetDTOAdapter:    datasetDTOAdapter,
		sourceConnDTOAdapter: sourceConnDTOAdapter,
		filterAdapter:        filterAdapter,
	}
}

func (h *DataRegistryHandlers) GetRoutes() []rest.Route {
	log.Trace("DataRegistryHandlers GetRoutes")

	return []rest.Route{
		{
			Path:     "/v1/data/registry",
			Handler:  h.CreateDataset,
			Method:   http.MethodPost,
			SpanName: "create-dataset",
		},
		{
			Path:     pathPublicPublishedDatasets,
			Handler:  h.ReadPublishedDatasets,
			Method:   http.MethodGet,
			SpanName: "read-published-datasets",
		},
		{
			Path:     "/v1/public/data/registry/{datasetId}",
			Handler:  h.ReadPublishedDatasetByID,
			Method:   http.MethodGet,
			SpanName: "read-published-dataset-by-id",
		},
		{
			Path:     "/v1/public/data/registry/user/{userId}",
			Handler:  h.ReadPublishedDatasetsByUserID,
			Method:   http.MethodGet,
			SpanName: "read-published-dataset-by-user-id",
		},
		{
			Path:     pathAuthUserDatasets,
			Handler:  h.ReadDatasets,
			Method:   http.MethodGet,
			SpanName: "read-authenticated-user-datasets",
		},
		{
			Path:     "/v1/data/registry/{datasetId}",
			Handler:  h.ReadDatasetByID,
			Method:   http.MethodGet,
			SpanName: "read-authenticated-user-dataset-by-id",
		},
		{
			Path:     "/v1/data/registry/{datasetId}",
			Handler:  h.DeleteDataset,
			Method:   http.MethodDelete,
			SpanName: "delete-dataset",
		},
		{
			Path:     "/v1/data/registry/{datasetId}/publish",
			Handler:  h.PublishDataset,
			Method:   http.MethodPatch,
			SpanName: "publish-dataset",
		},
		{
			Path:     "/v1/data/registry/{datasetId}",
			Handler:  h.ReplaceDataset,
			Method:   http.MethodPut,
			SpanName: "replace-dataset",
		},
		{
			Path:     "/v1/data/registry/connector/{type}",
			Handler:  h.CreateSourceConnector,
			Method:   http.MethodPost,
			SpanName: "create-source-connector",
		},
		{
			Path:     "/v1/data/registry/connector/{type}/{connectorId}",
			Handler:  h.ReadSourceConnector,
			Method:   http.MethodGet,
			SpanName: "read-source-connector",
		},
		{
			Path:     "/v1/data/registry/connector/{type}/{connectorId}",
			Handler:  h.ReplaceSourceConnector,
			Method:   http.MethodPut,
			SpanName: "replace-source-connector",
		},
		{
			Path:     "/v1/data/registry/connector/{type}/{connectorId}",
			Handler:  h.DeleteSourceConnector,
			Method:   http.MethodDelete,
			SpanName: "delete-source-connector",
		},
	}
}

func (h *DataRegistryHandlers) CreateDataset(ctx context.Context, req *http.Request) (rest.APIResponse, error) {
	log.Trace("DataRegistryHandlers CreateDataset")

	idempotencyKey, err := rest.ReadIdempotencyIDHeader(ctx, req)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, contextKey("Dataset idempotency key"), idempotencyKey.String())

	userID, err := rest.ReadUserIDHeader(ctx, req)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, contextKey("UserID"), userID.String())

	dataset, err := h.requestToDataset(ctx, req)
	if err != nil {
		return nil, err
	}

	dataset.UserID = userID
	if err := h.datasetsUsecase.CreateDataset(ctx, dataset, idempotencyKey); err != nil {
		if domainErrors.IsServiceError(err, domainErrors.ErrResourceAlreadyExists) {
			return nil, rest.ErrConflict().Wrap(err).WithMessage("Dataset idempotency key already exists")
		}
		return nil, rest.ErrInternalServer().Wrap(err).WithMessage("Failed to create dataset")
	}

	datasetBytes, err := h.datasetDTOAdapter.ToDTO(ctx, dataset, pathAuthUserDatasets)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("encode create-dataset response failed")
		return nil, rest.ErrInternalServer().Wrap(err).WithMessage("Failed to encode dataset")
	}

	return rest.NewResponseWithPayload(http.StatusCreated, datasetBytes), nil
}

func (h *DataRegistryHandlers) ReadPublishedDatasets(ctx context.Context, req *http.Request) (rest.APIResponse, error) {
	log.Trace("DataRegistryHandlers ReadPublishedDatasets")

	pagination, filters, err := h.requestToPaginationAndFilters(ctx, req)
	if err != nil {
		return nil, err
	}

	ctx = context.WithValue(ctx, contextKey("Pagination"), pagination)
	ctx = context.WithValue(ctx, contextKey("Filters"), filters)

	datasets, count, err := h.datasetsUsecase.ReadPublishedDatasets(ctx, *pagination, filters)
	if err != nil {
		log.WithContext(ctx).WithError(err).Errorf("read published dataset failed")
		if domainErrors.IsServiceError(err, domainErrors.ErrResourceNotFound) {
			return nil, rest.ErrNotFound().Wrap(err).WithMessage("Datasets not found")
		}
		return nil, rest.ErrInternalServer().Wrap(err).WithMessage("Failed to read dataset")
	}

	metadata, _ := rest.NewMetadata(ctx, count, *pagination, req.URL.String())
	paginatedResponse := &rest.PaginatedResponse{
		Metadata: metadata,
	}
	if len(datasets) > 0 {
		paginatedResponse.Resources = h.datasetDTOAdapter.ToDTOs(ctx, datasets, pathPublicPublishedDatasets)
	}
	return rest.NewResponseWithPagination(http.StatusOK, paginatedResponse), nil
}

func (h *DataRegistryHandlers) ReadPublishedDatasetByID(ctx context.Context, req *http.Request) (rest.APIResponse, error) {
	log.Trace("DataRegistryHandlers ReadPublishedDatasetByID")

	ctx, datasetID, err := h.readDatasetId(ctx, req)
	if err != nil {
		return nil, err
	}

	dataset, err := h.datasetsUsecase.ReadPublishedDatasetByID(ctx, datasetID)
	if err != nil {
		log.WithContext(ctx).WithError(err).Errorf("read published dataset by id %s failed", datasetID.String())
		if domainErrors.IsServiceError(err, domainErrors.ErrResourceNotFound) {
			return nil, rest.ErrNotFound().Wrap(err).WithMessage("Dataset not found")
		}
		return nil, rest.ErrInternalServer().Wrap(err).WithMessage("Failed to read dataset")
	}

	datasetBytes, err := h.datasetDTOAdapter.ToDTO(ctx, dataset, pathPublicPublishedDatasets)
	if err != nil {
		log.WithContext(ctx).WithError(err).Errorf("encode published dataset failed for datasetID %s", datasetID.String())
		return nil, rest.ErrInternalServer().Wrap(err).WithMessage("Failed to encode dataset")
	}

	return rest.NewResponseWithPayload(http.StatusOK, datasetBytes), nil
}

func (h *DataRegistryHandlers) ReadPublishedDatasetsByUserID(ctx context.Context, req *http.Request) (rest.APIResponse, error) {
	log.Trace("DataRegistryHandlers ReadPublishedDatasetsByUserID")

	userID, err := uuid.Parse(mux.Vars(req)["userId"])
	if err != nil {
		log.Warnf("Invalid user ID when reading published dataset by user Id: %v, parse error %v", userID, err)
		return nil, rest.ErrBadRequest().Wrap(err).WithMessage("Invalid user ID")
	}
	ctx = context.WithValue(ctx, contextKey("UserID"), userID.String())

	pagination, filters, err := h.requestToPaginationAndFilters(ctx, req)
	if err != nil {
		return nil, err
	}

	ctx = context.WithValue(ctx, contextKey("Pagination"), pagination)
	ctx = context.WithValue(ctx, contextKey("Filters"), filters)

	datasets, count, err := h.datasetsUsecase.ReadPublishedDatasetsByUserID(ctx, userID, *pagination, filters)
	if err != nil {
		log.WithContext(ctx).WithError(err).Errorf("read published datasets for user %s failed", userID.String())
		if domainErrors.IsServiceError(err, domainErrors.ErrResourceNotFound) {
			return nil, rest.ErrNotFound().Wrap(err).WithMessage("Datasets not found")
		}
		return nil, rest.ErrInternalServer().Wrap(err).WithMessage("Failed to read dataset")
	}

	metadata, _ := rest.NewMetadata(ctx, count, *pagination, req.URL.String())
	paginatedResponse := &rest.PaginatedResponse{
		Metadata: metadata,
	}
	if len(datasets) > 0 {
		paginatedResponse.Resources = h.datasetDTOAdapter.ToDTOs(ctx, datasets, pathPublicPublishedDatasets)
	}
	return rest.NewResponseWithPagination(http.StatusOK, paginatedResponse), nil
}

func (h *DataRegistryHandlers) ReadDatasets(ctx context.Context, req *http.Request) (rest.APIResponse, error) {
	log.Trace("DataRegistryHandlers ReadDatasets")

	userID, err := rest.ReadUserIDHeader(ctx, req)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, contextKey("UserID"), userID.String())

	pagination, filters, err := h.requestToPaginationAndFilters(ctx, req)
	if err != nil {
		return nil, err
	}

	ctx = context.WithValue(ctx, contextKey("Pagination"), pagination)
	ctx = context.WithValue(ctx, contextKey("Filters"), filters)

	datasets, count, err := h.datasetsUsecase.ReadDatasetsForUser(ctx, userID, *pagination, filters)
	if err != nil {
		log.WithContext(ctx).WithError(err).Errorf("read datasets for user %s failed", userID.String())
		if domainErrors.IsServiceError(err, domainErrors.ErrResourceNotFound) {
			return nil, rest.ErrNotFound().Wrap(err).WithMessage("Datasets not found")
		}
		return nil, rest.ErrInternalServer().Wrap(err).WithMessage("Failed to read datasets")
	}

	metadata, _ := rest.NewMetadata(ctx, count, *pagination, req.URL.String())
	paginatedResponse := &rest.PaginatedResponse{
		Metadata: metadata,
	}
	if len(datasets) > 0 {
		paginatedResponse.Resources = h.datasetDTOAdapter.ToDTOs(ctx, datasets, pathAuthUserDatasets)
	}
	return rest.NewResponseWithPagination(http.StatusOK, paginatedResponse), nil
}

func (h *DataRegistryHandlers) ReadDatasetByID(ctx context.Context, req *http.Request) (rest.APIResponse, error) {
	log.Trace("DataRegistryHandlers ReadDatasetByID")

	userID, err := rest.ReadUserIDHeader(ctx, req)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, contextKey("UserID"), userID.String())

	ctx, datasetID, err := h.readDatasetId(ctx, req)
	if err != nil {
		return nil, err
	}

	dataset, err := h.datasetsUsecase.ReadDatasetForUser(ctx, datasetID, userID)
	if err != nil {
		log.WithContext(ctx).WithError(err).Errorf("read dataset %s failed for user %s", datasetID.String(), userID.String())
		if domainErrors.IsServiceError(err, domainErrors.ErrResourceNotFound) {
			return nil, rest.ErrNotFound().Wrap(err).WithMessage("Dataset not found")
		}
		return nil, rest.ErrInternalServer().Wrap(err).WithMessage("Failed to read dataset")
	}

	datasetBytes, err := h.datasetDTOAdapter.ToDTO(ctx, dataset, pathAuthUserDatasets)
	if err != nil {
		log.WithContext(ctx).WithError(err).Errorf("encode dataset %s failed for user %s", datasetID.String(), userID.String())
		return nil, rest.ErrInternalServer().Wrap(err).WithMessage("Failed to encode dataset")
	}
	return rest.NewResponseWithPayload(http.StatusOK, datasetBytes), nil
}

func (h *DataRegistryHandlers) DeleteDataset(ctx context.Context, req *http.Request) (rest.APIResponse, error) {
	log.Trace("DataRegistryHandlers DeleteDataset")

	ctx, datasetID, err := h.readDatasetId(ctx, req)
	if err != nil {
		return nil, err
	}

	userID, err := rest.ReadUserIDHeader(ctx, req)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, contextKey("UserID"), userID.String())

	if err := h.datasetsUsecase.DeleteDataset(ctx, datasetID, userID); err != nil {
		if domainErrors.IsServiceError(err, domainErrors.ErrResourceNotFound) {
			return nil, rest.ErrNotFound().Wrap(err).WithMessage("Dataset not found")
		}
		return nil, rest.ErrInternalServer().Wrap(err).WithMessage("Failed to delete dataset")
	}
	return rest.NewReponse(http.StatusOK), nil
}

func (h *DataRegistryHandlers) PublishDataset(ctx context.Context, req *http.Request) (rest.APIResponse, error) {
	log.Trace("DataRegistryHandlers PublishDataset")

	userID, err := rest.ReadUserIDHeader(ctx, req)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, contextKey("UserID"), userID.String())

	ctx, datasetID, err := h.readDatasetId(ctx, req)
	if err != nil {
		return nil, err
	}

	if err := h.datasetsUsecase.PublishDataset(ctx, datasetID, userID); err != nil {
		if domainErrors.IsServiceError(err, domainErrors.ErrResourceNotFound) {
			return nil, rest.ErrNotFound().Wrap(err).WithMessage("Dataset not found")
		}
		return nil, rest.ErrInternalServer().Wrap(err).WithMessage("Failed to publish dataset")
	}

	return rest.NewReponse(http.StatusOK), nil
}

func (h *DataRegistryHandlers) ReplaceDataset(ctx context.Context, req *http.Request) (rest.APIResponse, error) {
	log.Trace("DataRegistryHandlers ReplaceDataset")

	userID, err := rest.ReadUserIDHeader(ctx, req)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, contextKey("UserID"), userID.String())

	ctx, datasetID, err := h.readDatasetId(ctx, req)
	if err != nil {
		return nil, err
	}

	dataset, err := h.requestToDataset(ctx, req)
	if err != nil {
		return nil, err
	}
	dataset.ID = datasetID
	dataset.UserID = userID

	updatedDataset, err := h.datasetsUsecase.ReplaceDataset(ctx, dataset)
	if err != nil {
		if domainErrors.IsServiceError(err, domainErrors.ErrResourceNotFound) {
			return nil, rest.ErrNotFound().Wrap(err).WithMessage("Dataset not found")
		}
		return nil, rest.ErrInternalServer().Wrap(err).WithMessage("Failed to replace dataset")
	}

	datasetBytes, err := h.datasetDTOAdapter.ToDTO(ctx, updatedDataset, pathAuthUserDatasets)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("encode replace-dataset response failed")
		return nil, rest.ErrInternalServer().Wrap(err).WithMessage("Failed to encode dataset")
	}

	return rest.NewResponseWithPayload(http.StatusOK, datasetBytes), nil
}

func (h *DataRegistryHandlers) CreateSourceConnector(ctx context.Context, req *http.Request) (rest.APIResponse, error) {
	log.Trace("DataRegistryHandlers CreateSourceConnector")

	idempotencyKey, err := rest.ReadIdempotencyIDHeader(ctx, req)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, contextKey("Source-connector idempotency key"), idempotencyKey.String())

	userID, err := rest.ReadUserIDHeader(ctx, req)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, contextKey("UserID"), userID.String())

	ctx, storageType, err := h.readStorageType(ctx, req)
	if err != nil {
		return nil, err
	}

	sourceConnector, err := h.fromSourceConnectorDTO(ctx, *storageType, req)
	if err != nil {
		return nil, err
	}

	sourceConnector.UserID = userID
	if err := h.sourceUsecase.CreateSourceConnector(ctx, sourceConnector, idempotencyKey); err != nil {
		if domainErrors.IsServiceError(err, domainErrors.ErrResourceAlreadyExists) {
			return nil, rest.ErrConflict().Wrap(err).WithMessage("Source connector idempotency key already exists")
		}
		return nil, rest.ErrInternalServer().Wrap(err).WithMessage("Failed to create source connector")
	}

	connectorBytes, err := h.toSourceConnectorDTO(ctx, sourceConnector)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("encode create-source-connector response failed")
		return nil, rest.ErrInternalServer().Wrap(err).WithMessage("Failed to encode source connector")
	}

	return rest.NewResponseWithPayload(http.StatusCreated, connectorBytes), nil
}

func (h *DataRegistryHandlers) ReadSourceConnector(ctx context.Context, req *http.Request) (rest.APIResponse, error) {
	log.Trace("DataRegistryHandlers ReadSourceConnector")

	ctx, connectorID, err := h.readConnectorId(ctx, req)
	if err != nil {
		return nil, err
	}

	userID, err := rest.ReadUserIDHeader(ctx, req)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, contextKey("UserID"), userID.String())

	connector, err := h.sourceUsecase.ReadSourceConnector(ctx, connectorID, userID)
	if err != nil {
		if domainErrors.IsServiceError(err, domainErrors.ErrResourceNotFound) {
			return nil, rest.ErrNotFound().Wrap(err).WithMessage("Source connector not found")
		}
		return nil, rest.ErrInternalServer().Wrap(err).WithMessage("Failed to read source connector")
	}

	connectorBytes, err := h.toSourceConnectorDTO(ctx, connector)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("encode read-source-connector response failed")
		return nil, rest.ErrInternalServer().Wrap(err).WithMessage("Failed to encode source connector")
	}

	return rest.NewResponseWithPayload(http.StatusOK, connectorBytes), nil
}

func (h *DataRegistryHandlers) ReplaceSourceConnector(ctx context.Context, req *http.Request) (rest.APIResponse, error) {
	log.Trace("DataRegistryHandlers ReplaceSourceConnector")

	ctx, connectorID, err := h.readConnectorId(ctx, req)
	if err != nil {
		return nil, err
	}

	ctx, storageType, err := h.readStorageType(ctx, req)
	if err != nil {
		return nil, err
	}

	userID, err := rest.ReadUserIDHeader(ctx, req)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, contextKey("UserID"), userID.String())

	sourceConnector, err := h.fromSourceConnectorDTO(ctx, *storageType, req)
	if err != nil {
		return nil, err
	}
	sourceConnector.ID = connectorID
	sourceConnector.UserID = userID

	if err := h.sourceUsecase.ReplaceSourceConnector(ctx, sourceConnector); err != nil {
		if domainErrors.IsServiceError(err, domainErrors.ErrResourceNotFound) {
			return nil, rest.ErrNotFound().Wrap(err).WithMessage("Source connector not found")
		}
		return nil, rest.ErrInternalServer().Wrap(err).WithMessage("Failed to replace source connector")
	}

	connectorBytes, err := h.toSourceConnectorDTO(ctx, sourceConnector)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("encode replace-source-connector response failed")
		return nil, rest.ErrInternalServer().Wrap(err).WithMessage("Failed to encode source connector")
	}

	return rest.NewResponseWithPayload(http.StatusOK, connectorBytes), nil
}

func (h *DataRegistryHandlers) DeleteSourceConnector(ctx context.Context, req *http.Request) (rest.APIResponse, error) {
	log.Trace("DataRegistryHandlers DeleteSourceConnector")

	ctx, connectorID, err := h.readConnectorId(ctx, req)
	if err != nil {
		return nil, err
	}

	userID, err := rest.ReadUserIDHeader(ctx, req)
	if err != nil {
		return nil, err
	}

	if err := h.sourceUsecase.DeleteSourceConnector(ctx, connectorID, userID); err != nil {
		if domainErrors.IsServiceError(err, domainErrors.ErrResourceNotFound) {
			return nil, rest.ErrNotFound().Wrap(err).WithMessage("Source connector not found")
		}
		return nil, rest.ErrInternalServer().Wrap(err).WithMessage("Failed to delete source connector")
	}

	return rest.NewReponse(http.StatusOK), nil
}

func (h *DataRegistryHandlers) fromSourceConnectorDTO(ctx context.Context, storageType model.StorageType, r *http.Request) (*model.SourceConnector, error) {
	log.Trace("DataRegistryHandlers fromSourceConnectorDTO")

	body, err := rest.ReadReqBody(ctx, r)
	if err != nil {
		return nil, err
	}

	sourceConnector, err := h.sourceConnDTOAdapter.FromDTO(ctx, storageType, body)
	if err != nil {
		// The error is always domainErrors.ErrValidationFailed error
		if err.Error() == domainErrors.ErrValidationFailed.Message {
			rest.ErrBadRequest().WithMessage(fmt.Sprintf("Invalid source connector configuration type. Storage type %v is unknown.", storageType))
		}
		return nil, rest.ErrBadRequest().Wrap(err).WithMessage("Validation failed. Malformed source connector configuration.")

	}
	return sourceConnector, nil
}

func (h *DataRegistryHandlers) toSourceConnectorDTO(ctx context.Context, connector *model.SourceConnector) ([]byte, error) {
	log.Trace("DataRegistryHandlers toSourceConnectorDTO")

	connectorBytes, err := h.sourceConnDTOAdapter.ToDTO(ctx, connector)
	if err != nil {
		return nil, rest.ErrInternalServer().Wrap(err).WithMessage("Invalid source connector configuration")
	}

	return connectorBytes, err
}

func (h *DataRegistryHandlers) requestToDataset(ctx context.Context, r *http.Request) (*model.Dataset, error) {
	log.Trace("DataRegistryHandlers parseDatasetPayload")

	body, err := rest.ReadReqBody(ctx, r)
	if err != nil {
		return nil, err
	}

	dataset, err := h.datasetDTOAdapter.FromDTO(ctx, body)
	if err != nil {
		return nil, rest.ErrBadRequest().Wrap(err).WithMessage("Invalid dataset")
	}
	return dataset, nil
}

func (h *DataRegistryHandlers) requestToPaginationAndFilters(ctx context.Context, r *http.Request) (*core.Pagination, []model.Filter, error) {
	log.Trace("DataRegistryHandlers requestToPaginationAndFilters")

	pagination, queryFilters, err := rest.ReadPaginationAndFilters(ctx, r)
	if err != nil {
		return nil, nil, err
	}

	filters, err := h.filterAdapter.QueryFiltersToDatasetsFilters(ctx, queryFilters)
	if err != nil {
		return nil, nil, rest.ErrBadRequest().Wrap(err).WithMessage("Invalid datasets filters")
	}

	return pagination, filters, nil
}

func (h *DataRegistryHandlers) readDatasetId(ctx context.Context, req *http.Request) (context.Context, uuid.UUID, error) {
	log.Trace("DataRegistryHandlers readDatasetId")

	datasetID, err := uuid.Parse(mux.Vars(req)["datasetId"])
	if err != nil {
		log.Warnf("Invalid dataset ID: %v", err)
		return nil, uuid.Nil, rest.ErrBadRequest().Wrap(err).WithMessage("Invalid dataset ID")
	}
	ctx2 := context.WithValue(ctx, contextKey("DatasetID"), datasetID)
	return ctx2, datasetID, nil
}

func (h *DataRegistryHandlers) readConnectorId(ctx context.Context, req *http.Request) (context.Context, uuid.UUID, error) {
	log.Trace("DataRegistryHandlers readConnectorId")

	connectorID, err := uuid.Parse(mux.Vars(req)["connectorId"])
	if err != nil {
		log.Warnf("Invalid connector ID: %v", err)
		return nil, uuid.Nil, rest.ErrBadRequest().Wrap(err).WithMessage("Invalid connector ID")
	}
	ctx2 := context.WithValue(ctx, contextKey("ConnectorID"), connectorID)
	return ctx2, connectorID, nil
}

func (h *DataRegistryHandlers) readStorageType(ctx context.Context, req *http.Request) (context.Context, *model.StorageType, error) {
	log.Trace("DataRegistryHandlers readStorageType")

	connectorType := mux.Vars(req)["type"]
	if connectorType == "" {
		log.Warn("Invalid connector type")
		return nil, nil, rest.ErrBadRequest().WithMessage("Invalid connector type")
	}

	// Azure has an underscore in the storage type, which we avoid in the API endpoint URL.
	if connectorType == "azure" {
		connectorType = "AZURE_STORAGE"
	}

	storageType, err := model.ToStorageType(connectorType)
	if err != nil {
		log.Warnf("Invalid storage type: %v", err)
		return nil, nil, rest.ErrBadRequest().Wrap(err).WithMessage("Invalid storage type")
	}

	ctx2 := context.WithValue(ctx, contextKey("StorageType"), storageType.String())
	return ctx2, &storageType, nil
}
