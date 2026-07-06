package rest

import (
	"context"
	app "data_registry_service/pkg/app"
	"data_registry_service/pkg/domain/model"
	"data_registry_service/pkg/infra/network/adapter"
	"fmt"

	domainErrors "data_registry_service/pkg/domain"
	"lib/shared_lib/ctxutil"
	core "lib/shared_lib/transport"
	"net/http"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
)

type contextKey string

const (
	pathAuthUserDatasets = "/v1/data/registry"
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

func (h *DataRegistryHandlers) GetRoutes() []Route {
	log.Trace("DataRegistryHandlers GetRoutes")

	return []Route{
		{
			Path:     "/v1/data/registry",
			Handler:  h.CreateDataset,
			Method:   http.MethodPost,
			SpanName: "create-dataset",
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
			Path:     "/v1/data/registry/{datasetId}/materialization",
			Handler:  h.ReadDatasetMaterialization,
			Method:   http.MethodGet,
			SpanName: "read-authenticated-user-dataset-materialization",
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

func (h *DataRegistryHandlers) CreateDataset(ctx context.Context, req *http.Request) (APIResponse, error) {
	log.Trace("DataRegistryHandlers CreateDataset")

	idempotencyKey, err := ReadIdempotencyIDHeader(ctx, req)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, contextKey("Dataset idempotency key"), idempotencyKey.String())

	userID, err := ReadUserIDHeader(ctx, req)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, contextKey("UserID"), userID.String())
	ctx = ctxutil.WithTenantID(ctx, userID)

	dataset, err := h.requestToDataset(ctx, req)
	if err != nil {
		return nil, err
	}

	dataset.UserID = userID
	if err := h.datasetsUsecase.CreateDataset(ctx, dataset, idempotencyKey); err != nil {
		if domainErrors.IsServiceError(err, domainErrors.ErrResourceAlreadyExists) {
			return nil, ErrConflict().Wrap(err).WithMessage("Dataset idempotency key already exists")
		}
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to create dataset")
	}

	datasetBytes, err := h.datasetDTOAdapter.ToDTO(ctx, dataset, pathAuthUserDatasets)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("encode create-dataset response failed")
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to encode dataset")
	}

	return NewResponseWithPayload(http.StatusCreated, datasetBytes), nil
}

func (h *DataRegistryHandlers) ReadDatasets(ctx context.Context, req *http.Request) (APIResponse, error) {
	log.Trace("DataRegistryHandlers ReadDatasets")

	userID, err := ReadUserIDHeader(ctx, req)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, contextKey("UserID"), userID.String())
	ctx = ctxutil.WithTenantID(ctx, userID)

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
			return nil, ErrNotFound().Wrap(err).WithMessage("Datasets not found")
		}
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to read datasets")
	}

	metadata, _ := NewMetadata(ctx, count, *pagination, req.URL.String())
	paginatedResponse := &PaginatedResponse{
		Metadata: metadata,
	}
	if len(datasets) > 0 {
		paginatedResponse.Resources = h.datasetDTOAdapter.ToDTOs(ctx, datasets, pathAuthUserDatasets)
	}
	return NewResponseWithPagination(http.StatusOK, paginatedResponse), nil
}

func (h *DataRegistryHandlers) ReadDatasetByID(ctx context.Context, req *http.Request) (APIResponse, error) {
	log.Trace("DataRegistryHandlers ReadDatasetByID")

	userID, err := ReadUserIDHeader(ctx, req)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, contextKey("UserID"), userID.String())
	ctx = ctxutil.WithTenantID(ctx, userID)

	ctx, datasetID, err := h.readDatasetId(ctx, req)
	if err != nil {
		return nil, err
	}

	dataset, err := h.datasetsUsecase.ReadDatasetForUser(ctx, datasetID, userID)
	if err != nil {
		log.WithContext(ctx).WithError(err).Errorf("read dataset %s failed for user %s", datasetID.String(), userID.String())
		if domainErrors.IsServiceError(err, domainErrors.ErrResourceNotFound) {
			return nil, ErrNotFound().Wrap(err).WithMessage("Dataset not found")
		}
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to read dataset")
	}

	datasetBytes, err := h.datasetDTOAdapter.ToDTO(ctx, dataset, pathAuthUserDatasets)
	if err != nil {
		log.WithContext(ctx).WithError(err).Errorf("encode dataset %s failed for user %s", datasetID.String(), userID.String())
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to encode dataset")
	}
	return NewResponseWithPayload(http.StatusOK, datasetBytes), nil
}

func (h *DataRegistryHandlers) ReadDatasetMaterialization(ctx context.Context, req *http.Request) (APIResponse, error) {
	log.Trace("DataRegistryHandlers ReadDatasetMaterialization")

	userID, err := ReadUserIDHeader(ctx, req)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, contextKey("UserID"), userID.String())
	ctx = ctxutil.WithTenantID(ctx, userID)

	ctx, datasetID, err := h.readDatasetId(ctx, req)
	if err != nil {
		return nil, err
	}

	dataset, err := h.datasetsUsecase.ReadDatasetTable(ctx, datasetID, userID, "")
	if err != nil {
		log.WithContext(ctx).WithError(err).Errorf("read dataset materialization %s failed for user %s", datasetID.String(), userID.String())
		if domainErrors.IsServiceError(err, domainErrors.ErrResourceNotFound) {
			return nil, ErrNotFound().Wrap(err).WithMessage("Dataset not found")
		}
		if domainErrors.IsServiceError(err, domainErrors.ErrValidationFailed) {
			return nil, ErrBadRequest().Wrap(err).WithMessage("Dataset is not materialized")
		}
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to read dataset materialization")
	}

	datasetBytes, err := h.datasetDTOAdapter.ToDTO(ctx, dataset, pathAuthUserDatasets)
	if err != nil {
		log.WithContext(ctx).WithError(err).Errorf("encode dataset materialization %s failed for user %s", datasetID.String(), userID.String())
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to encode dataset materialization")
	}
	return NewResponseWithPayload(http.StatusOK, datasetBytes), nil
}

func (h *DataRegistryHandlers) DeleteDataset(ctx context.Context, req *http.Request) (APIResponse, error) {
	log.Trace("DataRegistryHandlers DeleteDataset")

	ctx, datasetID, err := h.readDatasetId(ctx, req)
	if err != nil {
		return nil, err
	}

	userID, err := ReadUserIDHeader(ctx, req)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, contextKey("UserID"), userID.String())
	ctx = ctxutil.WithTenantID(ctx, userID)

	if err := h.datasetsUsecase.DeleteDataset(ctx, datasetID, userID); err != nil {
		if domainErrors.IsServiceError(err, domainErrors.ErrResourceNotFound) {
			return nil, ErrNotFound().Wrap(err).WithMessage("Dataset not found")
		}
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to delete dataset")
	}
	return NewReponse(http.StatusOK), nil
}

func (h *DataRegistryHandlers) PublishDataset(ctx context.Context, req *http.Request) (APIResponse, error) {
	log.Trace("DataRegistryHandlers PublishDataset")

	userID, err := ReadUserIDHeader(ctx, req)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, contextKey("UserID"), userID.String())
	ctx = ctxutil.WithTenantID(ctx, userID)

	ctx, datasetID, err := h.readDatasetId(ctx, req)
	if err != nil {
		return nil, err
	}

	if err := h.datasetsUsecase.PublishDataset(ctx, datasetID, userID); err != nil {
		if domainErrors.IsServiceError(err, domainErrors.ErrResourceNotFound) {
			return nil, ErrNotFound().Wrap(err).WithMessage("Dataset not found")
		}
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to publish dataset")
	}

	return NewReponse(http.StatusOK), nil
}

func (h *DataRegistryHandlers) ReplaceDataset(ctx context.Context, req *http.Request) (APIResponse, error) {
	log.Trace("DataRegistryHandlers ReplaceDataset")

	userID, err := ReadUserIDHeader(ctx, req)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, contextKey("UserID"), userID.String())
	ctx = ctxutil.WithTenantID(ctx, userID)

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
			return nil, ErrNotFound().Wrap(err).WithMessage("Dataset not found")
		}
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to replace dataset")
	}

	datasetBytes, err := h.datasetDTOAdapter.ToDTO(ctx, updatedDataset, pathAuthUserDatasets)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("encode replace-dataset response failed")
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to encode dataset")
	}

	return NewResponseWithPayload(http.StatusOK, datasetBytes), nil
}

func (h *DataRegistryHandlers) CreateSourceConnector(ctx context.Context, req *http.Request) (APIResponse, error) {
	log.Trace("DataRegistryHandlers CreateSourceConnector")

	idempotencyKey, err := ReadIdempotencyIDHeader(ctx, req)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, contextKey("Source-connector idempotency key"), idempotencyKey.String())

	userID, err := ReadUserIDHeader(ctx, req)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, contextKey("UserID"), userID.String())
	ctx = ctxutil.WithTenantID(ctx, userID)

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
			return nil, ErrConflict().Wrap(err).WithMessage("Source connector idempotency key already exists")
		}
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to create source connector")
	}

	connectorBytes, err := h.toSourceConnectorDTO(ctx, sourceConnector)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("encode create-source-connector response failed")
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to encode source connector")
	}

	return NewResponseWithPayload(http.StatusCreated, connectorBytes), nil
}

func (h *DataRegistryHandlers) ReadSourceConnector(ctx context.Context, req *http.Request) (APIResponse, error) {
	log.Trace("DataRegistryHandlers ReadSourceConnector")

	ctx, connectorID, err := h.readConnectorId(ctx, req)
	if err != nil {
		return nil, err
	}

	userID, err := ReadUserIDHeader(ctx, req)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, contextKey("UserID"), userID.String())
	ctx = ctxutil.WithTenantID(ctx, userID)

	connector, err := h.sourceUsecase.ReadSourceConnector(ctx, connectorID, userID)
	if err != nil {
		if domainErrors.IsServiceError(err, domainErrors.ErrResourceNotFound) {
			return nil, ErrNotFound().Wrap(err).WithMessage("Source connector not found")
		}
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to read source connector")
	}

	connectorBytes, err := h.toSourceConnectorDTO(ctx, connector)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("encode read-source-connector response failed")
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to encode source connector")
	}

	return NewResponseWithPayload(http.StatusOK, connectorBytes), nil
}

func (h *DataRegistryHandlers) ReplaceSourceConnector(ctx context.Context, req *http.Request) (APIResponse, error) {
	log.Trace("DataRegistryHandlers ReplaceSourceConnector")

	ctx, connectorID, err := h.readConnectorId(ctx, req)
	if err != nil {
		return nil, err
	}

	ctx, storageType, err := h.readStorageType(ctx, req)
	if err != nil {
		return nil, err
	}

	userID, err := ReadUserIDHeader(ctx, req)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, contextKey("UserID"), userID.String())
	ctx = ctxutil.WithTenantID(ctx, userID)

	sourceConnector, err := h.fromSourceConnectorDTO(ctx, *storageType, req)
	if err != nil {
		return nil, err
	}
	sourceConnector.ID = connectorID
	sourceConnector.UserID = userID

	if err := h.sourceUsecase.ReplaceSourceConnector(ctx, sourceConnector); err != nil {
		if domainErrors.IsServiceError(err, domainErrors.ErrResourceNotFound) {
			return nil, ErrNotFound().Wrap(err).WithMessage("Source connector not found")
		}
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to replace source connector")
	}

	connectorBytes, err := h.toSourceConnectorDTO(ctx, sourceConnector)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("encode replace-source-connector response failed")
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to encode source connector")
	}

	return NewResponseWithPayload(http.StatusOK, connectorBytes), nil
}

func (h *DataRegistryHandlers) DeleteSourceConnector(ctx context.Context, req *http.Request) (APIResponse, error) {
	log.Trace("DataRegistryHandlers DeleteSourceConnector")

	ctx, connectorID, err := h.readConnectorId(ctx, req)
	if err != nil {
		return nil, err
	}

	userID, err := ReadUserIDHeader(ctx, req)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, contextKey("UserID"), userID.String())
	ctx = ctxutil.WithTenantID(ctx, userID)

	if err := h.sourceUsecase.DeleteSourceConnector(ctx, connectorID, userID); err != nil {
		if domainErrors.IsServiceError(err, domainErrors.ErrResourceNotFound) {
			return nil, ErrNotFound().Wrap(err).WithMessage("Source connector not found")
		}
		return nil, ErrInternalServer().Wrap(err).WithMessage("Failed to delete source connector")
	}

	return NewReponse(http.StatusOK), nil
}

func (h *DataRegistryHandlers) fromSourceConnectorDTO(ctx context.Context, storageType model.StorageType, r *http.Request) (*model.SourceConnector, error) {
	log.Trace("DataRegistryHandlers fromSourceConnectorDTO")

	body, err := ReadReqBody(ctx, r)
	if err != nil {
		return nil, err
	}

	sourceConnector, err := h.sourceConnDTOAdapter.FromDTO(ctx, storageType, body)
	if err != nil {
		// The error is always domainErrors.ErrValidationFailed error
		if err.Error() == domainErrors.ErrValidationFailed.Message {
			ErrBadRequest().WithMessage(fmt.Sprintf("Invalid source connector configuration type. Storage type %v is unknown.", storageType))
		}
		return nil, ErrBadRequest().Wrap(err).WithMessage("Validation failed. Malformed source connector configuration.")

	}
	return sourceConnector, nil
}

func (h *DataRegistryHandlers) toSourceConnectorDTO(ctx context.Context, connector *model.SourceConnector) ([]byte, error) {
	log.Trace("DataRegistryHandlers toSourceConnectorDTO")

	connectorBytes, err := h.sourceConnDTOAdapter.ToDTO(ctx, connector)
	if err != nil {
		return nil, ErrInternalServer().Wrap(err).WithMessage("Invalid source connector configuration")
	}

	return connectorBytes, err
}

func (h *DataRegistryHandlers) requestToDataset(ctx context.Context, r *http.Request) (*model.Dataset, error) {
	log.Trace("DataRegistryHandlers parseDatasetPayload")

	body, err := ReadReqBody(ctx, r)
	if err != nil {
		return nil, err
	}

	dataset, err := h.datasetDTOAdapter.FromDTO(ctx, body)
	if err != nil {
		return nil, ErrBadRequest().Wrap(err).WithMessage("Invalid dataset")
	}
	return dataset, nil
}

func (h *DataRegistryHandlers) requestToPaginationAndFilters(ctx context.Context, r *http.Request) (*core.Pagination, []model.Filter, error) {
	log.Trace("DataRegistryHandlers requestToPaginationAndFilters")

	pagination, queryFilters, err := ReadPaginationAndFilters(ctx, r)
	if err != nil {
		return nil, nil, err
	}

	filters, err := h.filterAdapter.QueryFiltersToDatasetsFilters(ctx, queryFilters)
	if err != nil {
		return nil, nil, ErrBadRequest().Wrap(err).WithMessage("Invalid datasets filters")
	}

	return pagination, filters, nil
}

func (h *DataRegistryHandlers) readDatasetId(ctx context.Context, req *http.Request) (context.Context, uuid.UUID, error) {
	log.Trace("DataRegistryHandlers readDatasetId")

	datasetID, err := uuid.Parse(mux.Vars(req)["datasetId"])
	if err != nil {
		log.Warnf("Invalid dataset ID: %v", err)
		return nil, uuid.Nil, ErrBadRequest().Wrap(err).WithMessage("Invalid dataset ID")
	}
	ctx2 := context.WithValue(ctx, contextKey("DatasetID"), datasetID)
	return ctx2, datasetID, nil
}

func (h *DataRegistryHandlers) readConnectorId(ctx context.Context, req *http.Request) (context.Context, uuid.UUID, error) {
	log.Trace("DataRegistryHandlers readConnectorId")

	connectorID, err := uuid.Parse(mux.Vars(req)["connectorId"])
	if err != nil {
		log.Warnf("Invalid connector ID: %v", err)
		return nil, uuid.Nil, ErrBadRequest().Wrap(err).WithMessage("Invalid connector ID")
	}
	ctx2 := context.WithValue(ctx, contextKey("ConnectorID"), connectorID)
	return ctx2, connectorID, nil
}

func (h *DataRegistryHandlers) readStorageType(ctx context.Context, req *http.Request) (context.Context, *model.StorageType, error) {
	log.Trace("DataRegistryHandlers readStorageType")

	connectorType := mux.Vars(req)["type"]
	if connectorType == "" {
		log.Warn("Invalid connector type")
		return nil, nil, ErrBadRequest().WithMessage("Invalid connector type")
	}

	// Azure has an underscore in the storage type, which we avoid in the API endpoint URL.
	if connectorType == "azure" {
		connectorType = "AZURE_STORAGE"
	}

	storageType, err := model.ToStorageType(connectorType)
	if err != nil {
		log.Warnf("Invalid storage type: %v", err)
		return nil, nil, ErrBadRequest().Wrap(err).WithMessage("Invalid storage type")
	}

	ctx2 := context.WithValue(ctx, contextKey("StorageType"), storageType.String())
	return ctx2, &storageType, nil
}
