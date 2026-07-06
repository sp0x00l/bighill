package rest

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	domainErrors "data_registry_service/pkg/domain"
	"data_registry_service/pkg/domain/model"
	"data_registry_service/pkg/infra/network/adapter"
	"lib/shared_lib/ctxutil"
	serializers "lib/shared_lib/serializer"
	transport "lib/shared_lib/transport"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestRest(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Data registry REST unit test suite")
}

type datasetUsecaseStub struct {
	createDataset        *model.Dataset
	createIdempotencyKey uuid.UUID
	createErr            error

	readDatasetID uuid.UUID
	readUserID    uuid.UUID
	readDataset   *model.Dataset
	readErr       error

	readTableDatasetID uuid.UUID
	readTableUserID    uuid.UUID
	readTableFormat    string
	readTableDataset   *model.Dataset
	readTableErr       error

	readManyUserID     uuid.UUID
	readManyPagination transport.Pagination
	readManyFilters    []model.Filter
	readManyDatasets   []*model.Dataset
	readManyTotal      int
	readManyErr        error

	deleteDatasetID uuid.UUID
	deleteUserID    uuid.UUID
	deleteErr       error

	publishDatasetID uuid.UUID
	publishUserID    uuid.UUID
	publishErr       error

	replaceDataset *model.Dataset
	replaceResult  *model.Dataset
	replaceErr     error
}

func (s *datasetUsecaseStub) CreateDataset(_ context.Context, dataset *model.Dataset, idempotencyKey uuid.UUID) error {
	s.createDataset = dataset
	s.createIdempotencyKey = idempotencyKey
	return s.createErr
}

func (s *datasetUsecaseStub) ReadDatasetsForUser(_ context.Context, userID uuid.UUID, pagination transport.Pagination, filters []model.Filter) ([]*model.Dataset, int, error) {
	s.readManyUserID = userID
	s.readManyPagination = pagination
	s.readManyFilters = filters
	return s.readManyDatasets, s.readManyTotal, s.readManyErr
}

func (s *datasetUsecaseStub) ReadDatasetForUser(_ context.Context, datasetID uuid.UUID, userID uuid.UUID) (*model.Dataset, error) {
	s.readDatasetID = datasetID
	s.readUserID = userID
	return s.readDataset, s.readErr
}

func (s *datasetUsecaseStub) ReadDatasetTable(_ context.Context, datasetID uuid.UUID, userID uuid.UUID, tableFormat string) (*model.Dataset, error) {
	s.readTableDatasetID = datasetID
	s.readTableUserID = userID
	s.readTableFormat = tableFormat
	return s.readTableDataset, s.readTableErr
}

func (s *datasetUsecaseStub) DeleteDataset(_ context.Context, datasetID uuid.UUID, userID uuid.UUID) error {
	s.deleteDatasetID = datasetID
	s.deleteUserID = userID
	return s.deleteErr
}

func (s *datasetUsecaseStub) PublishDataset(_ context.Context, datasetID uuid.UUID, userID uuid.UUID) error {
	s.publishDatasetID = datasetID
	s.publishUserID = userID
	return s.publishErr
}

func (s *datasetUsecaseStub) ReplaceDataset(_ context.Context, dataset *model.Dataset) (*model.Dataset, error) {
	s.replaceDataset = dataset
	if s.replaceResult != nil {
		return s.replaceResult, s.replaceErr
	}
	return dataset, s.replaceErr
}

func (s *datasetUsecaseStub) AdvanceDatasetProcessingState(context.Context, uuid.UUID, uuid.UUID, model.ProcessingState) (*model.Dataset, error) {
	return nil, nil
}

func (s *datasetUsecaseStub) RecordDatasetMaterialization(context.Context, *model.Dataset, model.ProcessingState) (*model.Dataset, error) {
	return nil, nil
}

type sourceUsecaseStub struct {
	createConnector      *model.SourceConnector
	createIdempotencyKey uuid.UUID
	createErr            error

	readConnectorID uuid.UUID
	readUserID      uuid.UUID
	readConnector   *model.SourceConnector
	readErr         error

	replaceConnector *model.SourceConnector
	replaceErr       error

	deleteConnectorID uuid.UUID
	deleteUserID      uuid.UUID
	deleteTenantID    uuid.UUID
	deleteErr         error
}

func (s *sourceUsecaseStub) CreateSourceConnector(_ context.Context, connector *model.SourceConnector, idempotencyKey uuid.UUID) error {
	s.createConnector = connector
	s.createIdempotencyKey = idempotencyKey
	if connector.ID == uuid.Nil {
		connector.ID = uuid.New()
	}
	return s.createErr
}

func (s *sourceUsecaseStub) ReadSourceConnector(_ context.Context, connectorID, userID uuid.UUID) (*model.SourceConnector, error) {
	s.readConnectorID = connectorID
	s.readUserID = userID
	return s.readConnector, s.readErr
}

func (s *sourceUsecaseStub) ReplaceSourceConnector(_ context.Context, connector *model.SourceConnector) error {
	s.replaceConnector = connector
	return s.replaceErr
}

func (s *sourceUsecaseStub) DeleteSourceConnector(ctx context.Context, connectorID, userID uuid.UUID) error {
	s.deleteConnectorID = connectorID
	s.deleteUserID = userID
	if tenantID, ok := ctxutil.TenantID(ctx); ok {
		s.deleteTenantID = tenantID
	}
	return s.deleteErr
}

var _ = Describe("DataRegistryHandlers", func() {
	var (
		ctx       context.Context
		datasets  *datasetUsecaseStub
		sources   *sourceUsecaseStub
		handlers  *DataRegistryHandlers
		userID    uuid.UUID
		requestID uuid.UUID
		datasetID uuid.UUID
	)

	BeforeEach(func() {
		ctx = context.Background()
		datasets = &datasetUsecaseStub{}
		sources = &sourceUsecaseStub{}
		encoder := serializers.NewJSONSerializer()
		handlers = NewDataRegistryHandlers(
			datasets,
			sources,
			adapter.NewDatasetDTOAdapter(encoder),
			adapter.NewRestSourceConnDTOAdapter(adapter.GetConnCfgToDTOFunc, adapter.GetConnCfgFromDTOFunc, encoder),
			adapter.NewFilterDTOAdapter(),
		)
		userID = uuid.New()
		requestID = uuid.New()
		datasetID = uuid.New()
	})

	It("creates datasets from REST DTOs", func() {
		req := newJSONRequest(http.MethodPost, "/v1/data/registry", `{"title":"Movies","category":"rag"}`, userID, requestID)

		res, err := handlers.CreateDataset(ctx, req)

		Expect(err).NotTo(HaveOccurred())
		Expect(res.StatusCode()).To(Equal(http.StatusCreated))
		Expect(datasets.createIdempotencyKey).To(Equal(requestID))
		Expect(datasets.createDataset.UserID).To(Equal(userID))
		Expect(datasets.createDataset.Title).To(Equal("Movies"))
		Expect(datasets.createDataset.Category).To(Equal("rag"))
	})

	It("exposes all REST routes for the service router", func() {
		routes := handlers.GetRoutes()

		Expect(routes).To(HaveLen(11))
		Expect(routeExists(routes, "/v1/data/registry", http.MethodPost)).To(BeTrue())
		Expect(routeExists(routes, "/v1/data/registry/{datasetId}/materialization", http.MethodGet)).To(BeTrue())
		Expect(routeExists(routes, "/v1/data/registry/connector/{type}/{connectorId}", http.MethodDelete)).To(BeTrue())
	})

	It("rejects malformed dataset DTOs", func() {
		req := newJSONRequest(http.MethodPost, "/v1/data/registry", `{"description":"missing title"}`, userID, requestID)

		_, err := handlers.CreateDataset(ctx, req)

		var httpErr *HTTPError
		Expect(errors.As(err, &httpErr)).To(BeTrue())
		Expect(httpErr.statusCode).To(Equal(http.StatusBadRequest))
	})

	It("maps missing datasets to a not-found response", func() {
		datasets.readErr = domainErrors.ErrResourceNotFound
		req := newJSONRequest(http.MethodGet, "/v1/data/registry/"+datasetID.String(), `{}`, userID, uuid.Nil)
		req = mux.SetURLVars(req, map[string]string{"datasetId": datasetID.String()})

		_, err := handlers.ReadDatasetByID(ctx, req)

		var httpErr *HTTPError
		Expect(errors.As(err, &httpErr)).To(BeTrue())
		Expect(httpErr.statusCode).To(Equal(http.StatusNotFound))
		Expect(datasets.readDatasetID).To(Equal(datasetID))
		Expect(datasets.readUserID).To(Equal(userID))
	})

	It("reads dataset materialization for the authenticated user", func() {
		featureSnapshotID := uuid.New()
		datasets.readTableDataset = &model.Dataset{
			ID:                datasetID,
			UserID:            userID,
			Title:             "Movies",
			DatasetVersion:    4,
			FeatureSnapshotID: featureSnapshotID,
			Location:          "s3://lakehouse/features/movies.parquet",
			TableName:         "movies",
			TableFormat:       model.Parquet,
			ProcessingState:   model.DatasetProcessingFeatureMaterialized,
		}
		req := newJSONRequest(http.MethodGet, "/v1/data/registry/"+datasetID.String()+"/materialization", `{}`, userID, uuid.Nil)
		req = mux.SetURLVars(req, map[string]string{"datasetId": datasetID.String()})

		res, err := handlers.ReadDatasetMaterialization(ctx, req)

		Expect(err).NotTo(HaveOccurred())
		Expect(res.StatusCode()).To(Equal(http.StatusOK))
		Expect(datasets.readTableDatasetID).To(Equal(datasetID))
		Expect(datasets.readTableUserID).To(Equal(userID))
		Expect(datasets.readTableFormat).To(BeEmpty())
		Expect(res.Payload()).To(ContainSubstring(featureSnapshotID.String()))
		Expect(res.Payload()).To(ContainSubstring("s3://lakehouse/features/movies.parquet"))
	})

	It("maps missing dataset materialization to not found", func() {
		datasets.readTableErr = domainErrors.ErrResourceNotFound
		req := newJSONRequest(http.MethodGet, "/v1/data/registry/"+datasetID.String()+"/materialization", `{}`, userID, uuid.Nil)
		req = mux.SetURLVars(req, map[string]string{"datasetId": datasetID.String()})

		_, err := handlers.ReadDatasetMaterialization(ctx, req)

		var httpErr *HTTPError
		Expect(errors.As(err, &httpErr)).To(BeTrue())
		Expect(httpErr.statusCode).To(Equal(http.StatusNotFound))
		Expect(datasets.readTableDatasetID).To(Equal(datasetID))
		Expect(datasets.readTableUserID).To(Equal(userID))
	})

	It("lists datasets for the authenticated user", func() {
		datasets.readManyDatasets = []*model.Dataset{{
			ID:     datasetID,
			UserID: userID,
			Title:  "Movies",
		}}
		datasets.readManyTotal = 1
		req := newJSONRequest(http.MethodGet, "/v1/data/registry?page=1&limit=10", `{}`, userID, uuid.Nil)

		res, err := handlers.ReadDatasets(ctx, req)

		Expect(err).NotTo(HaveOccurred())
		Expect(res.StatusCode()).To(Equal(http.StatusOK))
		Expect(datasets.readManyUserID).To(Equal(userID))
		Expect(datasets.readManyPagination.Page).To(Equal(1))
		Expect(datasets.readManyPagination.Limit).To(Equal(10))
		Expect(datasets.readManyFilters).To(BeEmpty())
		Expect(res.Payload()).NotTo(BeEmpty())
	})

	It("maps missing dataset lists to not found", func() {
		datasets.readManyErr = domainErrors.ErrResourceNotFound
		req := newJSONRequest(http.MethodGet, "/v1/data/registry", `{}`, userID, uuid.Nil)

		_, err := handlers.ReadDatasets(ctx, req)

		var httpErr *HTTPError
		Expect(errors.As(err, &httpErr)).To(BeTrue())
		Expect(httpErr.statusCode).To(Equal(http.StatusNotFound))
	})

	It("replaces datasets from REST DTOs", func() {
		datasets.replaceResult = &model.Dataset{ID: datasetID, UserID: userID, Title: "Updated", Category: "rag"}
		req := newJSONRequest(http.MethodPut, "/v1/data/registry/"+datasetID.String(), `{"title":"Updated","category":"rag"}`, userID, uuid.Nil)
		req = mux.SetURLVars(req, map[string]string{"datasetId": datasetID.String()})

		res, err := handlers.ReplaceDataset(ctx, req)

		Expect(err).NotTo(HaveOccurred())
		Expect(res.StatusCode()).To(Equal(http.StatusOK))
		Expect(datasets.replaceDataset.ID).To(Equal(datasetID))
		Expect(datasets.replaceDataset.UserID).To(Equal(userID))
		Expect(datasets.replaceDataset.Title).To(Equal("Updated"))
	})

	It("publishes datasets for the authenticated user", func() {
		req := newJSONRequest(http.MethodPatch, "/v1/data/registry/"+datasetID.String()+"/publish", `{}`, userID, uuid.Nil)
		req = mux.SetURLVars(req, map[string]string{"datasetId": datasetID.String()})

		res, err := handlers.PublishDataset(ctx, req)

		Expect(err).NotTo(HaveOccurred())
		Expect(res.StatusCode()).To(Equal(http.StatusOK))
		Expect(datasets.publishDatasetID).To(Equal(datasetID))
		Expect(datasets.publishUserID).To(Equal(userID))
	})

	It("deletes datasets for the authenticated user", func() {
		req := newJSONRequest(http.MethodDelete, "/v1/data/registry/"+datasetID.String(), `{}`, userID, uuid.Nil)
		req = mux.SetURLVars(req, map[string]string{"datasetId": datasetID.String()})

		res, err := handlers.DeleteDataset(ctx, req)

		Expect(err).NotTo(HaveOccurred())
		Expect(res.StatusCode()).To(Equal(http.StatusOK))
		Expect(datasets.deleteDatasetID).To(Equal(datasetID))
		Expect(datasets.deleteUserID).To(Equal(userID))
	})

	It("creates source connectors from REST DTOs", func() {
		body := `{
			"config":{
				"hostname":"localhost",
				"port":5432,
				"databaseName":"mlops",
				"username":"postgres",
				"password":"password",
				"authenticationType":"MASTER"
			}
		}`
		req := newJSONRequest(http.MethodPost, "/v1/data/registry/connector/POSTGRES", body, userID, requestID)
		req = mux.SetURLVars(req, map[string]string{"type": "POSTGRES"})

		res, err := handlers.CreateSourceConnector(ctx, req)

		Expect(err).NotTo(HaveOccurred())
		Expect(res.StatusCode()).To(Equal(http.StatusCreated))
		Expect(sources.createIdempotencyKey).To(Equal(requestID))
		Expect(sources.createConnector.UserID).To(Equal(userID))
		Expect(sources.createConnector.Config.GetStorageType()).To(Equal(model.Postgres))
	})

	It("reads source connectors for the authenticated user", func() {
		connectorID := uuid.New()
		sources.readConnector = &model.SourceConnector{
			ID:     connectorID,
			UserID: userID,
			Config: &model.PostgresDBConnCfg{
				Hostname:           "localhost",
				Port:               5432,
				DatabaseName:       "mlops",
				Username:           "postgres",
				Password:           "password",
				AuthenticationType: model.Master,
			},
		}
		req := newJSONRequest(http.MethodGet, "/v1/data/registry/connector/POSTGRES/"+connectorID.String(), `{}`, userID, uuid.Nil)
		req = mux.SetURLVars(req, map[string]string{"type": "POSTGRES", "connectorId": connectorID.String()})

		res, err := handlers.ReadSourceConnector(ctx, req)

		Expect(err).NotTo(HaveOccurred())
		Expect(res.StatusCode()).To(Equal(http.StatusOK))
		Expect(sources.readConnectorID).To(Equal(connectorID))
		Expect(sources.readUserID).To(Equal(userID))
	})

	It("replaces source connectors for the authenticated user", func() {
		connectorID := uuid.New()
		body := `{
			"config":{
				"hostname":"localhost",
				"port":5432,
				"databaseName":"mlops",
				"username":"postgres",
				"password":"password",
				"authenticationType":"MASTER"
			}
		}`
		req := newJSONRequest(http.MethodPut, "/v1/data/registry/connector/POSTGRES/"+connectorID.String(), body, userID, uuid.Nil)
		req = mux.SetURLVars(req, map[string]string{"type": "POSTGRES", "connectorId": connectorID.String()})

		res, err := handlers.ReplaceSourceConnector(ctx, req)

		Expect(err).NotTo(HaveOccurred())
		Expect(res.StatusCode()).To(Equal(http.StatusOK))
		Expect(sources.replaceConnector.ID).To(Equal(connectorID))
		Expect(sources.replaceConnector.UserID).To(Equal(userID))
		Expect(sources.replaceConnector.Config.GetStorageType()).To(Equal(model.Postgres))
	})

	It("deletes source connectors with tenant context", func() {
		connectorID := uuid.New()
		req := newJSONRequest(http.MethodDelete, "/v1/data/registry/connector/POSTGRES/"+connectorID.String(), `{}`, userID, uuid.Nil)
		req = mux.SetURLVars(req, map[string]string{"type": "POSTGRES", "connectorId": connectorID.String()})

		res, err := handlers.DeleteSourceConnector(ctx, req)

		Expect(err).NotTo(HaveOccurred())
		Expect(res.StatusCode()).To(Equal(http.StatusOK))
		Expect(sources.deleteConnectorID).To(Equal(connectorID))
		Expect(sources.deleteUserID).To(Equal(userID))
		Expect(sources.deleteTenantID).To(Equal(userID))
	})

	It("rejects invalid connector types", func() {
		req := newJSONRequest(http.MethodPost, "/v1/data/registry/connector/not-a-type", `{"config":{}}`, userID, requestID)
		req = mux.SetURLVars(req, map[string]string{"type": "not-a-type"})

		_, err := handlers.CreateSourceConnector(ctx, req)

		var httpErr *HTTPError
		Expect(errors.As(err, &httpErr)).To(BeTrue())
		Expect(httpErr.statusCode).To(Equal(http.StatusBadRequest))
	})
})

var _ = Describe("REST support helpers", func() {
	It("builds conflict and internal server errors with wrapping", func() {
		cause := errors.New("database unavailable")

		conflict := ErrConflict().WithMessage("duplicate").Wrap(cause)
		internal := ErrInternalServer().Wrap(cause)

		Expect(conflict.statusCode).To(Equal(http.StatusConflict))
		Expect(conflict.Error()).To(Equal("duplicate"))
		Expect(errors.Is(conflict, cause)).To(BeTrue())
		Expect(internal.statusCode).To(Equal(http.StatusInternalServerError))
		Expect(internal.Error()).To(Equal(http.StatusText(http.StatusInternalServerError)))
		Expect(errors.Is(internal, cause)).To(BeTrue())
	})

	It("returns the standard status text for empty HTTP errors", func() {
		err := (&HTTPError{statusCode: http.StatusTeapot}).Error()

		Expect(err).To(Equal(http.StatusText(http.StatusTeapot)))
	})
})

func newJSONRequest(method, path, body string, userID, requestID uuid.UUID) *http.Request {
	req := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	if userID != uuid.Nil {
		req.Header.Set("X-User-ID", userID.String())
	}
	if requestID != uuid.Nil {
		req.Header.Set("X-Request-ID", requestID.String())
	}
	return req
}

func routeExists(routes []Route, path, method string) bool {
	for _, route := range routes {
		if route.Path == path && route.Method == method {
			return true
		}
	}
	return false
}
