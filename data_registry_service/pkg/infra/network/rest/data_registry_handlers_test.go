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

func (s *datasetUsecaseStub) ReadDatasetTable(context.Context, uuid.UUID, uuid.UUID, string) (*model.Dataset, error) {
	return nil, nil
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

func (s *sourceUsecaseStub) DeleteSourceConnector(_ context.Context, connectorID, userID uuid.UUID) error {
	s.deleteConnectorID = connectorID
	s.deleteUserID = userID
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

	It("rejects invalid connector types", func() {
		req := newJSONRequest(http.MethodPost, "/v1/data/registry/connector/not-a-type", `{"config":{}}`, userID, requestID)
		req = mux.SetURLVars(req, map[string]string{"type": "not-a-type"})

		_, err := handlers.CreateSourceConnector(ctx, req)

		var httpErr *HTTPError
		Expect(errors.As(err, &httpErr)).To(BeTrue())
		Expect(httpErr.statusCode).To(Equal(http.StatusBadRequest))
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
