package usecase_test

import (
	"context"
	"errors"

	usecase "data_registry_service/pkg/app"
	domainErrors "data_registry_service/pkg/domain"
	"data_registry_service/pkg/domain/model"
	"data_registry_service/pkg/mocks"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type stubSourceRepository struct {
	createConnector      *model.SourceConnector
	createIdempotencyKey uuid.UUID
	createErr            error

	readConnectorID uuid.UUID
	readUserID      uuid.UUID
	readConnector   *model.SourceConnector
	readErr         error

	readCatalogIDConnectorID uuid.UUID
	readCatalogIDUserID      uuid.UUID
	readCatalogID            uuid.UUID
	readCatalogIDErr         error

	deleteConnectorID uuid.UUID
	deleteUserID      uuid.UUID
	deleteErr         error

	replaceConnectors []*model.SourceConnector
	replaceErr        error
}

func (s *stubSourceRepository) Close() {}

func (s *stubSourceRepository) Create(_ context.Context, connector *model.SourceConnector, idempotencyKey uuid.UUID) error {
	s.createConnector = connector
	s.createIdempotencyKey = idempotencyKey
	return s.createErr
}

func (s *stubSourceRepository) ReadByUserID(_ context.Context, _ uuid.UUID) ([]model.SourceConnector, error) {
	return nil, nil
}

func (s *stubSourceRepository) ReadByID(_ context.Context, connectorID, userID uuid.UUID) (*model.SourceConnector, error) {
	s.readConnectorID = connectorID
	s.readUserID = userID
	return s.readConnector, s.readErr
}

func (s *stubSourceRepository) ReadCatalogID(_ context.Context, connectorID, userID uuid.UUID) (uuid.UUID, error) {
	s.readCatalogIDConnectorID = connectorID
	s.readCatalogIDUserID = userID
	return s.readCatalogID, s.readCatalogIDErr
}

func (s *stubSourceRepository) Delete(_ context.Context, connectorID, userID uuid.UUID) error {
	s.deleteConnectorID = connectorID
	s.deleteUserID = userID
	return s.deleteErr
}

func (s *stubSourceRepository) Replace(_ context.Context, connector *model.SourceConnector) error {
	s.replaceConnectors = append(s.replaceConnectors, connector)
	return s.replaceErr
}

type stubCatalogClient struct {
	createName   string
	createConfig model.ConnectorConfig
	createID     uuid.UUID
	createErr    error

	replaceName      string
	replaceCatalogID uuid.UUID
	replaceConfig    model.ConnectorConfig
	replaceErr       error

	deleteCatalogIDs []uuid.UUID
	deleteErr        error
}

func (s *stubCatalogClient) CreateResource(_ context.Context, name string, sourceConnCfg model.ConnectorConfig) (uuid.UUID, error) {
	s.createName = name
	s.createConfig = sourceConnCfg
	return s.createID, s.createErr
}

func (s *stubCatalogClient) ReplaceResource(_ context.Context, name string, catalogID uuid.UUID, sourceConnCfg model.ConnectorConfig) error {
	s.replaceName = name
	s.replaceCatalogID = catalogID
	s.replaceConfig = sourceConnCfg
	return s.replaceErr
}

func (s *stubCatalogClient) DeleteResource(_ context.Context, catalogID uuid.UUID) error {
	s.deleteCatalogIDs = append(s.deleteCatalogIDs, catalogID)
	return s.deleteErr
}

var _ = Describe("SourceUsecase", func() {
	var (
		ctx       context.Context
		repo      *stubSourceRepository
		catalog   *stubCatalogClient
		uc        usecase.SourceUsecase
		userID    uuid.UUID
		catalogID uuid.UUID
		config    model.ConnectorConfig
		connector *model.SourceConnector
	)

	BeforeEach(func() {
		ctx = context.Background()
		repo = &stubSourceRepository{}
		catalog = &stubCatalogClient{}
		uc = usecase.NewSourceUsecase(repo, catalog)
		userID = uuid.New()
		catalogID = uuid.New()
		config = &mocks.MockSourceConfig{NextSourceType: model.S3}
		connector = &model.SourceConnector{UserID: userID, Config: config}
		catalog.createID = catalogID
	})

	It("creates a catalog source before saving the connector", func() {
		idempotencyKey := uuid.New()

		Expect(uc.CreateSourceConnector(ctx, connector, idempotencyKey)).To(Succeed())

		Expect(catalog.createName).NotTo(BeEmpty())
		Expect(catalog.createConfig).To(Equal(config))
		Expect(repo.createConnector).To(Equal(connector))
		Expect(repo.createConnector.ID).NotTo(Equal(uuid.Nil))
		Expect(repo.createConnector.CatalogID).To(Equal(catalogID))
		Expect(repo.createIdempotencyKey).To(Equal(idempotencyKey))
	})

	It("rolls back the catalog source when repository create fails", func() {
		expectedErr := errors.New("create failed")
		repo.createErr = expectedErr

		Expect(uc.CreateSourceConnector(ctx, connector, uuid.New())).To(MatchError(expectedErr))
		Expect(catalog.deleteCatalogIDs).To(Equal([]uuid.UUID{catalogID}))
	})

	It("reads a source connector through the repository", func() {
		connectorID := uuid.New()
		expected := &model.SourceConnector{ID: connectorID, UserID: userID}
		repo.readConnector = expected

		got, err := uc.ReadSourceConnector(ctx, connectorID, userID)

		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(expected))
		Expect(repo.readConnectorID).To(Equal(connectorID))
		Expect(repo.readUserID).To(Equal(userID))
	})

	It("deletes the catalog source before deleting the repository record", func() {
		connectorID := uuid.New()
		repo.readCatalogID = catalogID

		Expect(uc.DeleteSourceConnector(ctx, connectorID, userID)).To(Succeed())

		Expect(repo.readCatalogIDConnectorID).To(Equal(connectorID))
		Expect(repo.readCatalogIDUserID).To(Equal(userID))
		Expect(catalog.deleteCatalogIDs).To(Equal([]uuid.UUID{catalogID}))
		Expect(repo.deleteConnectorID).To(Equal(connectorID))
		Expect(repo.deleteUserID).To(Equal(userID))
	})

	It("continues deleting the repository record if catalog delete reports not found", func() {
		connectorID := uuid.New()
		repo.readCatalogID = catalogID
		catalog.deleteErr = domainErrors.ErrResourceNotFound

		Expect(uc.DeleteSourceConnector(ctx, connectorID, userID)).To(Succeed())
		Expect(repo.deleteConnectorID).To(Equal(connectorID))
	})
})
