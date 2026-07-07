package usecase_test

import (
	"context"
	"errors"

	usecase "data_registry_service/pkg/app"
	domainErrors "data_registry_service/pkg/domain"
	"data_registry_service/pkg/domain/model"
	"lib/shared_lib/ctxutil"
	shareduow "lib/shared_lib/uow"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type stubSourceRepository struct {
	reserveID  uuid.UUID
	reserveErr error

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
	replaceErrs       []error
}

func (s *stubSourceRepository) Close() {}

func (s *stubSourceRepository) ReserveID(context.Context, pgx.Tx) (uuid.UUID, error) {
	if s.reserveErr != nil {
		return uuid.Nil, s.reserveErr
	}
	if s.reserveID == uuid.Nil {
		s.reserveID = uuid.New()
	}
	return s.reserveID, nil
}

func (s *stubSourceRepository) Create(_ context.Context, _ pgx.Tx, connector *model.SourceConnector, idempotencyKey uuid.UUID) error {
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

func (s *stubSourceRepository) Delete(_ context.Context, _ pgx.Tx, connectorID, userID uuid.UUID) error {
	s.deleteConnectorID = connectorID
	s.deleteUserID = userID
	return s.deleteErr
}

func (s *stubSourceRepository) Replace(_ context.Context, _ pgx.Tx, connector *model.SourceConnector) error {
	s.replaceConnectors = append(s.replaceConnectors, connector)
	if len(s.replaceErrs) > 0 {
		err := s.replaceErrs[0]
		s.replaceErrs = s.replaceErrs[1:]
		return err
	}
	return s.replaceErr
}

type stubSourceUnitOfWork struct {
	calls int
	err   error
}

func (s *stubSourceUnitOfWork) Do(ctx context.Context, fn shareduow.TxFunc) error {
	s.calls++
	if s.err != nil {
		return s.err
	}
	return fn(ctx, nil, func(shareduow.OutboundMessage) error { return nil })
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

type mockSourceConfig struct {
	getStorageTypeCalled bool
	nextSourceType       model.StorageType
}

func (m *mockSourceConfig) GetStorageType() model.StorageType {
	m.getStorageTypeCalled = true
	return m.nextSourceType
}

var _ = Describe("SourceUsecase", func() {
	var (
		ctx       context.Context
		repo      *stubSourceRepository
		uow       *stubSourceUnitOfWork
		catalog   *stubCatalogClient
		uc        usecase.SourceUsecase
		userID    uuid.UUID
		catalogID uuid.UUID
		config    model.ConnectorConfig
		connector *model.SourceConnector
	)

	BeforeEach(func() {
		userID = uuid.New()
		ctx = ctxutil.WithActorOrg(context.Background(), userID, uuid.New())
		repo = &stubSourceRepository{}
		uow = &stubSourceUnitOfWork{}
		catalog = &stubCatalogClient{}
		uc = usecase.NewSourceUsecase(repo, uow, catalog)
		catalogID = uuid.New()
		config = &mockSourceConfig{nextSourceType: model.S3}
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
		Expect(uow.calls).To(Equal(1))
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

	It("replaces the catalog resource before the repository record", func() {
		connectorID := uuid.New()
		originalConfig := &mockSourceConfig{nextSourceType: model.Postgres}
		updatedConfig := &mockSourceConfig{nextSourceType: model.MySQL}
		repo.readConnector = &model.SourceConnector{
			ID:        connectorID,
			UserID:    userID,
			CatalogID: catalogID,
			Config:    originalConfig,
		}
		replacement := &model.SourceConnector{
			ID:     connectorID,
			UserID: userID,
			Config: updatedConfig,
		}

		Expect(uc.ReplaceSourceConnector(ctx, replacement)).To(Succeed())

		Expect(repo.readConnectorID).To(Equal(connectorID))
		Expect(repo.readUserID).To(Equal(userID))
		Expect(replacement.CatalogID).To(Equal(catalogID))
		Expect(repo.replaceConnectors).To(HaveLen(1))
		Expect(repo.replaceConnectors[0]).To(Equal(replacement))
		Expect(catalog.replaceName).To(Equal(connectorID.String()))
		Expect(catalog.replaceCatalogID).To(Equal(catalogID))
		Expect(catalog.replaceConfig).To(Equal(updatedConfig))
		Expect(uow.calls).To(Equal(1))
	})

	It("returns repository read errors before replacing source connectors", func() {
		expectedErr := errors.New("read failed")
		repo.readErr = expectedErr
		replacement := &model.SourceConnector{ID: uuid.New(), UserID: userID, Config: config}

		err := uc.ReplaceSourceConnector(ctx, replacement)

		Expect(err).To(MatchError(expectedErr))
		Expect(repo.replaceConnectors).To(BeEmpty())
		Expect(catalog.replaceName).To(BeEmpty())
	})

	It("returns repository replace errors after replacing catalog resources", func() {
		expectedErr := errors.New("replace failed")
		connectorID := uuid.New()
		repo.readConnector = &model.SourceConnector{ID: connectorID, UserID: userID, CatalogID: catalogID, Config: config}
		repo.replaceErr = expectedErr

		err := uc.ReplaceSourceConnector(ctx, &model.SourceConnector{ID: connectorID, UserID: userID, Config: config})

		Expect(err).To(MatchError(expectedErr))
		Expect(repo.replaceConnectors).To(HaveLen(1))
		Expect(catalog.replaceName).To(Equal(connectorID.String()))
	})

	It("does not replace the repository record when catalog replace fails", func() {
		expectedErr := errors.New("catalog replace failed")
		connectorID := uuid.New()
		updatedConfig := &mockSourceConfig{nextSourceType: model.MySQL}
		repo.readConnector = &model.SourceConnector{ID: connectorID, UserID: userID, CatalogID: catalogID, Config: config}
		catalog.replaceErr = expectedErr

		err := uc.ReplaceSourceConnector(ctx, &model.SourceConnector{ID: connectorID, UserID: userID, Config: updatedConfig})

		Expect(err).To(MatchError(expectedErr))
		Expect(repo.replaceConnectors).To(BeEmpty())
		Expect(uow.calls).To(Equal(0))
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
		Expect(uow.calls).To(Equal(1))
	})

	It("continues deleting the repository record if catalog delete reports not found", func() {
		connectorID := uuid.New()
		repo.readCatalogID = catalogID
		catalog.deleteErr = domainErrors.ErrResourceNotFound

		Expect(uc.DeleteSourceConnector(ctx, connectorID, userID)).To(Succeed())
		Expect(repo.deleteConnectorID).To(Equal(connectorID))
	})
})
