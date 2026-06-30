package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	usecase "data_registry_service/pkg/app"
	domainErrors "data_registry_service/pkg/domain"
	"data_registry_service/pkg/domain/model"
	catalogclient "data_registry_service/pkg/infra/network/client"
	repo "data_registry_service/pkg/infra/repo/db"
	dbconn "lib/shared_lib/db"
	"lib/shared_lib/transport"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"
)

func TestDataRegistryIntegration(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Data registry integration test suite")
}

var _ = Describe("Data registry integration", Ordered, func() {
	var (
		ctx        context.Context
		cancel     context.CancelFunc
		database   *dbconn.Database
		datasetDB  usecase.DatasetRepositoryAdapter
		sourceDB   usecase.SourceRepositoryAdapter
		datasets   usecase.DatasetUsecase
		connectors usecase.SourceUsecase
	)

	BeforeAll(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 90*time.Second)

		cfg := dbconn.DatabaseConfig{}
		cfg.WithDbName("DATA_REGISTRY_DB_NAME", "bighill_data_registry_db")
		cfg.WithDbUser("DATA_REGISTRY_DB_USER", "bighill_data_registry_db_user")
		cfg.WithDbPassword("DATA_REGISTRY_DB_PASSWORD", "")
		cfg.WithDbMaxConnections("DATA_REGISTRY_DB_MAX_CONNECTIONS", "20")

		var err error
		database, err = dbconn.InitDatabase(ctx, cfg.GetName(), cfg.GetConnectionString(), log.StandardLogger())
		Expect(err).NotTo(HaveOccurred())

		datasetDB = repo.NewDatasetDB(database)
		sourceDB = repo.NewSourceConnectorDB(database)
		datasets = usecase.NewDatasetUseCase(datasetDB)
		connectors = usecase.NewSourceUsecase(sourceDB, catalogclient.NewLocalCatalogClient())
	})

	AfterAll(func() {
		if datasetDB != nil {
			datasetDB.Close()
		}
		if cancel != nil {
			cancel()
		}
	})

	It("persists dataset metadata through Postgres", func() {
		datasetID := uuid.New()
		userID := uuid.New()
		dataset := &model.Dataset{
			ID:              datasetID,
			UserID:          userID,
			Title:           "Movie features",
			Description:     "Feature metadata for movie records",
			Location:        "s3://local-dev-bucket/raw/movies.csv",
			Category:        "movies",
			TableNamespace:  "features",
			TableName:       "movie_features",
			TableFormat:     model.Parquet,
			CatalogProvider: model.LocalCatalog,
			SchemaVersion:   1,
			SchemaMetadata:  `{"columns":["title","release_year"]}`,
		}

		Expect(datasets.CreateDataset(ctx, dataset, uuid.New())).To(Succeed())

		read, err := datasets.ReadDatasetForUser(ctx, datasetID, userID)
		Expect(err).NotTo(HaveOccurred())
		Expect(read.Title).To(Equal("Movie features"))
		Expect(read.TableNamespace).To(Equal("features"))
		Expect(read.TableName).To(Equal("movie_features"))
		Expect(read.TableFormat).To(Equal(model.Parquet))

		Expect(datasets.PublishDataset(ctx, datasetID, userID)).To(Succeed())

		published, err := datasets.ReadPublishedDatasetByID(ctx, datasetID)
		Expect(err).NotTo(HaveOccurred())
		Expect(published.Status).To(Equal(model.Published))

		Expect(datasets.DeleteDataset(ctx, datasetID, userID)).To(Succeed())
		_, err = datasets.ReadDatasetForUser(ctx, datasetID, userID)
		Expect(errors.Is(err, domainErrors.ErrResourceNotFound)).To(BeTrue())
	})

	It("reports duplicate idempotency keys and missing datasets with domain errors", func() {
		idempotencyKey := uuid.New()
		userID := uuid.New()
		first := &model.Dataset{ID: uuid.New(), UserID: userID, Title: "duplicate-a"}
		second := &model.Dataset{ID: uuid.New(), UserID: userID, Title: "duplicate-b"}

		Expect(datasets.CreateDataset(ctx, first, idempotencyKey)).To(Succeed())
		err := datasets.CreateDataset(ctx, second, idempotencyKey)
		Expect(errors.Is(err, domainErrors.ErrResourceAlreadyExists)).To(BeTrue())

		_, err = datasets.ReadDatasetForUser(ctx, uuid.New(), userID)
		Expect(errors.Is(err, domainErrors.ErrResourceNotFound)).To(BeTrue())
	})

	It("persists source connectors through Postgres", func() {
		userID := uuid.New()
		connector := &model.SourceConnector{
			UserID: userID,
			Config: &model.ClickHouseConnCfg{
				Hostname:           "127.0.0.1",
				Port:               19000,
				DatabaseName:       "mlops",
				Username:           "user",
				Password:           "password",
				AuthenticationType: model.Master,
			},
		}

		Expect(connectors.CreateSourceConnector(ctx, connector, uuid.New())).To(Succeed())
		Expect(connector.ID).NotTo(Equal(uuid.Nil))
		Expect(connector.CatalogID).NotTo(Equal(uuid.Nil))

		read, err := connectors.ReadSourceConnector(ctx, connector.ID, userID)
		Expect(err).NotTo(HaveOccurred())
		Expect(read.Config.GetStorageType()).To(Equal(model.ClickHouse))
		cfg, ok := read.Config.(*model.ClickHouseConnCfg)
		Expect(ok).To(BeTrue())
		Expect(cfg.DatabaseName).To(Equal("mlops"))

		Expect(connectors.DeleteSourceConnector(ctx, connector.ID, userID)).To(Succeed())
		_, err = connectors.ReadSourceConnector(ctx, connector.ID, userID)
		Expect(errors.Is(err, domainErrors.ErrResourceNotFound)).To(BeTrue())
	})

	It("returns no rows rather than failing when a requested page is beyond the dataset count", func() {
		userID := uuid.New()
		Expect(datasets.CreateDataset(ctx, &model.Dataset{
			ID:     uuid.New(),
			UserID: userID,
			Title:  "pagination-check",
		}, uuid.New())).To(Succeed())

		got, total, err := datasets.ReadDatasetsForUser(ctx, userID, transport.Pagination{Limit: 10, Page: 99}, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(total).To(BeNumerically(">", 0))
		Expect(got).To(BeNil())
	})
})
