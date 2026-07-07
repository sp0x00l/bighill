package db

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	domainErrors "data_registry_service/pkg/domain"
	"data_registry_service/pkg/domain/model"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestDB(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Data registry repository DB unit test suite")
}

var _ = Describe("DatasetDAO", func() {
	var (
		ctx       context.Context
		datasetID uuid.UUID
		userID    uuid.UUID
	)

	BeforeEach(func() {
		ctx = context.Background()
		datasetID = uuid.New()
		userID = uuid.New()
	})

	It("maps domain datasets to database arguments", func() {
		rawSnapshotID := uuid.New()
		dataset := &model.Dataset{
			ID:                datasetID,
			UserID:            userID,
			Title:             "Movies",
			Description:       "Movie rows",
			Location:          "s3://bucket/raw/movies.parquet",
			Category:          "rag",
			TableNamespace:    "features",
			TableName:         "movies",
			TableFormat:       model.Parquet,
			CatalogProvider:   model.LocalCatalog,
			ProcessingProfile: model.TextRAGProfile,
			SchemaVersion:     2,
			SchemaMetadata:    `{"columns":["title"]}`,
			ProcessingState:   model.DatasetProcessingRawMaterialized,
			DatasetVersion:    3,
			RawSnapshotID:     rawSnapshotID,
		}

		args := (&Dataset{IdempotencyKey: pgtype.UUID{Bytes: uuid.New(), Valid: true}}).toDAO(dataset)

		Expect(args["id"]).To(Equal(pgtype.UUID{Bytes: datasetID, Valid: true}))
		Expect(args["user_id"]).To(Equal(pgtype.UUID{Bytes: userID, Valid: true}))
		Expect(args["title"]).To(Equal(pgtype.Text{String: "Movies", Valid: true}))
		Expect(args["table_format"]).To(Equal(pgtype.Text{String: "PARQUET", Valid: true}))
		Expect(args["processing_profile"]).To(Equal(pgtype.Text{String: "TEXT_RAG_PROCESSING_PROFILE", Valid: true}))
		Expect(args["raw_snapshot_id"]).To(Equal(pgtype.UUID{Bytes: rawSnapshotID, Valid: true}))
	})

	It("maps database rows to domain datasets", func() {
		dao := validDatasetDAO(datasetID, userID)

		dataset, err := fromDAO(ctx, dao)

		Expect(err).NotTo(HaveOccurred())
		Expect(dataset.ID).To(Equal(datasetID))
		Expect(dataset.UserID).To(Equal(userID))
		Expect(dataset.Title).To(Equal("Movies"))
		Expect(dataset.TableFormat).To(Equal(model.Parquet))
		Expect(dataset.CatalogProvider).To(Equal(model.LocalCatalog))
		Expect(dataset.ProcessingProfile).To(Equal(model.TextRAGProfile))
		Expect(dataset.ProcessingState).To(Equal(model.DatasetProcessingRawMaterialized))
	})

	It("maps scanned database rows to dataset DAOs", func() {
		dao := validDatasetDAO(datasetID, userID)

		got, err := toDatasetDAO(&datasetRowStub{dao: dao})

		Expect(err).NotTo(HaveOccurred())
		Expect(got.ID).To(Equal(dao.ID))
		Expect(got.UserID).To(Equal(dao.UserID))
		Expect(got.Title).To(Equal(dao.Title))
		Expect(got.ProcessingState).To(Equal(dao.ProcessingState))
	})

	It("maps row scan misses to resource-not-found domain errors", func() {
		_, err := fromDatasetRow(ctx, &datasetRowStub{err: pgx.ErrNoRows})

		Expect(errors.Is(err, domainErrors.ErrResourceNotFound)).To(BeTrue())
	})

	It("rejects invalid database enum values", func() {
		dao := validDatasetDAO(datasetID, userID)
		dao.Status = pgtype.Text{String: "not-a-status", Valid: true}

		_, err := fromDAO(ctx, dao)

		Expect(errors.Is(err, domainErrors.ErrValidationFailed)).To(BeTrue())
	})
})

var _ = Describe("SourceConnectorDAO", func() {
	var ctx context.Context

	BeforeEach(func() {
		ctx = context.Background()
	})

	It("maps domain source connectors to database arguments", func() {
		connectorID := uuid.New()
		userID := uuid.New()
		catalogID := uuid.New()
		idempotencyKey := uuid.New()
		config := &model.PostgresDBConnCfg{
			Hostname:           "localhost",
			Port:               5432,
			DatabaseName:       "mlops",
			Username:           "postgres",
			Password:           "password",
			AuthenticationType: model.Master,
		}

		args, err := toSourceConnDAO(ctx, &model.SourceConnector{
			ID:        connectorID,
			UserID:    userID,
			CatalogID: catalogID,
			Config:    config,
		}, idempotencyKey)

		Expect(err).NotTo(HaveOccurred())
		Expect(args["id"]).To(Equal(pgtype.UUID{Bytes: connectorID, Valid: true}))
		Expect(args["user_id"]).To(Equal(pgtype.UUID{Bytes: userID, Valid: true}))
		Expect(args["catalog_id"]).To(Equal(pgtype.UUID{Bytes: catalogID, Valid: true}))
		Expect(args["storage_type"]).To(Equal(pgtype.Text{String: model.Postgres.String(), Valid: true}))
		Expect(args["idempotency_key"]).To(Equal(pgtype.UUID{Bytes: idempotencyKey, Valid: true}))

		var decoded model.PostgresDBConnCfg
		Expect(json.Unmarshal(args["config"].([]byte), &decoded)).To(Succeed())
		Expect(decoded.DatabaseName).To(Equal("mlops"))
	})

	It("omits nil idempotency keys from source connector database arguments", func() {
		args, err := toSourceConnDAO(ctx, &model.SourceConnector{
			ID:        uuid.New(),
			UserID:    uuid.New(),
			CatalogID: uuid.New(),
			Config:    &model.PostgresDBConnCfg{},
		}, uuid.Nil)

		Expect(err).NotTo(HaveOccurred())
		Expect(args).NotTo(HaveKey("idempotency_key"))
	})

	It("rejects unserializable source connector configs", func() {
		_, err := toSourceConnDAO(ctx, &model.SourceConnector{
			ID:        uuid.New(),
			UserID:    uuid.New(),
			CatalogID: uuid.New(),
			Config:    unserializableConnectorConfig{},
		}, uuid.New())

		Expect(errors.Is(err, domainErrors.ErrValidationFailed)).To(BeTrue())
	})

	It("maps Postgres connector configs from DAO rows", func() {
		connectorID := uuid.New()
		userID := uuid.New()
		catalogID := uuid.New()
		dao := SourceConnectorDAO{
			ID:          pgtype.UUID{Bytes: connectorID, Valid: true},
			UserID:      pgtype.UUID{Bytes: userID, Valid: true},
			CatalogID:   pgtype.UUID{Bytes: catalogID, Valid: true},
			StorageType: pgtype.Text{String: model.Postgres.String(), Valid: true},
			Config:      []byte(`{"Hostname":"localhost","Port":5432,"DatabaseName":"mlops","Username":"postgres","Password":"password","AuthenticationType":1}`),
		}

		var connector model.SourceConnector
		err := fromSourceConnDAO(ctx, &connector, dao)

		Expect(err).NotTo(HaveOccurred())
		Expect(connector.ID).To(Equal(connectorID))
		Expect(connector.UserID).To(Equal(userID))
		Expect(connector.CatalogID).To(Equal(catalogID))
		cfg, ok := connector.Config.(*model.PostgresDBConnCfg)
		Expect(ok).To(BeTrue())
		Expect(cfg.DatabaseName).To(Equal("mlops"))
	})

	DescribeTable("maps connector-specific configs from DAO rows",
		func(storageType model.StorageType, config model.ConnectorConfig, expected any) {
			payload, err := json.Marshal(config)
			Expect(err).NotTo(HaveOccurred())

			var connector model.SourceConnector
			err = fromSourceConnDAO(ctx, &connector, SourceConnectorDAO{
				ID:          pgtype.UUID{Bytes: uuid.New(), Valid: true},
				UserID:      pgtype.UUID{Bytes: uuid.New(), Valid: true},
				CatalogID:   pgtype.UUID{Bytes: uuid.New(), Valid: true},
				StorageType: pgtype.Text{String: storageType.String(), Valid: true},
				Config:      payload,
			})

			Expect(err).NotTo(HaveOccurred())
			Expect(connector.Config).To(BeAssignableToTypeOf(expected))
		},
		Entry("S3", model.S3, &model.AwsS3StorageConnCfg{RootPath: "s3://bucket"}, &model.AwsS3StorageConnCfg{}),
		Entry("Azure", model.AzureStorage, &model.AzureStorageConnCfg{AccountName: "storage"}, &model.AzureStorageConnCfg{}),
		Entry("GCS", model.GoogleCloudStorage, &model.GoogleCloudStorageConnCfg{ProjectID: "project"}, &model.GoogleCloudStorageConnCfg{}),
		Entry("MySQL", model.MySQL, &model.MysqlDBConnCfg{Hostname: "mysql"}, &model.MysqlDBConnCfg{}),
		Entry("Oracle", model.Oracle, &model.OracleDBConnCfg{Hostname: "oracle"}, &model.OracleDBConnCfg{}),
		Entry("Mongo", model.MongoDB, &model.MongoDBConnCfg{HostList: []model.Host{{Hostname: "localhost", Port: 27017}}}, &model.MongoDBConnCfg{}),
		Entry("ClickHouse", model.ClickHouse, &model.ClickHouseConnCfg{Hostname: "clickhouse"}, &model.ClickHouseConnCfg{}),
	)

	It("rejects invalid connector storage types", func() {
		dao := SourceConnectorDAO{
			ID:          pgtype.UUID{Bytes: uuid.New(), Valid: true},
			UserID:      pgtype.UUID{Bytes: uuid.New(), Valid: true},
			StorageType: pgtype.Text{String: "NOT_A_SOURCE", Valid: true},
			Config:      []byte(`{}`),
		}

		var connector model.SourceConnector
		err := fromSourceConnDAO(ctx, &connector, dao)

		Expect(errors.Is(err, domainErrors.ErrValidationFailed)).To(BeTrue())
	})

	It("rejects corrupt connector config JSON", func() {
		dao := SourceConnectorDAO{
			ID:          pgtype.UUID{Bytes: uuid.New(), Valid: true},
			UserID:      pgtype.UUID{Bytes: uuid.New(), Valid: true},
			StorageType: pgtype.Text{String: model.Postgres.String(), Valid: true},
			Config:      []byte(`{"Hostname":`),
		}

		var connector model.SourceConnector
		err := fromSourceConnDAO(ctx, &connector, dao)

		Expect(errors.Is(err, domainErrors.ErrValidationFailed)).To(BeTrue())
	})
})

type unserializableConnectorConfig struct {
	Callback func()
}

func (u unserializableConnectorConfig) GetStorageType() model.StorageType {
	return model.Postgres
}

func validDatasetDAO(datasetID, userID uuid.UUID) *DatasetDAO {
	return &DatasetDAO{
		ID:                  pgtype.UUID{Bytes: datasetID, Valid: true},
		UserID:              pgtype.UUID{Bytes: userID, Valid: true},
		OrgID:               pgtype.UUID{Bytes: userID, Valid: true},
		Title:               pgtype.Text{String: "Movies", Valid: true},
		Description:         pgtype.Text{String: "Movie rows", Valid: true},
		Origin:              pgtype.Text{String: model.Standard.String(), Valid: true},
		Location:            pgtype.Text{String: "s3://bucket/raw/movies.parquet", Valid: true},
		Status:              pgtype.Text{String: model.Draft.String(), Valid: true},
		Category:            pgtype.Text{String: "rag", Valid: true},
		TableNamespace:      pgtype.Text{String: "features", Valid: true},
		TableName:           pgtype.Text{String: "movies", Valid: true},
		TableFormat:         pgtype.Text{String: model.Parquet.String(), Valid: true},
		CatalogProvider:     pgtype.Text{String: model.LocalCatalog.String(), Valid: true},
		ProcessingProfile:   pgtype.Text{String: model.TextRAGProfile.String(), Valid: true},
		SchemaVersion:       pgtype.Int4{Int32: 1, Valid: true},
		SchemaMetadata:      pgtype.Text{String: "{}", Valid: true},
		ProcessingState:     pgtype.Text{String: model.DatasetProcessingRawMaterialized.String(), Valid: true},
		DatasetVersion:      pgtype.Int4{Int32: 2, Valid: true},
		EmbeddingDimensions: pgtype.Int4{Int32: 384, Valid: true},
		EmbeddingCount:      pgtype.Int8{Int64: 10, Valid: true},
	}
}

type datasetRowStub struct {
	dao *DatasetDAO
	err error
}

func (r *datasetRowStub) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	values := []any{
		r.dao.ID,
		r.dao.UserID,
		r.dao.OrgID,
		r.dao.Title,
		r.dao.Description,
		r.dao.Origin,
		r.dao.Location,
		r.dao.SourceType,
		r.dao.SourceConnectorID,
		r.dao.SourceQuery,
		r.dao.SourceDatabase,
		r.dao.SourceCollection,
		r.dao.Status,
		r.dao.Category,
		r.dao.TableNamespace,
		r.dao.TableName,
		r.dao.TableFormat,
		r.dao.CatalogProvider,
		r.dao.ProcessingProfile,
		r.dao.SchemaVersion,
		r.dao.SchemaMetadata,
		r.dao.ProcessingState,
		r.dao.DatasetVersion,
		r.dao.RawSnapshotID,
		r.dao.FeatureSnapshotID,
		r.dao.EmbeddingSnapshotID,
		r.dao.VectorStore,
		r.dao.CollectionName,
		r.dao.EmbeddingDimensions,
		r.dao.EmbeddingCount,
		r.dao.EmbeddingStrategyVersion,
		r.dao.EmbeddingChunkerName,
		r.dao.EmbeddingChunkerVersion,
		r.dao.EmbeddingChunkSize,
		r.dao.EmbeddingChunkOverlap,
		r.dao.EmbeddingProvider,
		r.dao.EmbeddingModel,
	}
	for i := range dest {
		switch target := dest[i].(type) {
		case *pgtype.UUID:
			*target = values[i].(pgtype.UUID)
		case *pgtype.Text:
			*target = values[i].(pgtype.Text)
		case *pgtype.Int4:
			*target = values[i].(pgtype.Int4)
		case *pgtype.Int8:
			*target = values[i].(pgtype.Int8)
		default:
			Fail("unexpected scan target")
		}
	}
	return nil
}
