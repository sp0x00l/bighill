package db

import (
	"context"
	"errors"

	domainErrors "data_registry_service/pkg/domain"
	"data_registry_service/pkg/domain/model"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

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
		Expect(args["processing_profile"]).To(Equal(pgtype.Text{String: "TEXT_RAG", Valid: true}))
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

func validDatasetDAO(datasetID, userID uuid.UUID) *DatasetDAO {
	return &DatasetDAO{
		ID:                  pgtype.UUID{Bytes: datasetID, Valid: true},
		UserID:              pgtype.UUID{Bytes: userID, Valid: true},
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
