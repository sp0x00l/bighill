package model_test

import (
	"data_registry_service/pkg/domain/model"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Model type conversions", func() {
	DescribeTable("converts storage types",
		func(input string, expected model.StorageType) {
			got, err := model.ToStorageType(input)

			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal(expected))
		},
		Entry("s3", "S3", model.S3),
		Entry("postgres", "POSTGRES", model.Postgres),
		Entry("mongo", "MONGO", model.MongoDB),
		Entry("clickhouse", "CLICKHOUSE", model.ClickHouse),
	)

	It("rejects unknown storage types", func() {
		_, err := model.ToStorageType("NOT_A_SOURCE")

		Expect(err).To(HaveOccurred())
	})

	DescribeTable("converts table formats",
		func(input string, expected model.TableFormat) {
			got, err := model.ToTableFormat(input)

			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal(expected))
			Expect(got.String()).To(Equal(input))
		},
		Entry("parquet", "PARQUET", model.Parquet),
		Entry("iceberg", "ICEBERG", model.Iceberg),
	)

	DescribeTable("converts CTAS formats through the table-format parser",
		func(input string, expected model.CtasFormat) {
			got, err := model.ToCtasFormat(input)

			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal(expected))
		},
		Entry("parquet", "PARQUET", model.CtasFormat(model.Parquet)),
		Entry("iceberg", "ICEBERG", model.CtasFormat(model.Iceberg)),
	)

	It("rejects unknown table formats", func() {
		_, err := model.ToTableFormat("CSV")

		Expect(err).To(HaveOccurred())

		_, err = model.ToCtasFormat("CSV")
		Expect(err).To(HaveOccurred())
	})

	DescribeTable("converts catalog providers",
		func(input string, expected model.CatalogProvider) {
			got, err := model.ToCatalogProvider(input)

			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal(expected))
			Expect(got.String()).To(Equal(input))
		},
		Entry("local", "LOCAL", model.LocalCatalog),
		Entry("polaris", "POLARIS", model.PolarisCatalog),
	)

	It("rejects unknown catalog providers", func() {
		_, err := model.ToCatalogProvider("NESSIE")

		Expect(err).To(HaveOccurred())
	})

	DescribeTable("converts authentication types",
		func(input string, expected model.AuthenticationType) {
			got, err := model.ToAuthenticationType(input)

			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal(expected))
			Expect(got.String()).To(Equal(input))
		},
		Entry("anonymous", "ANONYMOUS", model.Anonymous),
		Entry("master", "MASTER", model.Master),
	)

	It("rejects unknown authentication types", func() {
		_, err := model.ToAuthenticationType("TOKEN")

		Expect(err).To(HaveOccurred())
	})

	DescribeTable("converts azure versions",
		func(input string, expected model.AzureVersion) {
			got, err := model.ToAzureVersion(input)

			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal(expected))
			Expect(got.String()).To(Equal(input))
		},
		Entry("v1", "STORAGE_V1", model.AzureV1),
		Entry("v2", "STORAGE_V2", model.AzureV2),
	)

	It("rejects unknown azure versions", func() {
		_, err := model.ToAzureVersion("BLOB_ONLY")

		Expect(err).To(HaveOccurred())
	})

	DescribeTable("converts credentials types",
		func(input string, expected model.CredentialsType) {
			got, err := model.ToCredentialsType(input)

			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal(expected))
			Expect(got.String()).To(Equal(input))
		},
		Entry("access key", "ACCESS_KEY", model.AccessKey),
		Entry("active directory", "AZURE_ACTIVE_DIRECTORY", model.ActiveDirectory),
	)

	It("rejects unknown credentials types", func() {
		_, err := model.ToCredentialsType("PASSWORD")

		Expect(err).To(HaveOccurred())
	})

	DescribeTable("converts google auth modes",
		func(input string, expected model.AuthMode) {
			got, err := model.ToAuthMode(input)

			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal(expected))
			Expect(got.String()).To(Equal(input))
		},
		Entry("auto", "AUTO", model.Auto),
		Entry("service account", "SERVICE_ACCOUNT_KEYS", model.ServiceAccountKeys),
	)

	It("rejects unknown google auth modes", func() {
		_, err := model.ToAuthMode("WORKLOAD_IDENTITY")

		Expect(err).To(HaveOccurred())
	})

	DescribeTable("converts dataset origin and status",
		func(origin string, expectedOrigin model.OriginType, status string, expectedStatus model.StatusType) {
			gotOrigin, err := model.ToOriginType(origin)
			Expect(err).NotTo(HaveOccurred())
			Expect(gotOrigin).To(Equal(expectedOrigin))

			gotStatus, err := model.ToStatusType(status)
			Expect(err).NotTo(HaveOccurred())
			Expect(gotStatus).To(Equal(expectedStatus))
		},
		Entry("standard draft", "standard", model.Standard, "draft", model.Draft),
		Entry("community published", "community", model.Community, "published", model.Published),
		Entry("standard blacklisted", "standard", model.Standard, "blacklisted", model.Blacklisted),
		Entry("db standard draft", "STANDARD", model.Standard, "DRAFT", model.Draft),
		Entry("db community published", "COMMUNITY", model.Community, "PUBLISHED", model.Published),
		Entry("db standard blacklisted", "STANDARD", model.Standard, "BLACKLISTED", model.Blacklisted),
	)

	It("rejects unknown origin and status values", func() {
		_, err := model.ToOriginType("vendor")
		Expect(err).To(HaveOccurred())

		_, err = model.ToStatusType("archived")
		Expect(err).To(HaveOccurred())
	})

	It("renders known origin, status, and filter values", func() {
		Expect(model.Standard.String()).To(Equal("standard"))
		Expect(model.Community.String()).To(Equal("community"))
		Expect(model.Standard.DBString()).To(Equal("STANDARD"))
		Expect(model.Community.DBString()).To(Equal("COMMUNITY"))
		Expect(model.Draft.String()).To(Equal("draft"))
		Expect(model.Published.String()).To(Equal("published"))
		Expect(model.Blacklisted.String()).To(Equal("blacklisted"))
		Expect(model.Draft.DBString()).To(Equal("DRAFT"))
		Expect(model.Published.DBString()).To(Equal("PUBLISHED"))
		Expect(model.Blacklisted.DBString()).To(Equal("BLACKLISTED"))
		Expect(model.FilterByCategory.String()).To(Equal("category"))
		Expect(model.FilterByInvalid.String()).To(Equal("invalid"))
	})
})

var _ = Describe("Dataset metadata normalization", func() {
	It("fills stable defaults for sparse datasets", func() {
		dataset := &model.Dataset{Title: "2026 Movie Reviews!"}

		model.NormalizeDatasetMetadata(dataset)

		Expect(dataset.ID).To(Equal(uuid.Nil))
		Expect(dataset.TableNamespace).To(Equal("default"))
		Expect(dataset.TableName).To(Equal("dataset_2026_movie_reviews"))
		Expect(dataset.SchemaVersion).To(Equal(1))
		Expect(dataset.SchemaMetadata).To(Equal("{}"))
		Expect(dataset.DatasetVersion).To(Equal(1))
	})

	It("does not overwrite explicit metadata", func() {
		datasetID := uuid.New()
		dataset := &model.Dataset{
			ID:              datasetID,
			TableNamespace:  "features",
			TableName:       "movie_features",
			SchemaVersion:   3,
			SchemaMetadata:  `{"columns":["title"]}`,
			DatasetVersion:  5,
			CatalogProvider: model.PolarisCatalog,
		}

		model.NormalizeDatasetMetadata(dataset)

		Expect(dataset.ID).To(Equal(datasetID))
		Expect(dataset.TableNamespace).To(Equal("features"))
		Expect(dataset.TableName).To(Equal("movie_features"))
		Expect(dataset.SchemaVersion).To(Equal(3))
		Expect(dataset.SchemaMetadata).To(Equal(`{"columns":["title"]}`))
		Expect(dataset.DatasetVersion).To(Equal(5))
		Expect(dataset.CatalogProvider).To(Equal(model.PolarisCatalog))
	})

	It("falls back to a generic table name when the title sanitizes empty", func() {
		datasetID := uuid.MustParse("4f4c95e4-9f8e-4491-a413-0d3eb9b3d67f")
		dataset := model.NewDataset(datasetID)
		dataset.Title = "!!!"

		model.NormalizeDatasetMetadata(dataset)

		Expect(dataset.TableName).To(Equal("dataset"))
	})
})

var _ = Describe("Dataset filters", func() {
	It("builds SQL placeholders and named args for category filters", func() {
		args := map[string]any{}
		filter := model.CategoryFilter{Values: []string{"movies", "books"}}

		sql := filter.GetFilterAndFillArguments("category", args)

		Expect(filter.GetType()).To(Equal(model.FilterByCategory))
		Expect(sql).To(Equal("category IN (@value_0,@value_1)"))
		Expect(args).To(Equal(map[string]any{"value_0": "movies", "value_1": "books"}))
	})

	It("handles empty category filters without inventing values", func() {
		args := map[string]any{}
		filter := model.CategoryFilter{}

		sql := filter.GetFilterAndFillArguments("category", args)

		Expect(sql).To(Equal("category IN ()"))
		Expect(args).To(BeEmpty())
	})
})

var _ = Describe("Connector storage types", func() {
	DescribeTable("reports the storage type for connector configs",
		func(config model.ConnectorConfig, expected model.StorageType) {
			Expect(config.GetStorageType()).To(Equal(expected))
		},
		Entry("aws s3", &model.AwsS3StorageConnCfg{}, model.S3),
		Entry("azure", &model.AzureStorageConnCfg{}, model.AzureStorage),
		Entry("gcs", &model.GoogleCloudStorageConnCfg{}, model.GoogleCloudStorage),
		Entry("postgres", &model.PostgresDBConnCfg{}, model.Postgres),
		Entry("mysql", &model.MysqlDBConnCfg{}, model.MySQL),
		Entry("oracle", &model.OracleDBConnCfg{}, model.Oracle),
		Entry("mongo", &model.MongoDBConnCfg{}, model.MongoDB),
		Entry("clickhouse", &model.ClickHouseConnCfg{}, model.ClickHouse),
	)
})
