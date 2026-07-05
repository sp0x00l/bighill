package adapter

import (
	"context"
	"encoding/json"

	"data_registry_service/pkg/domain/model"
	serializers "lib/shared_lib/serializer"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("DatasetDTOAdapter", func() {
	var adapter *dtoAdapter

	BeforeEach(func() {
		adapter = NewDatasetDTOAdapter(serializers.NewJSONSerializer())
	})

	It("maps processing profile from request DTOs", func() {
		dataset, err := adapter.FromDTO(context.Background(), []byte(`{"title":"RAG corpus","processingProfile":"TEXT_RAG"}`))

		Expect(err).NotTo(HaveOccurred())
		Expect(dataset.ProcessingProfile).To(Equal(model.TextRAGProfile))
	})

	It("defaults missing processing profile to generic parquet", func() {
		dataset, err := adapter.FromDTO(context.Background(), []byte(`{"title":"Training data"}`))

		Expect(err).NotTo(HaveOccurred())
		Expect(dataset.ProcessingProfile).To(Equal(model.GenericParquetProfile))
	})

	It("rejects invalid processing profiles", func() {
		_, err := adapter.FromDTO(context.Background(), []byte(`{"title":"Invalid","processingProfile":"CUSTOM"}`))

		Expect(err).To(HaveOccurred())
	})

	It("serializes processing profile in response DTOs", func() {
		datasetID := uuid.New()
		userID := uuid.New()
		payload, err := adapter.ToDTO(context.Background(), &model.Dataset{
			ID:                datasetID,
			UserID:            userID,
			Title:             "RAG corpus",
			Origin:            model.Standard,
			Status:            model.Published,
			ProcessingProfile: model.TextRAGProfile,
			TableFormat:       model.Parquet,
			CatalogProvider:   model.LocalCatalog,
			ProcessingState:   model.DatasetProcessingRawMaterialized,
		}, "/datasets")

		Expect(err).NotTo(HaveOccurred())
		var dto DatasetDTO
		Expect(json.Unmarshal(payload, &dto)).To(Succeed())
		Expect(dto.ID).To(Equal(datasetID.String()))
		Expect(dto.UserID).To(Equal(userID.String()))
		Expect(dto.ProcessingProfile).To(Equal(model.TextRAGProfile.String()))
		Expect(dto.ProcessingState).To(Equal(model.DatasetProcessingRawMaterialized.String()))
		Expect(dto.Links.Self.Href).To(Equal("/datasets/" + datasetID.String()))
	})

	It("serializes dataset lists into resource DTOs", func() {
		firstID := uuid.New()
		secondID := uuid.New()

		resources := adapter.ToDTOs(context.Background(), []*model.Dataset{
			{
				ID:                firstID,
				UserID:            uuid.New(),
				Title:             "Training data",
				Origin:            model.Standard,
				Status:            model.Draft,
				ProcessingProfile: model.GenericParquetProfile,
				TableFormat:       model.Parquet,
				CatalogProvider:   model.LocalCatalog,
				ProcessingState:   model.DatasetProcessingPending,
			},
			{
				ID:                secondID,
				UserID:            uuid.New(),
				Title:             "RAG corpus",
				Origin:            model.Community,
				Status:            model.Published,
				ProcessingProfile: model.TextRAGProfile,
				TableFormat:       model.Iceberg,
				CatalogProvider:   model.PolarisCatalog,
				ProcessingState:   model.DatasetProcessingFeatureMaterialized,
			},
		}, "/datasets")

		Expect(resources).To(HaveLen(2))
		first, ok := resources[0].(*DatasetDTO)
		Expect(ok).To(BeTrue())
		Expect(first.ID).To(Equal(firstID.String()))
		Expect(first.Links.Self.Href).To(Equal("/datasets/" + firstID.String()))
		second, ok := resources[1].(*DatasetDTO)
		Expect(ok).To(BeTrue())
		Expect(second.ID).To(Equal(secondID.String()))
		Expect(second.TableFormat).To(Equal(model.Iceberg.String()))
	})

	It("maps connector-backed dataset source metadata", func() {
		connectorID := uuid.New()

		dataset, err := adapter.FromDTO(context.Background(), []byte(`{
			"title":"Warehouse movies",
			"processingProfile":"TEXT_RAG",
			"sourceType":"POSTGRES",
			"sourceConnectorId":"`+connectorID.String()+`",
			"sourceQuery":"SELECT title FROM movies",
			"tableNamespace":"features",
			"tableName":"movies"
		}`))

		Expect(err).NotTo(HaveOccurred())
		Expect(dataset.SourceType).To(Equal(model.Postgres))
		Expect(dataset.SourceConnectorID).To(Equal(connectorID))
		Expect(dataset.SourceQuery).To(Equal("SELECT title FROM movies"))
		Expect(dataset.TableNamespace).To(Equal("features"))
		Expect(dataset.TableName).To(Equal("movies"))
	})

	It("requires source type when a source connector id is set", func() {
		_, err := adapter.FromDTO(context.Background(), []byte(`{
			"title":"Warehouse movies",
			"sourceConnectorId":"`+uuid.NewString()+`",
			"sourceQuery":"SELECT title FROM movies"
		}`))

		Expect(err).To(HaveOccurred())
	})

	It("rejects invalid dataset and user ids", func() {
		_, err := adapter.FromDTO(context.Background(), []byte(`{"id":"not-a-uuid","title":"Invalid id"}`))
		Expect(err).To(HaveOccurred())

		_, err = adapter.FromDTO(context.Background(), []byte(`{"userId":"not-a-uuid","title":"Invalid user"}`))
		Expect(err).To(HaveOccurred())
	})

	It("rejects invalid source connector ids", func() {
		_, err := adapter.FromDTO(context.Background(), []byte(`{
			"title":"Warehouse movies",
			"sourceType":"POSTGRES",
			"sourceConnectorId":"not-a-uuid"
		}`))

		Expect(err).To(HaveOccurred())
	})

	It("rejects invalid enum metadata", func() {
		invalidBodies := [][]byte{
			[]byte(`{"title":"Invalid","origin":"vendor"}`),
			[]byte(`{"title":"Invalid","status":"archived"}`),
			[]byte(`{"title":"Invalid","tableFormat":"CSV"}`),
			[]byte(`{"title":"Invalid","catalogProvider":"NESSIE"}`),
			[]byte(`{"title":"Invalid","sourceType":"SALESFORCE"}`),
		}

		for _, body := range invalidBodies {
			_, err := adapter.FromDTO(context.Background(), body)
			Expect(err).To(HaveOccurred())
		}
	})
})
