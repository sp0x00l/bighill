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
		payload, err := adapter.ToDTO(context.Background(), &model.Dataset{
			ID:                uuid.New(),
			UserID:            uuid.New(),
			Title:             "RAG corpus",
			ProcessingProfile: model.TextRAGProfile,
		}, "/datasets")

		Expect(err).NotTo(HaveOccurred())
		var dto DatasetDTO
		Expect(json.Unmarshal(payload, &dto)).To(Succeed())
		Expect(dto.ProcessingProfile).To(Equal(model.TextRAGProfile.String()))
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
})
