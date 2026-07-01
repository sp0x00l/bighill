package adapter

import (
	"context"
	"encoding/json"

	serializers "data_registry_service/pkg/common/serializer"
	"data_registry_service/pkg/domain/model"

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
})
