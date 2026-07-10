package adapter

import (
	"context"
	"encoding/json"
	"testing"

	serializers "lib/shared_lib/serializer"
	"model_registry_service/pkg/domain/model"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestModelDTOAdapter(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Model DTO Adapter Suite")
}

var _ = Describe("ModelDTOAdapter", func() {
	var (
		adapter *modelDTOAdapter
		record  *model.Model
		baseURL string
	)

	BeforeEach(func() {
		adapter = NewModelDTOAdapter(serializers.NewJSONSerializer())
		baseURL = "http://model-registry.local/v1/models"
		record = &model.Model{
			ModelID:            uuid.New(),
			UserID:             uuid.New(),
			OrgID:              uuid.New(),
			TrainingRunID:      uuid.New(),
			DatasetID:          uuid.New(),
			ModelKind:          model.ModelKindFineTuned,
			Source:             model.ModelSourceTraining,
			SourceURI:          "s3://bucket/training/model",
			Name:               "trained-rag-model",
			ModelVersion:       7,
			BaseModel:          "llama3.1:8b-instruct",
			ArtifactLocation:   "s3://bucket/models/model-1",
			ArtifactFormat:     "GGUF_LORA_ADAPTER",
			ArtifactChecksum:   "sha256:abc",
			ArtifactSizeBytes:  2048,
			AdapterURI:         "s3://bucket/models/model-1/adapter.gguf",
			ServingTarget:      "local",
			ServingModel:       "bighill-trained-rag-model-v7-abc",
			ServingProtocol:    model.ServingProtocolOpenAIChatCompletions,
			ServingLoadStatus:  model.ModelLoadStatusLoaded,
			PromotionReportURI: "s3://bucket/reports/report.json",
			PromotionDeltas:    `{"loss":-0.1}`,
			PromotionDecision:  "promote",
			PromotionReason:    "candidate improved validation metric",
			Status:             model.ModelStatusReady,
			FailureReason:      "",
		}
	})

	It("encodes a model record into the public DTO shape", func() {
		payload, err := adapter.ToDTO(context.Background(), record, baseURL)
		Expect(err).NotTo(HaveOccurred())

		var dto ModelDTO
		Expect(json.Unmarshal(payload, &dto)).To(Succeed())
		Expect(dto.ID).To(Equal(record.ModelID.String()))
		Expect(dto.UserID).To(Equal(record.UserID.String()))
		Expect(dto.OrgID).To(Equal(record.OrgID.String()))
		Expect(dto.TrainingRunID).To(Equal(record.TrainingRunID.String()))
		Expect(dto.DatasetID).To(Equal(record.DatasetID.String()))
		Expect(dto.ModelKind).To(Equal(record.ModelKind.String()))
		Expect(dto.Source).To(Equal(record.Source.String()))
		Expect(dto.SourceURI).To(Equal(record.SourceURI))
		Expect(dto.Name).To(Equal(record.Name))
		Expect(dto.ModelVersion).To(Equal(record.ModelVersion))
		Expect(dto.BaseModel).To(Equal(record.BaseModel))
		Expect(dto.ArtifactLocation).To(Equal(record.ArtifactLocation))
		Expect(dto.ArtifactFormat).To(Equal(record.ArtifactFormat))
		Expect(dto.ArtifactChecksum).To(Equal(record.ArtifactChecksum))
		Expect(dto.ArtifactSizeBytes).To(Equal(record.ArtifactSizeBytes))
		Expect(dto.AdapterURI).To(Equal(record.AdapterURI))
		Expect(dto.ServingTarget).To(Equal(record.ServingTarget))
		Expect(dto.ServingModel).To(Equal(record.ServingModel))
		Expect(dto.ServingProtocol).To(Equal(record.ServingProtocol.String()))
		Expect(dto.ServingLoadStatus).To(Equal(record.ServingLoadStatus.String()))
		Expect(dto.PromotionReportURI).To(Equal(record.PromotionReportURI))
		Expect(dto.PromotionDeltas).To(Equal(record.PromotionDeltas))
		Expect(dto.PromotionDecision).To(Equal(record.PromotionDecision))
		Expect(dto.PromotionReason).To(Equal(record.PromotionReason))
		Expect(dto.Status).To(Equal(record.Status.String()))
		Expect(dto.FailureReason).To(BeEmpty())
		Expect(dto.Links.Self.Href).To(Equal(baseURL + "/" + record.ModelID.String()))
	})

	It("omits optional UUID fields when they are not present", func() {
		record.UserID = uuid.Nil
		record.OrgID = uuid.Nil
		record.TrainingRunID = uuid.Nil
		record.DatasetID = uuid.Nil

		payload, err := adapter.ToDTO(context.Background(), record, baseURL)
		Expect(err).NotTo(HaveOccurred())

		var raw map[string]any
		Expect(json.Unmarshal(payload, &raw)).To(Succeed())
		Expect(raw).NotTo(HaveKey("user_id"))
		Expect(raw).NotTo(HaveKey("org_id"))
		Expect(raw).NotTo(HaveKey("training_run_id"))
		Expect(raw).NotTo(HaveKey("dataset_id"))
	})

	It("maps model lists to DTO resources", func() {
		second := *record
		second.ModelID = uuid.New()
		second.Name = "second-model"

		resources := adapter.ToDTOs(context.Background(), []*model.Model{record, &second}, baseURL)
		Expect(resources).To(HaveLen(2))

		first, ok := resources[0].(*ModelDTO)
		Expect(ok).To(BeTrue())
		Expect(first.ID).To(Equal(record.ModelID.String()))
		Expect(first.Links.Self.Href).To(Equal(baseURL + "/" + record.ModelID.String()))

		mappedSecond, ok := resources[1].(*ModelDTO)
		Expect(ok).To(BeTrue())
		Expect(mappedSecond.ID).To(Equal(second.ModelID.String()))
		Expect(mappedSecond.Name).To(Equal("second-model"))
	})
})
