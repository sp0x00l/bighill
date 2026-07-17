package messaging

import (
	"model_registry_service/pkg/domain/model"

	modelregistrypb "lib/data_contracts_lib/model_registry"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
)

var _ = Describe("ModelEventBuilder", func() {
	It("includes mutable event state in the model updated dispatch key", func() {
		builder := NewModelEventBuilder("model_registry")
		modelRecord := validEventBuilderModel()

		notLoaded := builder.ModelUpdatedMessage(modelRecord)
		modelRecord.ServingLoadStatus = model.ModelLoadStatusLoaded
		loaded := builder.ModelUpdatedMessage(modelRecord)

		Expect(notLoaded.DispatchKey).NotTo(Equal(loaded.DispatchKey))
	})

	It("uses a stable model updated dispatch key for identical event state", func() {
		builder := NewModelEventBuilder("model_registry")
		modelRecord := validEventBuilderModel()

		first := builder.ModelUpdatedMessage(modelRecord)
		second := builder.ModelUpdatedMessage(modelRecord)

		Expect(first.DispatchKey).To(Equal(second.DispatchKey))
	})

	It("includes org id in model updated and promotion requested payloads", func() {
		builder := NewModelEventBuilder("model_registry")
		modelRecord := validEventBuilderModel()
		modelRecord.EffectiveBaseID = "sha256-effective-base"

		modelUpdated := builder.ModelUpdatedMessage(modelRecord)
		var updatedEvent modelregistrypb.ModelUpdatedEvent
		Expect(proto.Unmarshal(modelUpdated.Message.Payload, &updatedEvent)).To(Succeed())
		Expect(updatedEvent.GetOrgId()).To(Equal(modelRecord.OrgID.String()))
		Expect(updatedEvent.GetEffectiveBaseId()).To(Equal("sha256-effective-base"))

		promotionRequested := builder.PromotionRequestedMessage(modelRecord, nil)
		var promotionEvent modelregistrypb.PromotionRequestedEvent
		Expect(proto.Unmarshal(promotionRequested.Message.Payload, &promotionEvent)).To(Succeed())
		Expect(promotionEvent.GetOrgId()).To(Equal(modelRecord.OrgID.String()))
	})
})

func validEventBuilderModel() *model.Model {
	userID := uuid.New()
	datasetID := uuid.New()
	trainingRunID := uuid.New()
	return &model.Model{
		ModelID:            uuid.New(),
		UserID:             userID,
		OrgID:              uuid.New(),
		DatasetID:          datasetID,
		TrainingRunID:      trainingRunID,
		ModelKind:          model.ModelKindFineTuned,
		Source:             model.ModelSourceTraining,
		Name:               "movie-ranker",
		ModelVersion:       7,
		BaseModel:          "mistral-7b",
		ArtifactLocation:   "s3://bucket/models/model",
		ArtifactFormat:     "HF_PEFT_ADAPTER",
		ArtifactChecksum:   "sha256:abc",
		ArtifactSizeBytes:  128,
		AdapterURI:         "s3://bucket/models/model",
		ServingTarget:      "vllm-local",
		ServingModel:       "movie-ranker-v7",
		ServingLoadStatus:  model.ModelLoadStatusNotLoaded,
		MetricsMetadata:    "{}",
		Status:             model.ModelStatusEvaluated,
		PromotionDeltas:    "{}",
		PromotionDecision:  model.PromotionDecisionOutcomeAccepted.String(),
		PromotionReportURI: "s3://bucket/promotion/model.json",
	}
}
