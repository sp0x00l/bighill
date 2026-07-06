package messaging

import (
	"model_registry_service/pkg/domain/model"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
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
})

func validEventBuilderModel() *model.Model {
	userID := uuid.New()
	datasetID := uuid.New()
	trainingRunID := uuid.New()
	return &model.Model{
		ModelID:            uuid.New(),
		UserID:             userID,
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
