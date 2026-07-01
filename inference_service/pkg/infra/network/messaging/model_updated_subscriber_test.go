package messaging_test

import (
	"context"
	"testing"

	"inference_service/pkg/domain/model"
	inferencemessaging "inference_service/pkg/infra/network/messaging"

	modelregistrypb "lib/data_contracts_lib/model_registry"
	sharedmessaging "lib/shared_lib/messaging"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestMessaging(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Inference service messaging unit test suite")
}

type recordingInferenceUsecase struct {
	model          *model.InferenceModel
	idempotencyKey uuid.UUID
	err            error
}

func (u *recordingInferenceUsecase) RecordModelUpdated(_ context.Context, inferenceModel *model.InferenceModel, idempotencyKey uuid.UUID) (*model.InferenceModel, error) {
	u.model = inferenceModel
	u.idempotencyKey = idempotencyKey
	return inferenceModel, u.err
}

func (u *recordingInferenceUsecase) ReadModel(context.Context, uuid.UUID) (*model.InferenceModel, error) {
	return nil, nil
}

var _ = Describe("ModelUpdatedEventListener", func() {
	It("exposes the model updated message type", func() {
		listener := inferencemessaging.NewModelUpdatedEventListener(&recordingInferenceUsecase{})

		Expect(listener.MsgType()).To(Equal(sharedmessaging.MsgTypeModelUpdated))
		Expect(listener.NewMessage()).To(Equal(&modelregistrypb.ModelUpdatedEvent{}))
	})

	It("maps a model updated event into the inference use case", func() {
		modelID := uuid.New()
		trainingRunID := uuid.New()
		datasetID := uuid.New()
		uc := &recordingInferenceUsecase{}
		listener := inferencemessaging.NewModelUpdatedEventListener(uc)

		err := listener.Handle(context.Background(), modelID, &modelregistrypb.ModelUpdatedEvent{
			ModelId:           modelID.String(),
			TrainingRunId:     trainingRunID.String(),
			DatasetId:         datasetID.String(),
			Name:              "movie-ranker",
			ModelVersion:      2,
			BaseModel:         "base-model",
			ArtifactLocation:  "s3://local-dev-bucket/models/model-1",
			ArtifactFormat:    "HF_PEFT_ADAPTER",
			ArtifactChecksum:  "checksum",
			ArtifactSizeBytes: 42,
			MetricsMetadata:   `{"accuracy":0.9}`,
			Status:            "READY",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(uc.model.ModelID).To(Equal(modelID))
		Expect(uc.model.TrainingRunID).To(Equal(trainingRunID))
		Expect(uc.model.DatasetID).To(Equal(datasetID))
		Expect(uc.model.Status).To(Equal(model.ModelStatusReady))
		Expect(uc.model.ArtifactLocation).To(Equal("s3://local-dev-bucket/models/model-1"))
		Expect(uc.idempotencyKey).NotTo(Equal(uuid.Nil))
	})

	It("defaults optional display metadata", func() {
		modelID := uuid.New()
		uc := &recordingInferenceUsecase{}
		listener := inferencemessaging.NewModelUpdatedEventListener(uc)

		err := listener.Handle(context.Background(), modelID, &modelregistrypb.ModelUpdatedEvent{
			ModelId:       modelID.String(),
			TrainingRunId: uuid.NewString(),
			DatasetId:     uuid.NewString(),
			Status:        "PENDING",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(uc.model.Name).To(HavePrefix("model_"))
		Expect(uc.model.ModelVersion).To(Equal(1))
		Expect(uc.model.MetricsMetadata).To(Equal("{}"))
	})

	It("returns non-retryable errors for invalid wire payloads", func() {
		modelID := uuid.New()
		listener := inferencemessaging.NewModelUpdatedEventListener(&recordingInferenceUsecase{})

		err := listener.Handle(context.Background(), modelID, &modelregistrypb.ModelUpdatedEvent{
			ModelId:       uuid.NewString(),
			TrainingRunId: uuid.NewString(),
			DatasetId:     uuid.NewString(),
			Status:        "READY",
		})

		Expect(err).To(HaveOccurred())
		Expect(sharedmessaging.IsNonRetryable(err)).To(BeTrue())
	})
})
