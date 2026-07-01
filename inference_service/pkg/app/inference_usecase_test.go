package app_test

import (
	"context"
	"errors"
	"testing"

	"inference_service/pkg/app"
	"inference_service/pkg/domain"
	"inference_service/pkg/domain/model"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestApp(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Inference service app unit test suite")
}

type inferenceModelRepositoryStub struct {
	model          *model.InferenceModel
	upsertedModel  *model.InferenceModel
	idempotencyKey uuid.UUID
	readID         uuid.UUID
	err            error
}

func (s *inferenceModelRepositoryStub) UpsertModel(_ context.Context, inferenceModel *model.InferenceModel, idempotencyKey uuid.UUID) (*model.InferenceModel, error) {
	s.upsertedModel = inferenceModel
	s.idempotencyKey = idempotencyKey
	if s.err != nil {
		return nil, s.err
	}
	return inferenceModel, nil
}

func (s *inferenceModelRepositoryStub) ReadByID(_ context.Context, modelID uuid.UUID) (*model.InferenceModel, error) {
	s.readID = modelID
	if s.err != nil {
		return nil, s.err
	}
	return s.model, nil
}

var _ = Describe("InferenceUsecase", func() {
	It("records a complete model update", func() {
		repository := &inferenceModelRepositoryStub{}
		uc := app.NewInferenceUsecase(repository)
		idempotencyKey := uuid.New()

		recorded, err := uc.RecordModelUpdated(context.Background(), validInferenceModel(), idempotencyKey)

		Expect(err).NotTo(HaveOccurred())
		Expect(recorded.ModelID).To(Equal(repository.upsertedModel.ModelID))
		Expect(repository.idempotencyKey).To(Equal(idempotencyKey))
	})

	It("reads a model by id", func() {
		expected := validInferenceModel()
		repository := &inferenceModelRepositoryStub{model: expected}
		uc := app.NewInferenceUsecase(repository)

		readModel, err := uc.ReadModel(context.Background(), expected.ModelID)

		Expect(err).NotTo(HaveOccurred())
		Expect(readModel).To(Equal(expected))
		Expect(repository.readID).To(Equal(expected.ModelID))
	})

	It("rejects missing model identity", func() {
		uc := app.NewInferenceUsecase(&inferenceModelRepositoryStub{})

		_, err := uc.RecordModelUpdated(context.Background(), &model.InferenceModel{}, uuid.New())

		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
	})

	It("rejects ready models without artifact locations", func() {
		uc := app.NewInferenceUsecase(&inferenceModelRepositoryStub{})
		inferenceModel := validInferenceModel()
		inferenceModel.ArtifactLocation = ""

		_, err := uc.RecordModelUpdated(context.Background(), inferenceModel, uuid.New())

		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
	})
})

func validInferenceModel() *model.InferenceModel {
	return &model.InferenceModel{
		ModelID:           uuid.New(),
		TrainingRunID:     uuid.New(),
		DatasetID:         uuid.New(),
		Name:              "sentence-transformer",
		ModelVersion:      1,
		BaseModel:         "base-model",
		ArtifactLocation:  "s3://local-dev-bucket/models/model-1",
		ArtifactFormat:    "HF_PEFT_ADAPTER",
		ArtifactChecksum:  "checksum",
		ArtifactSizeBytes: 10,
		MetricsMetadata:   "{}",
		Status:            model.ModelStatusReady,
	}
}
