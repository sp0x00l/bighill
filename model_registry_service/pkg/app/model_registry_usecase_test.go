package app_test

import (
	"context"
	"testing"

	"model_registry_service/pkg/app"
	"model_registry_service/pkg/domain"
	"model_registry_service/pkg/domain/model"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestApp(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Model registry app unit test suite")
}

type modelRepositoryStub struct {
	createdModel *model.Model
	readModel    *model.Model
	status       model.ModelStatus
	createErr    error
	readErr      error
	updateErr    error
}

func (s *modelRepositoryStub) Close() {}

func (s *modelRepositoryStub) Create(_ context.Context, registeredModel *model.Model, _ uuid.UUID) (*model.Model, error) {
	s.createdModel = registeredModel
	return registeredModel, s.createErr
}

func (s *modelRepositoryStub) ReadByID(context.Context, uuid.UUID) (*model.Model, error) {
	return s.readModel, s.readErr
}

func (s *modelRepositoryStub) ReadByTrainingRunID(context.Context, uuid.UUID) (*model.Model, error) {
	return s.readModel, s.readErr
}

func (s *modelRepositoryStub) UpdateStatus(_ context.Context, _ uuid.UUID, status model.ModelStatus, _, _ string) (*model.Model, error) {
	s.status = status
	return s.readModel, s.updateErr
}

var _ = Describe("ModelRegistryUsecase", func() {
	It("registers a model through the repository", func() {
		repo := &modelRepositoryStub{}
		uc := app.NewModelRegistryUsecase(repo)
		registeredModel := validModel()

		result, err := uc.RegisterModel(context.Background(), registeredModel, uuid.New())

		Expect(err).NotTo(HaveOccurred())
		Expect(result.ModelID).NotTo(Equal(uuid.Nil))
		Expect(repo.createdModel).To(Equal(registeredModel))
	})

	It("marks a model ready", func() {
		repo := &modelRepositoryStub{readModel: validModel()}
		uc := app.NewModelRegistryUsecase(repo)

		result, err := uc.MarkModelReady(context.Background(), uuid.New(), "s3://models/run/model")

		Expect(err).NotTo(HaveOccurred())
		Expect(repo.status).To(Equal(model.ModelStatusReady))
		Expect(result).NotTo(BeNil())
	})

	It("marks a model failed", func() {
		repo := &modelRepositoryStub{readModel: validModel()}
		uc := app.NewModelRegistryUsecase(repo)

		_, err := uc.MarkModelFailed(context.Background(), uuid.New(), "training failed")

		Expect(err).NotTo(HaveOccurred())
		Expect(repo.status).To(Equal(model.ModelStatusFailed))
	})

	It("records completed training as a ready model", func() {
		repo := &modelRepositoryStub{}
		uc := app.NewModelRegistryUsecase(repo)
		trainedModel := validModel()
		trainedModel.ArtifactLocation = "s3://models/run/model"

		result, err := uc.RecordModelTrainingCompleted(context.Background(), trainedModel, uuid.New())

		Expect(err).NotTo(HaveOccurred())
		Expect(result.Status).To(Equal(model.ModelStatusReady))
		Expect(repo.createdModel.ArtifactLocation).To(Equal("s3://models/run/model"))
	})

	It("returns existing training-run records on replay", func() {
		existing := validModel()
		repo := &modelRepositoryStub{readModel: existing, createErr: domain.ErrModelExists}
		uc := app.NewModelRegistryUsecase(repo)
		trainedModel := validModel()
		trainedModel.ArtifactLocation = "s3://models/run/model"

		result, err := uc.RecordModelTrainingCompleted(context.Background(), trainedModel, uuid.New())

		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(existing))
	})

	It("records failed training as a failed model", func() {
		repo := &modelRepositoryStub{}
		uc := app.NewModelRegistryUsecase(repo)
		failedModel := validModel()
		failedModel.FailureReason = "training failed"

		result, err := uc.RecordModelTrainingFailed(context.Background(), failedModel, uuid.New())

		Expect(err).NotTo(HaveOccurred())
		Expect(result.Status).To(Equal(model.ModelStatusFailed))
		Expect(repo.createdModel.FailureReason).To(Equal("training failed"))
	})

})

func validModel() *model.Model {
	return &model.Model{
		ModelID:           uuid.New(),
		TrainingRunID:     uuid.New(),
		DatasetID:         uuid.New(),
		Name:              "movie-ranker",
		ModelVersion:      1,
		BaseModel:         "mistral-7b",
		ArtifactLocation:  "s3://local-dev-bucket/models/pending",
		ArtifactFormat:    "HF_PEFT_ADAPTER",
		ArtifactChecksum:  "sha256:pending",
		ArtifactSizeBytes: 1,
		MetricsMetadata:   `{"eval_loss":0.12}`,
	}
}
