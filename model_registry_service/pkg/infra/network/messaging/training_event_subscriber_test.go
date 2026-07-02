package messaging_test

import (
	"context"
	"errors"
	"testing"

	"model_registry_service/pkg/domain/model"
	registrymessaging "model_registry_service/pkg/infra/network/messaging"

	trainingpb "lib/data_contracts_lib/training"
	shared "lib/shared_lib/messaging"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestMessaging(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Model registry messaging unit test suite")
}

type recordingModelRegistryUsecase struct {
	completedModel    *model.Model
	failedModel       *model.Model
	idempotencyKey    uuid.UUID
	completedResponse *model.Model
	failedResponse    *model.Model
	err               error
}

func (r *recordingModelRegistryUsecase) RegisterModel(context.Context, *model.Model, uuid.UUID) (*model.Model, error) {
	return nil, nil
}

func (r *recordingModelRegistryUsecase) ReadModel(context.Context, uuid.UUID) (*model.Model, error) {
	return nil, nil
}

func (r *recordingModelRegistryUsecase) MarkModelReady(context.Context, uuid.UUID, string) (*model.Model, error) {
	return nil, nil
}

func (r *recordingModelRegistryUsecase) MarkModelFailed(context.Context, uuid.UUID, string) (*model.Model, error) {
	return nil, nil
}

func (r *recordingModelRegistryUsecase) RecordModelTrainingCompleted(_ context.Context, trainedModel *model.Model, idempotencyKey uuid.UUID) (*model.Model, error) {
	r.completedModel = trainedModel
	r.idempotencyKey = idempotencyKey
	if r.completedResponse != nil {
		return r.completedResponse, r.err
	}
	return trainedModel, r.err
}

func (r *recordingModelRegistryUsecase) RecordModelTrainingFailed(_ context.Context, failedModel *model.Model, idempotencyKey uuid.UUID) (*model.Model, error) {
	r.failedModel = failedModel
	r.idempotencyKey = idempotencyKey
	if r.failedResponse != nil {
		return r.failedResponse, r.err
	}
	return failedModel, r.err
}

func (r *recordingModelRegistryUsecase) RecordModelServingStatus(context.Context, *model.ServedModelStatus, uuid.UUID) (*model.Model, error) {
	return nil, nil
}

var _ = Describe("Training event listeners", func() {
	It("maps completed training events into ready model registrations", func() {
		datasetID := uuid.New()
		trainingRunID := uuid.New()
		uc := &recordingModelRegistryUsecase{}
		listener := registrymessaging.NewModelTrainingCompletedEventListener(uc)

		err := listener.Handle(context.Background(), datasetID, &trainingpb.ModelTrainingCompletedEvent{
			TrainingRunId:     trainingRunID.String(),
			DatasetId:         datasetID.String(),
			DatasetVersion:    "4",
			FeatureSnapshotId: uuid.NewString(),
			ModelId:           uuid.NewString(),
			ModelName:         "movie-ranker",
			ModelVersion:      "4",
			BaseModel:         "mistral-7b",
			ArtifactLocation:  "s3://local-dev-bucket/models/run",
			ArtifactFormat:    "HF_PEFT_ADAPTER",
			ArtifactChecksum:  "sha256:abc",
			ArtifactSizeBytes: 128,
			AdapterUri:        "s3://local-dev-bucket/models/run",
			ServingTarget:     "vllm-local",
			ServingModel:      "movie-ranker-v4",
			ServingLoadStatus: "LOADED",
			MetricsMetadata:   `{"eval_loss":0.12}`,
			ReportLocation:    "s3://local-dev-bucket/evals/run.json",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(uc.idempotencyKey).To(Equal(trainingRunID))
		Expect(uc.completedModel.TrainingRunID).To(Equal(trainingRunID))
		Expect(uc.completedModel.DatasetID).To(Equal(datasetID))
		Expect(uc.completedModel.ModelVersion).To(Equal(4))
		Expect(uc.completedModel.ArtifactLocation).To(Equal("s3://local-dev-bucket/models/run"))
		Expect(uc.completedModel.AdapterURI).To(Equal("s3://local-dev-bucket/models/run"))
		Expect(uc.completedModel.ServingLoadStatus).To(Equal(model.ModelLoadStatusLoaded))
	})

	It("maps failed training events into failed model registrations", func() {
		datasetID := uuid.New()
		trainingRunID := uuid.New()
		uc := &recordingModelRegistryUsecase{}
		listener := registrymessaging.NewModelTrainingFailedEventListener(uc)

		err := listener.Handle(context.Background(), datasetID, &trainingpb.ModelTrainingFailedEvent{
			TrainingRunId:  trainingRunID.String(),
			DatasetId:      datasetID.String(),
			DatasetVersion: "5",
			ModelId:        uuid.NewString(),
			ModelName:      "movie-ranker",
			ModelVersion:   "dataset-v5",
			BaseModel:      "mistral-7b",
			FailureReason:  "training failed",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(uc.idempotencyKey).To(Equal(trainingRunID))
		Expect(uc.failedModel.TrainingRunID).To(Equal(trainingRunID))
		Expect(uc.failedModel.DatasetID).To(Equal(datasetID))
		Expect(uc.failedModel.ModelVersion).To(Equal(5))
		Expect(uc.failedModel.MetricsMetadata).To(Equal("{}"))
		Expect(uc.failedModel.FailureReason).To(Equal("training failed"))
	})

	It("returns non-retryable errors for invalid wire payloads", func() {
		datasetID := uuid.New()
		listener := registrymessaging.NewModelTrainingCompletedEventListener(&recordingModelRegistryUsecase{})

		err := listener.Handle(context.Background(), datasetID, &trainingpb.ModelTrainingCompletedEvent{
			TrainingRunId: uuid.NewString(),
			DatasetId:     uuid.NewString(),
			ModelId:       uuid.NewString(),
			BaseModel:     "mistral-7b",
		})

		Expect(err).To(HaveOccurred())
		Expect(shared.IsNonRetryable(err)).To(BeTrue())
	})

	It("propagates usecase errors", func() {
		datasetID := uuid.New()
		expectedErr := errors.New("db unavailable")
		listener := registrymessaging.NewModelTrainingCompletedEventListener(&recordingModelRegistryUsecase{err: expectedErr})

		err := listener.Handle(context.Background(), datasetID, &trainingpb.ModelTrainingCompletedEvent{
			TrainingRunId:     uuid.NewString(),
			DatasetId:         datasetID.String(),
			DatasetVersion:    "1",
			ModelId:           uuid.NewString(),
			ModelName:         "movie-ranker",
			ModelVersion:      "1",
			BaseModel:         "mistral-7b",
			ArtifactLocation:  "s3://local-dev-bucket/models/run",
			ArtifactFormat:    "HF_PEFT_ADAPTER",
			ArtifactChecksum:  "sha256:abc",
			ArtifactSizeBytes: 128,
			AdapterUri:        "s3://local-dev-bucket/models/run",
			MetricsMetadata:   `{"eval_loss":0.12}`,
		})

		Expect(errors.Is(err, expectedErr)).To(BeTrue())
	})
})
