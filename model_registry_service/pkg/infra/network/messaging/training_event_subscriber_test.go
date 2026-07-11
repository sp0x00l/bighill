package messaging_test

import (
	"context"
	"errors"
	"testing"

	transport "lib/shared_lib/transport"
	"model_registry_service/pkg/domain/model"
	registrymessaging "model_registry_service/pkg/infra/network/messaging"

	ingestionpb "lib/data_contracts_lib/ingestion"
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
	ingestedModel     *model.Model
	promotionReport   model.PromotionReportResult
	idempotencyKey    uuid.UUID
	completedResponse *model.Model
	failedResponse    *model.Model
	err               error
}

func (r *recordingModelRegistryUsecase) RegisterModel(context.Context, *model.Model, uuid.UUID) (*model.Model, error) {
	return nil, nil
}

func (r *recordingModelRegistryUsecase) ReadModelSystem(context.Context, uuid.UUID) (*model.Model, error) {
	return nil, nil
}

func (r *recordingModelRegistryUsecase) ReadModelForUser(context.Context, uuid.UUID, uuid.UUID) (*model.Model, error) {
	return nil, nil
}

func (r *recordingModelRegistryUsecase) ListModels(context.Context, uuid.UUID, transport.Pagination, model.ListFilter) ([]*model.Model, int, error) {
	return nil, 0, nil
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

func (r *recordingModelRegistryUsecase) RecordModelArtifactIngested(_ context.Context, ingestedModel *model.Model, idempotencyKey uuid.UUID) (*model.Model, error) {
	r.ingestedModel = ingestedModel
	r.idempotencyKey = idempotencyKey
	return ingestedModel, r.err
}

func (r *recordingModelRegistryUsecase) RecordModelServingStatus(context.Context, *model.ServedModelStatus, uuid.UUID) (*model.Model, error) {
	return nil, nil
}

func (r *recordingModelRegistryUsecase) RecordPromotionReportReady(_ context.Context, report model.PromotionReportResult, idempotencyKey uuid.UUID) (*model.Model, error) {
	r.promotionReport = report
	r.idempotencyKey = idempotencyKey
	return &model.Model{ModelID: report.ModelID}, r.err
}

func (r *recordingModelRegistryUsecase) PromoteCandidate(_ context.Context, modelID uuid.UUID) (*model.Model, error) {
	if r.completedResponse != nil {
		return r.completedResponse, r.err
	}
	return &model.Model{ModelID: modelID}, r.err
}

var _ = Describe("Training event listeners", func() {
	It("maps completed training events into candidates and requests promotion", func() {
		datasetID := uuid.New()
		userID := uuid.New()
		orgID := uuid.New()
		trainingRunID := uuid.New()
		uc := &recordingModelRegistryUsecase{}
		listener := registrymessaging.NewModelTrainingCompletedEventListener(uc)

		err := listener.Handle(context.Background(), datasetID, &trainingpb.ModelTrainingCompletedEvent{
			TrainingRunId:     trainingRunID.String(),
			UserId:            userID.String(),
			OrgId:             orgID.String(),
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
			AdapterRank:       16,
			ServingTarget:     "vllm-local",
			ServingModel:      "movie-ranker-v4",
			ServingLoadStatus: "LOADED",
			MetricsMetadata:   `{"eval_loss":0.12}`,
			ReportLocation:    "s3://local-dev-bucket/evals/run.json",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(uc.idempotencyKey).To(Equal(trainingRunID))
		Expect(uc.completedModel.UserID).To(Equal(userID))
		Expect(uc.completedModel.OrgID).To(Equal(orgID))
		Expect(uc.completedModel.TrainingRunID).To(Equal(trainingRunID))
		Expect(uc.completedModel.DatasetID).To(Equal(datasetID))
		Expect(uc.completedModel.ModelVersion).To(Equal(4))
		Expect(uc.completedModel.ArtifactLocation).To(Equal("s3://local-dev-bucket/models/run"))
		Expect(uc.completedModel.AdapterURI).To(Equal("s3://local-dev-bucket/models/run"))
		Expect(uc.completedModel.AdapterRank).To(Equal(16))
		Expect(uc.completedModel.ServingLoadStatus).To(Equal(model.ModelLoadStatusLoaded))
	})

	It("maps promotion report ready events into promotion decisions", func() {
		modelID := uuid.New()
		userID := uuid.New()
		orgID := uuid.New()
		trainingRunID := uuid.New()
		uc := &recordingModelRegistryUsecase{}
		listener := registrymessaging.NewPromotionReportReadyEventListener(uc)

		err := listener.Handle(context.Background(), modelID, &trainingpb.PromotionReportReadyEvent{
			UserId:             userID.String(),
			OrgId:              orgID.String(),
			ModelId:            modelID.String(),
			TrainingRunId:      trainingRunID.String(),
			PromotionReportUri: "s3://local-dev-bucket/promotion/model.json",
			DeepchecksPassed:   true,
			EvidentlyPassed:    true,
			PromotionDeltas:    `{"faithfulness":0.2}`,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(uc.idempotencyKey).To(Equal(modelID))
		Expect(uc.promotionReport.UserID).To(Equal(userID))
		Expect(uc.promotionReport.OrgID).To(Equal(orgID))
		Expect(uc.promotionReport.ModelID).To(Equal(modelID))
		Expect(uc.promotionReport.TrainingRunID).To(Equal(trainingRunID))
		Expect(uc.promotionReport.PromotionReportURI).To(Equal("s3://local-dev-bucket/promotion/model.json"))
		Expect(uc.promotionReport.Deltas).To(HaveKeyWithValue("faithfulness", 0.2))
	})

	It("maps failed training events into failed model registrations", func() {
		datasetID := uuid.New()
		userID := uuid.New()
		orgID := uuid.New()
		trainingRunID := uuid.New()
		uc := &recordingModelRegistryUsecase{}
		listener := registrymessaging.NewModelTrainingFailedEventListener(uc)

		err := listener.Handle(context.Background(), datasetID, &trainingpb.ModelTrainingFailedEvent{
			TrainingRunId:  trainingRunID.String(),
			UserId:         userID.String(),
			OrgId:          orgID.String(),
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
		Expect(uc.failedModel.UserID).To(Equal(userID))
		Expect(uc.failedModel.OrgID).To(Equal(orgID))
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
			UserId:        uuid.NewString(),
			OrgId:         uuid.NewString(),
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
			UserId:            uuid.NewString(),
			OrgId:             uuid.NewString(),
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

var _ = Describe("Model artifact ingested listener", func() {
	It("maps uploaded base model artifacts into model registrations", func() {
		artifactID := uuid.New()
		uploadID := uuid.New()
		datasetID := uuid.New()
		userID := uuid.New()
		orgID := uuid.New()
		uc := &recordingModelRegistryUsecase{}
		listener := registrymessaging.NewModelArtifactIngestedEventListener(uc)

		err := listener.Handle(context.Background(), artifactID, &ingestionpb.ModelArtifactIngestedEvent{
			ArtifactId:        artifactID.String(),
			UploadId:          uploadID.String(),
			UserId:            userID.String(),
			OrgId:             orgID.String(),
			DatasetId:         datasetID.String(),
			Source:            "upload",
			StorageLocation:   "s3://local-dev-bucket/models/artifacts/base",
			ArtifactType:      "BASE_MODEL",
			ArtifactFormat:    "HF_MODEL",
			ArtifactSizeBytes: 2048,
			ArtifactChecksum:  "sha256:base",
			FileName:          "model.safetensors",
			ModelName:         "llama-local",
			ModelVersion:      "1",
			BaseModel:         "meta-llama/Llama-3.1-8B-Instruct",
			ContentType:       "application/octet-stream",
			SourceMetadata:    `{"upload_id":"` + uploadID.String() + `"}`,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(uc.idempotencyKey).To(Equal(uploadID))
		Expect(uc.ingestedModel.ModelID).To(Equal(artifactID))
		Expect(uc.ingestedModel.UserID).To(Equal(userID))
		Expect(uc.ingestedModel.OrgID).To(Equal(orgID))
		Expect(uc.ingestedModel.DatasetID).To(Equal(datasetID))
		Expect(uc.ingestedModel.ModelKind).To(Equal(model.ModelKindBase))
		Expect(uc.ingestedModel.Source).To(Equal(model.ModelSourceUpload))
		Expect(uc.ingestedModel.ArtifactLocation).To(Equal("s3://local-dev-bucket/models/artifacts/base"))
		Expect(uc.ingestedModel.AdapterURI).To(Equal(""))
		Expect(uc.ingestedModel.MetricsMetadata).To(Equal("{}"))
	})

	It("maps uploaded LoRA adapters to adapter-backed model registrations", func() {
		artifactID := uuid.New()
		uploadID := uuid.New()
		datasetID := uuid.New()
		userID := uuid.New()
		orgID := uuid.New()
		uc := &recordingModelRegistryUsecase{}
		listener := registrymessaging.NewModelArtifactIngestedEventListener(uc)

		err := listener.Handle(context.Background(), artifactID, &ingestionpb.ModelArtifactIngestedEvent{
			ArtifactId:        artifactID.String(),
			UploadId:          uploadID.String(),
			UserId:            userID.String(),
			OrgId:             orgID.String(),
			DatasetId:         datasetID.String(),
			Source:            "upload",
			StorageLocation:   "s3://local-dev-bucket/models/artifacts/adapter",
			ArtifactType:      "LORA_ADAPTER",
			ArtifactFormat:    "HF_PEFT_ADAPTER",
			ArtifactSizeBytes: 1024,
			ArtifactChecksum:  "sha256:adapter",
			ModelName:         "movie-adapter",
			ModelVersion:      "2",
			BaseModel:         "mistral-7b",
			AdapterRank:       16,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(uc.ingestedModel.UserID).To(Equal(userID))
		Expect(uc.ingestedModel.OrgID).To(Equal(orgID))
		Expect(uc.ingestedModel.ModelKind).To(Equal(model.ModelKindFineTuned))
		Expect(uc.ingestedModel.DatasetID).To(Equal(datasetID))
		Expect(uc.ingestedModel.AdapterURI).To(Equal("s3://local-dev-bucket/models/artifacts/adapter"))
		Expect(uc.ingestedModel.AdapterRank).To(Equal(16))
		Expect(uc.ingestedModel.ModelVersion).To(Equal(2))
	})

	It("maps uploaded model artifacts without a provenance dataset", func() {
		artifactID := uuid.New()
		uploadID := uuid.New()
		userID := uuid.New()
		orgID := uuid.New()
		uc := &recordingModelRegistryUsecase{}
		listener := registrymessaging.NewModelArtifactIngestedEventListener(uc)

		err := listener.Handle(context.Background(), artifactID, &ingestionpb.ModelArtifactIngestedEvent{
			ArtifactId:        artifactID.String(),
			UploadId:          uploadID.String(),
			UserId:            userID.String(),
			OrgId:             orgID.String(),
			Source:            "upload",
			StorageLocation:   "s3://local-dev-bucket/models/artifacts/base",
			ArtifactType:      "BASE_MODEL",
			ArtifactFormat:    "HF_MODEL",
			ArtifactSizeBytes: 1024,
			ArtifactChecksum:  "sha256:base",
			ModelName:         "movie-base",
			ModelVersion:      "1",
			BaseModel:         "mistral-7b",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(uc.ingestedModel.UserID).To(Equal(userID))
		Expect(uc.ingestedModel.OrgID).To(Equal(orgID))
		Expect(uc.ingestedModel.DatasetID).To(Equal(uuid.Nil))
	})

	It("rejects uploaded model artifacts without an owning user", func() {
		artifactID := uuid.New()
		uploadID := uuid.New()
		uc := &recordingModelRegistryUsecase{}
		listener := registrymessaging.NewModelArtifactIngestedEventListener(uc)

		err := listener.Handle(context.Background(), artifactID, &ingestionpb.ModelArtifactIngestedEvent{
			ArtifactId:        artifactID.String(),
			UploadId:          uploadID.String(),
			DatasetId:         uuid.NewString(),
			Source:            "upload",
			StorageLocation:   "s3://local-dev-bucket/models/artifacts/adapter",
			ArtifactType:      "LORA_ADAPTER",
			ArtifactFormat:    "HF_PEFT_ADAPTER",
			ArtifactSizeBytes: 1024,
			ArtifactChecksum:  "sha256:adapter",
			ModelName:         "movie-adapter",
			ModelVersion:      "2",
			BaseModel:         "mistral-7b",
			AdapterRank:       16,
		})

		Expect(err).To(MatchError(ContainSubstring("user_id required")))
	})

	It("rejects uploaded model artifacts without an owning org", func() {
		artifactID := uuid.New()
		uploadID := uuid.New()
		uc := &recordingModelRegistryUsecase{}
		listener := registrymessaging.NewModelArtifactIngestedEventListener(uc)

		err := listener.Handle(context.Background(), artifactID, &ingestionpb.ModelArtifactIngestedEvent{
			ArtifactId:        artifactID.String(),
			UploadId:          uploadID.String(),
			UserId:            uuid.NewString(),
			DatasetId:         uuid.NewString(),
			Source:            "upload",
			StorageLocation:   "s3://local-dev-bucket/models/artifacts/adapter",
			ArtifactType:      "LORA_ADAPTER",
			ArtifactFormat:    "HF_PEFT_ADAPTER",
			ArtifactSizeBytes: 1024,
			ArtifactChecksum:  "sha256:adapter",
			ModelName:         "movie-adapter",
			ModelVersion:      "2",
			BaseModel:         "mistral-7b",
			AdapterRank:       16,
		})

		Expect(err).To(MatchError(ContainSubstring("org_id required")))
	})
})
