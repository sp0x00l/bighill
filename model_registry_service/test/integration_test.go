package integration_test

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"testing"
	"time"

	"model_registry_service/pkg/app"
	"model_registry_service/pkg/domain"
	"model_registry_service/pkg/domain/model"
	registrymessaging "model_registry_service/pkg/infra/network/messaging"
	repo "model_registry_service/pkg/infra/repo/db"

	ingestionpb "lib/data_contracts_lib/ingestion"
	modelregistrypb "lib/data_contracts_lib/model_registry"
	trainingpb "lib/data_contracts_lib/training"
	"lib/shared_lib/ctxutil"
	dbconn "lib/shared_lib/db"
	env "lib/shared_lib/env"
	sharedmessaging "lib/shared_lib/messaging"
	shareduow "lib/shared_lib/uow"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"
)

func TestModelRegistryIntegration(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Model registry integration test suite")
}

var _ = Describe("Model registry integration", Ordered, func() {
	var (
		ctx       context.Context
		cancel    context.CancelFunc
		database  *dbconn.Database
		models    app.ModelRepository
		modelsUse app.ModelRegistryUsecase
		deployer  *recordingServingDeployer
	)

	BeforeAll(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 90*time.Second)

		dbName := env.WithDefaultString("MODEL_REGISTRY_SERVICE_DB_NAME", "bighill_model_registry_db")

		var err error
		database, err = dbconn.InitDatabase(ctx, dbName, testPostgresConnectionString(dbName), log.StandardLogger())
		Expect(err).NotTo(HaveOccurred())

		models = repo.NewModelRepository(database)
		outboxWriter, err := sharedmessaging.NewPostgresOutbox(database.Pool, database.Name, "")
		Expect(err).NotTo(HaveOccurred())
		orderedOutbox, ok := outboxWriter.(sharedmessaging.OrderedOutbox)
		Expect(ok).To(BeTrue())
		deployer = &recordingServingDeployer{}
		modelsUse = app.NewModelRegistryUsecase(
			models,
			shareduow.New(database.Pool, shareduow.WithTransactionalOutbox(orderedOutbox)),
			registrymessaging.NewModelEventBuilder("model_registry"),
			app.WithModelServingDeployer(deployer),
		)
	})

	BeforeEach(func() {
		Expect(truncateModelRegistry(ctx, database)).To(Succeed())
		deployer.reset()
	})

	AfterAll(func() {
		if models != nil {
			models.Close()
		}
		if cancel != nil {
			cancel()
		}
	})

	It("persists and updates model registry records", func() {
		modelRecord := validIntegrationModel()
		Expect(upsertModelRegistryTenant(ctx, database, modelRecord.UserID)).To(Succeed())

		registeredModel, err := modelsUse.RegisterModel(ctx, modelRecord, uuid.New())
		Expect(err).NotTo(HaveOccurred())
		Expect(registeredModel.ModelID).NotTo(Equal(uuid.Nil))
		Expect(registeredModel.Status).To(Equal(model.ModelStatusPending))

		readyModel, err := modelsUse.MarkModelReady(ctx, registeredModel.ModelID, "s3://local-dev-bucket/models/run/model")
		Expect(err).NotTo(HaveOccurred())
		Expect(readyModel.Status).To(Equal(model.ModelStatusReady))
		Expect(readyModel.ArtifactLocation).To(Equal("s3://local-dev-bucket/models/run/model"))

		readModel, err := modelsUse.ReadModelSystem(ctx, registeredModel.ModelID)
		Expect(err).NotTo(HaveOccurred())
		Expect(readModel.ModelID).To(Equal(registeredModel.ModelID))

		updatedEvents := modelUpdatedOutboxEvents(ctx, database, registeredModel.ModelID)
		Expect(updatedEvents).To(HaveLen(2))
		Expect(updatedEvents[0].UserId).To(Equal(modelRecord.UserID.String()))
		Expect(updatedEvents[0].Status).To(Equal(model.ModelStatusPending.String()))
		Expect(updatedEvents[1].Status).To(Equal(model.ModelStatusReady.String()))
		Expect(updatedEvents[1].ArtifactLocation).To(Equal("s3://local-dev-bucket/models/run/model"))
	})

	It("reports duplicate idempotency keys and missing models with domain errors", func() {
		idempotencyKey := uuid.New()
		modelRecord := validIntegrationModel()
		Expect(upsertModelRegistryTenant(ctx, database, modelRecord.UserID)).To(Succeed())

		_, err := modelsUse.RegisterModel(ctx, modelRecord, idempotencyKey)
		Expect(err).NotTo(HaveOccurred())

		duplicate := validIntegrationModel()
		duplicate.UserID = modelRecord.UserID
		duplicate.OrgID = modelRecord.OrgID
		_, err = modelsUse.RegisterModel(ctx, duplicate, idempotencyKey)
		Expect(errors.Is(err, domain.ErrModelExists)).To(BeTrue())

		_, err = modelsUse.ReadModelSystem(ctx, uuid.New())
		Expect(errors.Is(err, domain.ErrModelNotFound)).To(BeTrue())
	})

	It("enforces tenant projections and RLS on repository reads", func() {
		tenantA := uuid.New()
		tenantB := uuid.New()
		orgA := uuid.New()
		orgB := uuid.New()
		modelRecord := validIntegrationModel()
		modelRecord.UserID = tenantA
		modelRecord.OrgID = orgA
		Expect(upsertModelRegistryTenant(ctx, database, tenantA)).To(Succeed())
		Expect(upsertModelRegistryTenant(ctx, database, tenantB)).To(Succeed())

		registeredModel, err := modelsUse.RegisterModel(ctx, modelRecord, uuid.New())
		Expect(err).NotTo(HaveOccurred())

		_, err = models.ReadByID(ctxutil.WithActorOrg(ctx, tenantB, orgB), registeredModel.ModelID)
		Expect(errors.Is(err, domain.ErrModelNotFound)).To(BeTrue())

		_, err = models.ReadByID(ctx, registeredModel.ModelID)
		Expect(errors.Is(err, domain.ErrModelNotFound)).To(BeTrue())

		readModel, err := models.ReadByID(ctxutil.WithActorOrg(ctx, tenantA, orgA), registeredModel.ModelID)
		Expect(err).NotTo(HaveOccurred())
		Expect(readModel.ModelID).To(Equal(registeredModel.ModelID))
	})

	It("rejects writes for users that have not been projected into the service database", func() {
		modelRecord := validIntegrationModel()
		Expect(modelRecord.ModelKind).To(Equal(model.ModelKindFineTuned))
		var projected bool
		Expect(database.Pool.QueryRow(ctxutil.WithSystemContext(ctx), `
			SELECT EXISTS (
				SELECT 1 FROM `+database.Name+`.tenants WHERE id = $1
			)
		`, modelRecord.UserID).Scan(&projected)).To(Succeed())
		Expect(projected).To(BeFalse())

		_, err := modelsUse.RegisterModel(ctx, modelRecord, uuid.New())
		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue(), "err: %v", err)
		Expect(outboxMessageCount(ctx, database, uuid.Nil)).To(Equal(0))
	})

	It("records completed training through the listener, lands a candidate, and requests promotion", func() {
		datasetID := uuid.New()
		userID := uuid.New()
		orgID := uuid.New()
		trainingRunID := uuid.New()
		modelID := uuid.New()
		Expect(upsertModelRegistryTenant(ctx, database, userID)).To(Succeed())

		listener := registrymessaging.NewModelTrainingCompletedEventListener(modelsUse)
		Expect(listener.Handle(ctx, datasetID, &trainingpb.ModelTrainingCompletedEvent{
			TrainingRunId:     trainingRunID.String(),
			UserId:            userID.String(),
			OrgId:             orgID.String(),
			DatasetId:         datasetID.String(),
			DatasetVersion:    "7",
			FeatureSnapshotId: uuid.NewString(),
			ModelId:           modelID.String(),
			ModelName:         "movie-ranker",
			ModelVersion:      "7",
			BaseModel:         "mistral-7b",
			ArtifactLocation:  "s3://local-dev-bucket/models/" + trainingRunID.String(),
			ArtifactFormat:    "HF_PEFT_ADAPTER",
			ArtifactChecksum:  "sha256:abc",
			ArtifactSizeBytes: 128,
			AdapterUri:        "s3://local-dev-bucket/models/" + trainingRunID.String(),
			ServingTarget:     "vllm-local",
			ServingModel:      "movie-ranker-v7",
			ServingLoadStatus: "LOADED",
			MetricsMetadata:   integrationMetricsMetadata(0.91, 0.90, 0.89),
			ReportLocation:    "s3://local-dev-bucket/evals/" + trainingRunID.String() + ".json",
		})).To(Succeed())

		modelRecord, err := models.ReadByTrainingRunID(ctxutil.WithActorOrg(ctx, userID, orgID), trainingRunID)
		Expect(err).NotTo(HaveOccurred())
		Expect(modelRecord.UserID).To(Equal(userID))
		Expect(modelRecord.DatasetID).To(Equal(datasetID))
		Expect(modelRecord.Status).To(Equal(model.ModelStatusCandidate))
		Expect(modelRecord.ServingLoadStatus).To(Equal(model.ModelLoadStatusNotLoaded))
		Expect(modelRecord.ModelVersion).To(Equal(7))
		Expect(modelRecord.ArtifactLocation).To(Equal("s3://local-dev-bucket/models/" + trainingRunID.String()))
		Expect(modelRecord.ArtifactFormat).To(Equal("HF_PEFT_ADAPTER"))
		Expect(outboxMessagesByType(ctx, database, modelRecord.ModelID, sharedmessaging.MsgTypeModelUpdated)).To(HaveLen(1))
		promotionRequests := promotionRequestedOutboxEvents(ctx, database, modelRecord.ModelID)
		Expect(promotionRequests).To(HaveLen(1))
		Expect(promotionRequests[0].UserId).To(Equal(userID.String()))
		Expect(promotionRequests[0].OrgId).To(Equal(orgID.String()))
		Expect(promotionRequests[0].ModelId).To(Equal(modelRecord.ModelID.String()))
		Expect(promotionRequests[0].TrainingRunId).To(Equal(trainingRunID.String()))
		Expect(promotionRequests[0].CandidateMetricsMetadata).To(ContainSubstring("faithfulness"))
		Expect(deployer.models).To(BeEmpty())
	})

	It("accepts a positive promotion report, records deltas, and emits a serving intent", func() {
		candidate := createCandidateFromTraining(ctx, database, models, modelsUse, 0.91, 0.90, 0.89)
		promotionListener := registrymessaging.NewPromotionReportReadyEventListener(modelsUse)
		Expect(promotionListener.Handle(ctx, candidate.ModelID, &trainingpb.PromotionReportReadyEvent{
			UserId:             candidate.UserID.String(),
			OrgId:              candidate.OrgID.String(),
			ModelId:            candidate.ModelID.String(),
			TrainingRunId:      candidate.TrainingRunID.String(),
			PromotionReportUri: "s3://local-dev-bucket/promotion/" + candidate.ModelID.String() + ".json",
			PromotionDeltas:    "{}",
		})).To(Succeed())

		promotedModel, err := models.ReadByTrainingRunID(ctxutil.WithActorOrg(ctx, candidate.UserID, candidate.OrgID), candidate.TrainingRunID)
		Expect(err).NotTo(HaveOccurred())
		Expect(promotedModel.Status).To(Equal(model.ModelStatusEvaluated))
		Expect(promotedModel.PromotionReportURI).To(Equal("s3://local-dev-bucket/promotion/" + candidate.ModelID.String() + ".json"))
		Expect(promotedModel.PromotionDecision).To(ContainSubstring(model.PromotionDecisionOutcomeAccepted.String()))
		Expect(promotedModel.FailureReason).To(BeEmpty())
		Expect(outboxMessagesByType(ctx, database, candidate.ModelID, sharedmessaging.MsgTypeModelUpdated)).To(HaveLen(2))
		Expect(deployer.models).To(HaveLen(1))
		Expect(deployer.models[0].ModelID).To(Equal(candidate.ModelID))
	})

	It("rejects a promotion report when candidate metrics regress against the champion", func() {
		userID := uuid.New()
		orgID := uuid.New()
		Expect(upsertModelRegistryTenant(ctx, database, userID)).To(Succeed())
		champion := validIntegrationModel()
		champion.UserID = userID
		champion.OrgID = orgID
		champion.ModelVersion = 1
		champion.Status = model.ModelStatusReady
		champion.ServingLoadStatus = model.ModelLoadStatusLoaded
		champion.ServingProtocol = model.ServingProtocolOpenAIChatCompletions
		champion.MetricsMetadata = integrationMetricsMetadata(0.95, 0.95, 0.95)
		_, err := modelsUse.RegisterModel(ctx, champion, uuid.New())
		Expect(err).NotTo(HaveOccurred())

		candidate := trainingCompletedEvent(userID, orgID, uuid.New(), uuid.New(), uuid.New(), 2, 0.70, 0.70, 0.70)
		listener := registrymessaging.NewModelTrainingCompletedEventListener(modelsUse)
		Expect(listener.Handle(ctx, uuid.MustParse(candidate.DatasetId), candidate)).To(Succeed())
		candidateRecord, err := models.ReadByTrainingRunID(ctxutil.WithActorOrg(ctx, uuid.MustParse(candidate.UserId), uuid.MustParse(candidate.OrgId)), uuid.MustParse(candidate.TrainingRunId))
		Expect(err).NotTo(HaveOccurred())

		promotionListener := registrymessaging.NewPromotionReportReadyEventListener(modelsUse)
		Expect(promotionListener.Handle(ctx, candidateRecord.ModelID, &trainingpb.PromotionReportReadyEvent{
			UserId:             candidateRecord.UserID.String(),
			OrgId:              candidateRecord.OrgID.String(),
			ModelId:            candidateRecord.ModelID.String(),
			TrainingRunId:      candidateRecord.TrainingRunID.String(),
			PromotionReportUri: "s3://local-dev-bucket/promotion/rejected.json",
			PromotionDeltas:    "{}",
		})).To(Succeed())

		rejectedModel, err := models.ReadByTrainingRunID(ctxutil.WithActorOrg(ctx, candidateRecord.UserID, candidateRecord.OrgID), candidateRecord.TrainingRunID)
		Expect(err).NotTo(HaveOccurred())
		Expect(rejectedModel.Status).To(Equal(model.ModelStatusFailed))
		Expect(rejectedModel.PromotionDecision).To(ContainSubstring(model.PromotionDecisionOutcomeRejected.String()))
		Expect(rejectedModel.FailureReason).To(ContainSubstring("candidate metric faithfulness regressed"))
		Expect(deployer.models).To(BeEmpty())
	})

	It("rejects malformed promotion reports without creating outbox messages", func() {
		candidate := createCandidateFromTraining(ctx, database, models, modelsUse, 0.91, 0.90, 0.89)
		before := outboxMessageCount(ctx, database, candidate.ModelID)
		listener := registrymessaging.NewPromotionReportReadyEventListener(modelsUse)

		err := listener.Handle(ctx, uuid.New(), &trainingpb.PromotionReportReadyEvent{
			UserId:             candidate.UserID.String(),
			ModelId:            candidate.ModelID.String(),
			TrainingRunId:      candidate.TrainingRunID.String(),
			PromotionReportUri: "s3://local-dev-bucket/promotion/mismatch.json",
		})

		Expect(err).To(HaveOccurred())
		Expect(sharedmessaging.IsNonRetryable(err)).To(BeTrue())
		Expect(outboxMessageCount(ctx, database, candidate.ModelID)).To(Equal(before))
	})

	It("records failed training events and rejects invalid failed-training payloads", func() {
		datasetID := uuid.New()
		userID := uuid.New()
		orgID := uuid.New()
		trainingRunID := uuid.New()
		Expect(upsertModelRegistryTenant(ctx, database, userID)).To(Succeed())
		listener := registrymessaging.NewModelTrainingFailedEventListener(modelsUse)

		Expect(listener.Handle(ctx, datasetID, &trainingpb.ModelTrainingFailedEvent{
			TrainingRunId:  trainingRunID.String(),
			UserId:         userID.String(),
			OrgId:          orgID.String(),
			DatasetId:      datasetID.String(),
			DatasetVersion: "3",
			ModelId:        uuid.NewString(),
			ModelName:      "movie-ranker",
			ModelVersion:   "dataset-v3",
			BaseModel:      "mistral-7b",
			FailureReason:  "training failed",
		})).To(Succeed())

		failedModel, err := models.ReadByTrainingRunID(ctxutil.WithActorOrg(ctx, userID, orgID), trainingRunID)
		Expect(err).NotTo(HaveOccurred())
		Expect(failedModel.Status).To(Equal(model.ModelStatusFailed))
		Expect(failedModel.FailureReason).To(Equal("training failed"))
		Expect(modelUpdatedOutboxEvents(ctx, database, failedModel.ModelID)).To(HaveLen(1))

		err = listener.Handle(ctx, datasetID, &trainingpb.ModelTrainingFailedEvent{
			TrainingRunId: trainingRunID.String(),
			UserId:        userID.String(),
			OrgId:         orgID.String(),
			DatasetId:     datasetID.String(),
			ModelId:       uuid.NewString(),
			ModelName:     "movie-ranker",
			ModelVersion:  "dataset-v4",
			BaseModel:     "mistral-7b",
		})
		Expect(err).To(HaveOccurred())
		Expect(sharedmessaging.IsNonRetryable(err)).To(BeTrue())
	})

	It("registers uploaded base model artifacts and emits a serving intent", func() {
		artifactID := uuid.New()
		uploadID := uuid.New()
		userID := uuid.New()
		orgID := uuid.New()
		datasetID := uuid.New()
		Expect(upsertModelRegistryTenant(ctx, database, userID)).To(Succeed())
		listener := registrymessaging.NewModelArtifactIngestedEventListener(modelsUse)

		Expect(listener.Handle(ctx, artifactID, &ingestionpb.ModelArtifactIngestedEvent{
			ArtifactId:        artifactID.String(),
			UploadId:          uploadID.String(),
			UserId:            userID.String(),
			OrgId:             orgID.String(),
			DatasetId:         datasetID.String(),
			Source:            "upload",
			StorageLocation:   "s3://local-dev-bucket/models/base",
			ArtifactType:      "BASE_MODEL",
			ArtifactFormat:    "HF_MODEL",
			ArtifactSizeBytes: 2048,
			ArtifactChecksum:  "sha256:base",
			ModelName:         "llama-base",
			ModelVersion:      "1",
			BaseModel:         "meta-llama/Llama-3.1-8B-Instruct",
		})).To(Succeed())

		baseModel, err := models.ReadByID(ctxutil.WithSystemContext(ctx), artifactID)
		Expect(err).NotTo(HaveOccurred())
		Expect(baseModel.UserID).To(Equal(userID))
		Expect(baseModel.OrgID).To(Equal(orgID))
		Expect(baseModel.DatasetID).To(Equal(datasetID))
		Expect(baseModel.ModelKind).To(Equal(model.ModelKindBase))
		Expect(baseModel.Status).To(Equal(model.ModelStatusEvaluated))
		Expect(baseModel.Source).To(Equal(model.ModelSourceUpload))
		Expect(modelUpdatedOutboxEvents(ctx, database, artifactID)).To(HaveLen(1))
		Expect(deployer.models).To(HaveLen(1))
		Expect(deployer.models[0].ModelID).To(Equal(artifactID))
	})

	It("registers uploaded fine-tuned artifacts for a tenant and rejects malformed artifact events", func() {
		artifactID := uuid.New()
		uploadID := uuid.New()
		userID := uuid.New()
		orgID := uuid.New()
		datasetID := uuid.New()
		Expect(upsertModelRegistryTenant(ctx, database, userID)).To(Succeed())
		listener := registrymessaging.NewModelArtifactIngestedEventListener(modelsUse)

		Expect(listener.Handle(ctx, artifactID, &ingestionpb.ModelArtifactIngestedEvent{
			ArtifactId:        artifactID.String(),
			UploadId:          uploadID.String(),
			UserId:            userID.String(),
			OrgId:             orgID.String(),
			DatasetId:         datasetID.String(),
			Source:            "upload",
			StorageLocation:   "s3://local-dev-bucket/models/adapter",
			ArtifactType:      "LORA_ADAPTER",
			ArtifactFormat:    "HF_PEFT_ADAPTER",
			ArtifactSizeBytes: 1024,
			ArtifactChecksum:  "sha256:adapter",
			ModelName:         "movie-adapter",
			ModelVersion:      "2",
			BaseModel:         "mistral-7b",
		})).To(Succeed())

		adapterModel, err := models.ReadByID(ctxutil.WithActorOrg(ctx, userID, orgID), artifactID)
		Expect(err).NotTo(HaveOccurred())
		Expect(adapterModel.ModelKind).To(Equal(model.ModelKindFineTuned))
		Expect(adapterModel.DatasetID).To(Equal(datasetID))
		Expect(adapterModel.AdapterURI).To(Equal("s3://local-dev-bucket/models/adapter"))

		err = listener.Handle(ctx, uuid.New(), &ingestionpb.ModelArtifactIngestedEvent{
			ArtifactId:        uuid.NewString(),
			UploadId:          uuid.NewString(),
			Source:            "upload",
			StorageLocation:   "s3://local-dev-bucket/models/bad",
			ArtifactType:      "LORA_ADAPTER",
			ArtifactFormat:    "HF_PEFT_ADAPTER",
			ArtifactSizeBytes: 1024,
			ArtifactChecksum:  "sha256:bad",
			ModelName:         "bad-adapter",
			ModelVersion:      "2",
			BaseModel:         "mistral-7b",
		})
		Expect(err).To(HaveOccurred())
		Expect(sharedmessaging.IsNonRetryable(err)).To(BeTrue())
	})

	It("records serving status transitions and suppresses duplicate status outbox messages", func() {
		modelRecord := validIntegrationModel()
		Expect(upsertModelRegistryTenant(ctx, database, modelRecord.UserID)).To(Succeed())
		registeredModel, err := modelsUse.RegisterModel(ctx, modelRecord, uuid.New())
		Expect(err).NotTo(HaveOccurred())
		before := outboxMessageCount(ctx, database, registeredModel.ModelID)

		statusID := uuid.New()
		readyModel, err := modelsUse.RecordModelServingStatus(ctx, &model.ServedModelStatus{
			ModelID:           registeredModel.ModelID,
			ServingTarget:     "vllm",
			ServingModel:      "movie-ranker-v1",
			ServingProtocol:   model.ServingProtocolOpenAIChatCompletions,
			ServingLoadStatus: model.ModelLoadStatusLoaded,
		}, statusID)
		Expect(err).NotTo(HaveOccurred())
		Expect(readyModel.Status).To(Equal(model.ModelStatusReady))
		Expect(readyModel.ServingLoadStatus).To(Equal(model.ModelLoadStatusLoaded))
		Expect(outboxMessageCount(ctx, database, registeredModel.ModelID)).To(Equal(before + 1))

		unchanged, err := modelsUse.RecordModelServingStatus(ctx, &model.ServedModelStatus{
			ModelID:           registeredModel.ModelID,
			ServingTarget:     "vllm",
			ServingModel:      "movie-ranker-v1",
			ServingProtocol:   model.ServingProtocolOpenAIChatCompletions,
			ServingLoadStatus: model.ModelLoadStatusLoaded,
		}, statusID)
		Expect(err).NotTo(HaveOccurred())
		Expect(unchanged.ModelID).To(Equal(registeredModel.ModelID))
		Expect(outboxMessageCount(ctx, database, registeredModel.ModelID)).To(Equal(before + 1))

		failedModel, err := modelsUse.RecordModelServingStatus(ctx, &model.ServedModelStatus{
			ModelID:           registeredModel.ModelID,
			ServingTarget:     "vllm",
			ServingModel:      "movie-ranker-v1",
			ServingLoadStatus: model.ModelLoadStatusFailed,
			FailureReason:     "load failed",
		}, uuid.New())
		Expect(err).NotTo(HaveOccurred())
		Expect(failedModel.Status).To(Equal(model.ModelStatusFailed))
		Expect(failedModel.FailureReason).To(Equal("load failed"))
	})

	It("returns domain errors for serving status on unknown models", func() {
		_, err := modelsUse.RecordModelServingStatus(ctx, &model.ServedModelStatus{
			ModelID:           uuid.New(),
			ServingLoadStatus: model.ModelLoadStatusLoaded,
		}, uuid.New())
		Expect(errors.Is(err, domain.ErrModelNotFound)).To(BeTrue())
	})
})

func validIntegrationModel() *model.Model {
	return &model.Model{
		ModelID:           uuid.New(),
		UserID:            uuid.New(),
		OrgID:             uuid.New(),
		TrainingRunID:     uuid.New(),
		DatasetID:         uuid.New(),
		ModelKind:         model.ModelKindFineTuned,
		Source:            model.ModelSourceTraining,
		Name:              "movie-ranker",
		ModelVersion:      1,
		BaseModel:         "mistral-7b",
		ArtifactLocation:  "s3://local-dev-bucket/models/pending",
		ArtifactFormat:    "HF_PEFT_ADAPTER",
		ArtifactChecksum:  "sha256:pending",
		ArtifactSizeBytes: 1,
		AdapterURI:        "s3://local-dev-bucket/models/pending",
		ServingTarget:     "vllm-local",
		ServingModel:      "movie-ranker-v1",
		MetricsMetadata:   integrationMetricsMetadata(0.91, 0.90, 0.89),
	}
}

func trainingCompletedEvent(userID, orgID, datasetID, trainingRunID, modelID uuid.UUID, version int, faithfulness, answerRelevancy, contextPrecision float64) *trainingpb.ModelTrainingCompletedEvent {
	return &trainingpb.ModelTrainingCompletedEvent{
		TrainingRunId:     trainingRunID.String(),
		UserId:            userID.String(),
		OrgId:             orgID.String(),
		DatasetId:         datasetID.String(),
		DatasetVersion:    fmt.Sprintf("%d", version),
		FeatureSnapshotId: uuid.NewString(),
		ModelId:           modelID.String(),
		ModelName:         "movie-ranker",
		ModelVersion:      fmt.Sprintf("%d", version),
		BaseModel:         "mistral-7b",
		ArtifactLocation:  "s3://local-dev-bucket/models/" + trainingRunID.String(),
		ArtifactFormat:    "HF_PEFT_ADAPTER",
		ArtifactChecksum:  "sha256:abc",
		ArtifactSizeBytes: 128,
		AdapterUri:        "s3://local-dev-bucket/models/" + trainingRunID.String(),
		ServingTarget:     "vllm-local",
		ServingModel:      fmt.Sprintf("movie-ranker-v%d", version),
		ServingLoadStatus: model.ModelLoadStatusLoaded.String(),
		MetricsMetadata:   integrationMetricsMetadata(faithfulness, answerRelevancy, contextPrecision),
		ReportLocation:    "s3://local-dev-bucket/evals/" + trainingRunID.String() + ".json",
	}
}

func createCandidateFromTraining(ctx context.Context, database *dbconn.Database, modelRepository app.ModelRepository, usecase app.ModelRegistryUsecase, faithfulness, answerRelevancy, contextPrecision float64) *model.Model {
	datasetID := uuid.New()
	userID := uuid.New()
	orgID := uuid.New()
	trainingRunID := uuid.New()
	modelID := uuid.New()
	Expect(upsertModelRegistryTenant(ctx, database, userID)).To(Succeed())

	listener := registrymessaging.NewModelTrainingCompletedEventListener(usecase)
	Expect(listener.Handle(ctx, datasetID, trainingCompletedEvent(userID, orgID, datasetID, trainingRunID, modelID, 1, faithfulness, answerRelevancy, contextPrecision))).To(Succeed())

	candidate, err := modelRepository.ReadByTrainingRunID(ctxutil.WithActorOrg(ctx, userID, orgID), trainingRunID)
	Expect(err).NotTo(HaveOccurred())
	return candidate
}

func integrationMetricsMetadata(faithfulness float64, answerRelevancy float64, contextPrecision float64) string {
	return fmt.Sprintf(`{"passed":true,"metrics":{"faithfulness":%.2f,"answer_relevancy":%.2f,"context_precision":%.2f},"thresholds":{"faithfulness":0.8,"answer_relevancy":0.8,"context_precision":0.8},"report_uri":"s3://local-dev-bucket/evaluations/run.json","evaluator_name":"ragas","evaluator_version":"ragas-v1","metric_suite":"rag","eval_dataset_uri":"s3://evals/held-out.jsonl","eval_dataset_mode":"labeled"}`, faithfulness, answerRelevancy, contextPrecision)
}

func truncateModelRegistry(ctx context.Context, database *dbconn.Database) error {
	ctx = ctxutil.WithSystemContext(ctx)
	for _, table := range []string{"outbox_messages", "models", "tenants"} {
		if _, err := database.Pool.Exec(ctx, "DELETE FROM "+database.Name+"."+table); err != nil {
			return err
		}
	}
	return nil
}

func upsertModelRegistryTenant(ctx context.Context, database *dbconn.Database, userID uuid.UUID) error {
	ctx = ctxutil.WithSystemContext(ctx)
	_, err := database.Pool.Exec(ctx, `
		INSERT INTO `+database.Name+`.tenants (id, email, deleted)
		VALUES ($1, $2, false)
		ON CONFLICT (id) DO UPDATE SET email = EXCLUDED.email, deleted = false
	`, userID, userID.String()+"@example.test")
	return err
}

func testPostgresConnectionString(dbName string) string {
	user := env.WithDefaultString("MODEL_REGISTRY_SERVICE_DB_USER", "bighill_model_registry_db_user")
	password := env.WithDefaultString("MODEL_REGISTRY_SERVICE_DB_PASSWORD", env.WithDefaultString("BIGHILL_DB_PASSWORD", "LrDwb53E7DmFc2j4qw77n4pUUfKtULDVh4vrHjWw"))
	host := env.WithDefaultString("MODEL_REGISTRY_SERVICE_DB_HOST", env.WithDefaultString("PGHOST", "127.0.0.1"))
	if host == "" || host == "/private/tmp" {
		host = "127.0.0.1"
	}
	port := env.WithDefaultString("MODEL_REGISTRY_SERVICE_DB_PORT", env.WithDefaultString("PGPORT", "5432"))
	sslMode := env.WithDefaultString("MODEL_REGISTRY_SERVICE_DB_SSLMODE", env.WithDefaultString("PGSSLMODE", "disable"))
	maxConnections := env.WithDefaultInt("MODEL_REGISTRY_SERVICE_DB_MAX_CONNECTIONS", "20")
	if value := os.Getenv("MODEL_REGISTRY_SERVICE_DB_NAME"); value != "" {
		dbName = value
	}

	q := url.Values{}
	q.Set("sslmode", sslMode)
	q.Set("pool_max_conns", strconv.Itoa(maxConnections))
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?%s", url.QueryEscape(user), url.QueryEscape(password), host, port, dbName, q.Encode())
}

func outboxMessageCount(ctx context.Context, database *dbconn.Database, modelID uuid.UUID) int {
	ctx = ctxutil.WithSystemContext(ctx)
	var count int
	query := `
		SELECT COUNT(*)
		FROM ` + database.Name + `.outbox_messages
	`
	var err error
	if modelID == uuid.Nil {
		err = database.Pool.QueryRow(ctx, query).Scan(&count)
	} else {
		err = database.Pool.QueryRow(ctx, query+" WHERE resource_key = $1", modelID).Scan(&count)
	}
	Expect(err).NotTo(HaveOccurred())
	return count
}

func outboxMessagesByType(ctx context.Context, database *dbconn.Database, modelID uuid.UUID, msgType sharedmessaging.MsgType) []sharedmessaging.Message {
	ctx = ctxutil.WithSystemContext(ctx)
	rows, err := database.Pool.Query(ctx, `
		SELECT payload
		FROM `+database.Name+`.outbox_messages
		WHERE resource_key = $1 AND event_type = $2
		ORDER BY created_at, outbox_id
	`, modelID, msgType.String())
	Expect(err).NotTo(HaveOccurred())
	defer rows.Close()

	messages := []sharedmessaging.Message{}
	for rows.Next() {
		var payload []byte
		Expect(rows.Scan(&payload)).To(Succeed())
		var envelope sharedmessaging.Message
		Expect(envelope.Deserialize(ctx, payload)).To(Succeed())
		Expect(envelope.ResourceKey).To(Equal(modelID))
		Expect(envelope.MsgType).To(Equal(msgType))
		messages = append(messages, envelope)
	}
	Expect(rows.Err()).NotTo(HaveOccurred())
	return messages
}

func modelUpdatedOutboxEvents(ctx context.Context, database *dbconn.Database, modelID uuid.UUID) []*modelregistrypb.ModelUpdatedEvent {
	messages := outboxMessagesByType(ctx, database, modelID, sharedmessaging.MsgTypeModelUpdated)
	events := make([]*modelregistrypb.ModelUpdatedEvent, 0, len(messages))
	for _, message := range messages {
		event := &modelregistrypb.ModelUpdatedEvent{}
		Expect(message.DeserializePayload(event)).To(Succeed())
		events = append(events, event)
	}
	return events
}

func promotionRequestedOutboxEvents(ctx context.Context, database *dbconn.Database, modelID uuid.UUID) []*modelregistrypb.PromotionRequestedEvent {
	messages := outboxMessagesByType(ctx, database, modelID, sharedmessaging.MsgTypePromotionRequested)
	events := make([]*modelregistrypb.PromotionRequestedEvent, 0, len(messages))
	for _, message := range messages {
		event := &modelregistrypb.PromotionRequestedEvent{}
		Expect(message.DeserializePayload(event)).To(Succeed())
		events = append(events, event)
	}
	return events
}

type recordingServingDeployer struct {
	models []*model.Model
	err    error
}

func (d *recordingServingDeployer) EnsureServedModel(_ context.Context, modelRecord *model.Model) error {
	if d.err != nil {
		return d.err
	}
	copy := *modelRecord
	d.models = append(d.models, &copy)
	return nil
}

func (d *recordingServingDeployer) reset() {
	d.models = nil
	d.err = nil
}
