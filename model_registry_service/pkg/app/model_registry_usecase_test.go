package app_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"model_registry_service/pkg/app"
	"model_registry_service/pkg/domain"
	"model_registry_service/pkg/domain/model"
	registrymessaging "model_registry_service/pkg/infra/network/messaging"

	"lib/shared_lib/ctxutil"
	msgConn "lib/shared_lib/messaging"
	transport "lib/shared_lib/transport"
	shareduow "lib/shared_lib/uow"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestApp(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Model registry app unit test suite")
}

type modelRepositoryStub struct {
	createdModel       *model.Model
	createCtx          context.Context
	readModel          *model.Model
	champion           *model.Model
	status             model.ModelStatus
	loadStatus         model.ModelLoadStatus
	failure            string
	promotionReportURI string
	promotionDeltas    string
	promotionDecision  string
	servingKey         uuid.UUID
	createErr          error
	readErr            error
	championErr        error
	updateErr          error
}

func (s *modelRepositoryStub) Close() {}

func (s *modelRepositoryStub) Create(ctx context.Context, _ pgx.Tx, registeredModel *model.Model, _ uuid.UUID) (*model.Model, error) {
	s.createCtx = ctx
	s.createdModel = registeredModel
	return registeredModel, s.createErr
}

func (s *modelRepositoryStub) ReadByID(context.Context, uuid.UUID) (*model.Model, error) {
	return s.readModel, s.readErr
}

func (s *modelRepositoryStub) ReadByTrainingRunID(context.Context, uuid.UUID) (*model.Model, error) {
	return s.readModel, s.readErr
}

func (s *modelRepositoryStub) ReadChampion(context.Context, model.Lineage) (*model.Model, error) {
	return s.champion, s.championErr
}

func (s *modelRepositoryStub) List(context.Context, transport.Pagination, model.ListFilter) ([]*model.Model, int, error) {
	if s.readModel == nil {
		return nil, 0, s.readErr
	}
	return []*model.Model{s.readModel}, 1, s.readErr
}

func (s *modelRepositoryStub) UpdateStatus(_ context.Context, _ pgx.Tx, _ uuid.UUID, status model.ModelStatus, _ string, failureReason string) (*model.Model, error) {
	s.status = status
	s.failure = failureReason
	if s.readModel != nil {
		s.readModel.Status = status
		s.readModel.FailureReason = failureReason
	}
	return s.readModel, s.updateErr
}

func (s *modelRepositoryStub) UpdateServingStatus(_ context.Context, _ pgx.Tx, _ uuid.UUID, status model.ModelStatus, loadStatus model.ModelLoadStatus, _ string, _ string, _ model.ServingProtocol, _ string, idempotencyKey uuid.UUID) (*model.Model, bool, error) {
	s.status = status
	s.loadStatus = loadStatus
	s.servingKey = idempotencyKey
	if s.readModel != nil {
		s.readModel.Status = status
		s.readModel.ServingLoadStatus = loadStatus
	}
	return s.readModel, true, s.updateErr
}

func (s *modelRepositoryStub) UpdatePromotionDecision(_ context.Context, _ pgx.Tx, _ uuid.UUID, status model.ModelStatus, promotionReportURI string, promotionDeltas string, promotionDecision string, failureReason string) (*model.Model, error) {
	s.status = status
	s.promotionReportURI = promotionReportURI
	s.promotionDeltas = promotionDeltas
	s.promotionDecision = promotionDecision
	s.failure = failureReason
	if s.readModel != nil {
		s.readModel.Status = status
		s.readModel.PromotionReportURI = promotionReportURI
		s.readModel.PromotionDeltas = promotionDeltas
		s.readModel.PromotionDecision = promotionDecision
		s.readModel.FailureReason = failureReason
	}
	return s.readModel, s.updateErr
}

type modelUnitOfWorkStub struct {
	messages []msgConn.OutboundMessage
	err      error
}

func (s *modelUnitOfWorkStub) Do(ctx context.Context, fn shareduow.TxFunc) error {
	if s.err != nil {
		return s.err
	}
	return fn(ctx, nil, func(msg msgConn.OutboundMessage) error {
		s.messages = append(s.messages, msg)
		return nil
	})
}

type modelServingDeployerStub struct {
	servedModel *model.Model
	err         error
}

type publishedEndpointRepositoryStub struct {
	endpoint *model.PublishedEndpoint
	err      error
}

func (s *publishedEndpointRepositoryStub) UpsertEndpoint(_ context.Context, _ pgx.Tx, endpoint *model.PublishedEndpoint) error {
	s.endpoint = endpoint
	return s.err
}

func modelEventBuilder() app.ModelEventBuilder {
	return registrymessaging.NewModelEventBuilder("model_registry")
}

func (s *modelServingDeployerStub) EnsureServedModel(_ context.Context, registeredModel *model.Model) error {
	s.servedModel = registeredModel
	return s.err
}

var _ = Describe("ModelRegistryUsecase", func() {
	It("registers a model through the repository", func() {
		repo := &modelRepositoryStub{}
		uow := &modelUnitOfWorkStub{}
		uc := app.NewModelRegistryUsecase(repo, uow, modelEventBuilder())
		registeredModel := validModel()

		result, err := uc.RegisterModel(context.Background(), registeredModel, uuid.New())

		Expect(err).NotTo(HaveOccurred())
		Expect(result.ModelID).NotTo(Equal(uuid.Nil))
		Expect(repo.createdModel).To(Equal(registeredModel))
		Expect(uow.messages).To(HaveLen(1))
	})

	It("does not grant system context when a model record is missing actor and org", func() {
		repo := &modelRepositoryStub{}
		uow := &modelUnitOfWorkStub{}
		uc := app.NewModelRegistryUsecase(repo, uow, modelEventBuilder())
		registeredModel := validModel()
		registeredModel.UserID = uuid.Nil
		registeredModel.OrgID = uuid.Nil

		result, err := uc.RegisterModel(context.Background(), registeredModel, uuid.New())

		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(registeredModel))
		Expect(ctxutil.IsSystemContext(repo.createCtx)).To(BeFalse())
		_, hasOrg := ctxutil.OrgID(repo.createCtx)
		Expect(hasOrg).To(BeFalse())
	})

	It("marks a model ready", func() {
		repo := &modelRepositoryStub{readModel: validModel()}
		uc := app.NewModelRegistryUsecase(repo, &modelUnitOfWorkStub{}, modelEventBuilder())

		result, err := uc.MarkModelReady(context.Background(), uuid.New(), "s3://models/run/model")

		Expect(err).NotTo(HaveOccurred())
		Expect(repo.status).To(Equal(model.ModelStatusReady))
		Expect(result).NotTo(BeNil())
	})

	It("marks a model failed", func() {
		repo := &modelRepositoryStub{readModel: validModel()}
		uc := app.NewModelRegistryUsecase(repo, &modelUnitOfWorkStub{}, modelEventBuilder())

		_, err := uc.MarkModelFailed(context.Background(), uuid.New(), "training failed")

		Expect(err).NotTo(HaveOccurred())
		Expect(repo.status).To(Equal(model.ModelStatusFailed))
	})

	It("records completed training as a candidate and does not deploy it", func() {
		repo := &modelRepositoryStub{}
		deployer := &modelServingDeployerStub{}
		uc := app.NewModelRegistryUsecase(repo, &modelUnitOfWorkStub{}, modelEventBuilder(), app.WithModelServingDeployer(deployer))
		trainedModel := validModel()
		trainedModel.ArtifactLocation = "s3://models/run/model"

		result, err := uc.RecordModelTrainingCompleted(context.Background(), trainedModel, uuid.New())

		Expect(err).NotTo(HaveOccurred())
		Expect(result.Status).To(Equal(model.ModelStatusCandidate))
		Expect(result.ServingLoadStatus).To(Equal(model.ModelLoadStatusNotLoaded))
		Expect(repo.createdModel.ArtifactLocation).To(Equal("s3://models/run/model"))
		Expect(deployer.servedModel).To(BeNil())
	})

	It("does not let a completed-training event mark the model ready", func() {
		repo := &modelRepositoryStub{}
		deployer := &modelServingDeployerStub{}
		uc := app.NewModelRegistryUsecase(repo, &modelUnitOfWorkStub{}, modelEventBuilder(), app.WithModelServingDeployer(deployer))
		trainedModel := validModel()
		trainedModel.ServingLoadStatus = model.ModelLoadStatusLoaded

		result, err := uc.RecordModelTrainingCompleted(context.Background(), trainedModel, uuid.New())

		Expect(err).NotTo(HaveOccurred())
		Expect(result.Status).To(Equal(model.ModelStatusCandidate))
		Expect(result.ServingLoadStatus).To(Equal(model.ModelLoadStatusNotLoaded))
		Expect(deployer.servedModel).To(BeNil())
	})

	It("returns existing training-run records on replay", func() {
		existing := validModel()
		repo := &modelRepositoryStub{readModel: existing, createErr: domain.ErrModelExists}
		deployer := &modelServingDeployerStub{}
		uc := app.NewModelRegistryUsecase(repo, &modelUnitOfWorkStub{}, modelEventBuilder(), app.WithModelServingDeployer(deployer))
		trainedModel := validModel()
		trainedModel.ArtifactLocation = "s3://models/run/model"

		result, err := uc.RecordModelTrainingCompleted(context.Background(), trainedModel, uuid.New())

		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(existing))
		Expect(deployer.servedModel).To(BeNil())
	})

	It("promotes a first candidate that passes the gate and deploys it", func() {
		candidate := validModel()
		candidate.Status = model.ModelStatusCandidate
		candidate.ServingLoadStatus = model.ModelLoadStatusNotLoaded
		repo := &modelRepositoryStub{readModel: candidate}
		deployer := &modelServingDeployerStub{}
		uc := app.NewModelRegistryUsecase(repo, &modelUnitOfWorkStub{}, modelEventBuilder(), app.WithModelServingDeployer(deployer))

		result, err := uc.PromoteCandidate(context.Background(), candidate.ModelID)

		Expect(err).NotTo(HaveOccurred())
		Expect(result.Status).To(Equal(model.ModelStatusEvaluated))
		Expect(result.PromotionDecision).To(ContainSubstring("PROMOTION_ACCEPTED"))
		Expect(result.FailureReason).To(BeEmpty())
		Expect(repo.status).To(Equal(model.ModelStatusEvaluated))
		Expect(deployer.servedModel).To(Equal(result))
	})

	It("records a promotion report and deploys a first candidate only after the gate passes", func() {
		candidate := validModel()
		candidate.Status = model.ModelStatusCandidate
		candidate.ServingLoadStatus = model.ModelLoadStatusNotLoaded
		repo := &modelRepositoryStub{readModel: candidate}
		deployer := &modelServingDeployerStub{}
		uc := app.NewModelRegistryUsecase(repo, &modelUnitOfWorkStub{}, modelEventBuilder(), app.WithModelServingDeployer(deployer))

		result, err := uc.RecordPromotionReportReady(context.Background(), model.PromotionReportResult{
			UserID:             candidate.UserID,
			ModelID:            candidate.ModelID,
			TrainingRunID:      candidate.TrainingRunID,
			PromotionReportURI: "s3://local-dev-bucket/promotion/model.json",
			Deltas:             map[string]float64{"faithfulness": 0.1},
		}, uuid.New())

		Expect(err).NotTo(HaveOccurred())
		Expect(result.Status).To(Equal(model.ModelStatusEvaluated))
		Expect(repo.promotionReportURI).To(Equal("s3://local-dev-bucket/promotion/model.json"))
		Expect(repo.promotionDeltas).To(MatchJSON(`{}`))
		Expect(repo.promotionDecision).To(ContainSubstring("PROMOTION_ACCEPTED"))
		Expect(result.FailureReason).To(BeEmpty())
		Expect(deployer.servedModel).To(Equal(result))
	})

	It("records Go-computed promotion deltas for a champion comparison", func() {
		candidate := validModel()
		candidate.Status = model.ModelStatusCandidate
		candidate.ServingLoadStatus = model.ModelLoadStatusNotLoaded
		candidate.MetricsMetadata = metricsMetadata(0.84, 0.83, 0.82)
		champion := validModel()
		champion.ModelID = uuid.New()
		champion.Status = model.ModelStatusReady
		champion.ServingLoadStatus = model.ModelLoadStatusLoaded
		champion.MetricsMetadata = metricsMetadata(0.80, 0.82, 0.81)
		repo := &modelRepositoryStub{readModel: candidate, champion: champion}
		deployer := &modelServingDeployerStub{}
		uc := app.NewModelRegistryUsecase(repo, &modelUnitOfWorkStub{}, modelEventBuilder(), app.WithModelServingDeployer(deployer))

		result, err := uc.RecordPromotionReportReady(context.Background(), model.PromotionReportResult{
			UserID:             candidate.UserID,
			ModelID:            candidate.ModelID,
			TrainingRunID:      candidate.TrainingRunID,
			PromotionReportURI: "s3://local-dev-bucket/promotion/model.json",
			Deltas:             map[string]float64{"faithfulness": -100},
		}, uuid.New())

		Expect(err).NotTo(HaveOccurred())
		Expect(result.Status).To(Equal(model.ModelStatusEvaluated))
		var deltas map[string]float64
		Expect(json.Unmarshal([]byte(repo.promotionDeltas), &deltas)).To(Succeed())
		Expect(deltas).To(HaveKeyWithValue("faithfulness", BeNumerically("~", 0.04, 0.0001)))
		Expect(deltas).To(HaveKeyWithValue("answer_relevancy", BeNumerically("~", 0.01, 0.0001)))
		Expect(deltas).To(HaveKeyWithValue("context_precision", BeNumerically("~", 0.01, 0.0001)))
		Expect(deployer.servedModel).To(Equal(result))
	})

	It("records a failed promotion report without deploying the candidate", func() {
		candidate := validModel()
		candidate.Status = model.ModelStatusCandidate
		repo := &modelRepositoryStub{readModel: candidate}
		deployer := &modelServingDeployerStub{}
		uc := app.NewModelRegistryUsecase(repo, &modelUnitOfWorkStub{}, modelEventBuilder(), app.WithModelServingDeployer(deployer))

		result, err := uc.RecordPromotionReportReady(context.Background(), model.PromotionReportResult{
			UserID:             candidate.UserID,
			ModelID:            candidate.ModelID,
			TrainingRunID:      candidate.TrainingRunID,
			PromotionReportURI: "s3://local-dev-bucket/promotion/model.json",
			FailureReason:      "evidently evidence requires champion and candidate score_rows_uri",
		}, uuid.New())

		Expect(err).NotTo(HaveOccurred())
		Expect(result.Status).To(Equal(model.ModelStatusFailed))
		Expect(result.PromotionDecision).To(ContainSubstring("PROMOTION_REJECTED"))
		Expect(result.FailureReason).To(Equal("evidently evidence requires champion and candidate score_rows_uri"))
		Expect(deployer.servedModel).To(BeNil())
	})

	It("promotes a candidate when required evidence is present in metrics metadata", func() {
		candidate := validModel()
		candidate.Status = model.ModelStatusCandidate
		candidate.ServingLoadStatus = model.ModelLoadStatusNotLoaded
		candidate.MetricsMetadata = evidenceMetricsMetadata()
		repo := &modelRepositoryStub{readModel: candidate}
		deployer := &modelServingDeployerStub{}
		policy := model.DefaultGatePolicy()
		policy.RequireDeepchecks = true
		policy.RequireEvidently = true
		uc := app.NewModelRegistryUsecase(repo, &modelUnitOfWorkStub{}, modelEventBuilder(), app.WithModelServingDeployer(deployer), app.WithPromotionGatePolicy(policy))

		result, err := uc.PromoteCandidate(context.Background(), candidate.ModelID)

		Expect(err).NotTo(HaveOccurred())
		Expect(result.Status).To(Equal(model.ModelStatusEvaluated))
		Expect(result.PromotionDecision).To(ContainSubstring("PROMOTION_ACCEPTED"))
		Expect(result.FailureReason).To(BeEmpty())
		Expect(deployer.servedModel).To(Equal(result))
	})

	It("rejects a candidate that regresses against the champion", func() {
		candidate := validModel()
		candidate.Status = model.ModelStatusCandidate
		candidate.MetricsMetadata = metricsMetadata(0.70, 0.82, 0.81)
		champion := validModel()
		champion.ModelID = uuid.New()
		champion.Status = model.ModelStatusReady
		champion.ServingLoadStatus = model.ModelLoadStatusLoaded
		champion.MetricsMetadata = metricsMetadata(0.90, 0.82, 0.81)
		repo := &modelRepositoryStub{readModel: candidate, champion: champion}
		deployer := &modelServingDeployerStub{}
		uc := app.NewModelRegistryUsecase(repo, &modelUnitOfWorkStub{}, modelEventBuilder(), app.WithModelServingDeployer(deployer))

		result, err := uc.PromoteCandidate(context.Background(), candidate.ModelID)

		Expect(err).NotTo(HaveOccurred())
		Expect(result.Status).To(Equal(model.ModelStatusFailed))
		Expect(result.PromotionDecision).To(ContainSubstring("PROMOTION_REJECTED"))
		Expect(result.FailureReason).To(ContainSubstring("candidate metric faithfulness regressed"))
		Expect(deployer.servedModel).To(BeNil())
	})

	It("records serving loaded as a ready model", func() {
		modelRecord := validModel()
		repo := &modelRepositoryStub{readModel: modelRecord}
		uc := app.NewModelRegistryUsecase(repo, &modelUnitOfWorkStub{}, modelEventBuilder())
		idempotencyKey := uuid.New()

		result, err := uc.RecordModelServingStatus(context.Background(), &model.ServedModelStatus{
			ModelID:           modelRecord.ModelID,
			ServingTarget:     "vllm-local",
			ServingModel:      "movie-ranker-v1",
			ServingLoadStatus: model.ModelLoadStatusLoaded,
		}, idempotencyKey)

		Expect(err).NotTo(HaveOccurred())
		Expect(repo.status).To(Equal(model.ModelStatusReady))
		Expect(repo.loadStatus).To(Equal(model.ModelLoadStatusLoaded))
		Expect(repo.servingKey).To(Equal(idempotencyKey))
		Expect(result.Status).To(Equal(model.ModelStatusReady))
	})

	It("publishes a ready inference endpoint when a tenant model is loaded", func() {
		modelRecord := validModel()
		endpoints := &publishedEndpointRepositoryStub{}
		repo := &modelRepositoryStub{readModel: modelRecord}
		uc := app.NewModelRegistryUsecase(repo, &modelUnitOfWorkStub{}, modelEventBuilder(), app.WithPublishedEndpointRepository(endpoints))

		_, err := uc.RecordModelServingStatus(context.Background(), &model.ServedModelStatus{
			ModelID:           modelRecord.ModelID,
			ServingTarget:     "vllm-local",
			ServingModel:      "movie-ranker-v1",
			ServingLoadStatus: model.ModelLoadStatusLoaded,
		}, uuid.New())

		Expect(err).NotTo(HaveOccurred())
		Expect(endpoints.endpoint).NotTo(BeNil())
		Expect(endpoints.endpoint.OrgID).To(Equal(modelRecord.OrgID))
		Expect(endpoints.endpoint.ModelID).To(Equal(modelRecord.ModelID))
		Expect(endpoints.endpoint.DatasetID).To(Equal(modelRecord.DatasetID))
		Expect(endpoints.endpoint.Status).To(Equal(model.PublishedEndpointStatusReady))
		Expect(endpoints.endpoint.DisplayName).To(Equal(modelRecord.Name))
	})

	It("publishes a disabled inference endpoint for an unloaded tenant model", func() {
		modelRecord := validModel()
		endpoints := &publishedEndpointRepositoryStub{}
		repo := &modelRepositoryStub{}
		uc := app.NewModelRegistryUsecase(repo, &modelUnitOfWorkStub{}, modelEventBuilder(), app.WithPublishedEndpointRepository(endpoints))

		result, err := uc.RecordModelTrainingCompleted(context.Background(), modelRecord, uuid.New())

		Expect(err).NotTo(HaveOccurred())
		Expect(result.Status).To(Equal(model.ModelStatusCandidate))
		Expect(endpoints.endpoint).NotTo(BeNil())
		Expect(endpoints.endpoint.Status).To(Equal(model.PublishedEndpointStatusDisabled))
	})

	It("does not publish an inference endpoint for shared base models", func() {
		modelRecord := validModel()
		modelRecord.ModelKind = model.ModelKindBase
		modelRecord.UserID = uuid.Nil
		modelRecord.OrgID = uuid.Nil
		modelRecord.DatasetID = uuid.Nil
		endpoints := &publishedEndpointRepositoryStub{}
		repo := &modelRepositoryStub{}
		uc := app.NewModelRegistryUsecase(repo, &modelUnitOfWorkStub{}, modelEventBuilder(), app.WithPublishedEndpointRepository(endpoints))

		_, err := uc.RecordModelArtifactIngested(context.Background(), modelRecord, uuid.New())

		Expect(err).NotTo(HaveOccurred())
		Expect(endpoints.endpoint).To(BeNil())
	})

	It("does not let serving status make a candidate ready", func() {
		modelRecord := validModel()
		modelRecord.Status = model.ModelStatusCandidate
		repo := &modelRepositoryStub{readModel: modelRecord}
		uc := app.NewModelRegistryUsecase(repo, &modelUnitOfWorkStub{}, modelEventBuilder())

		result, err := uc.RecordModelServingStatus(context.Background(), &model.ServedModelStatus{
			ModelID:           modelRecord.ModelID,
			ServingLoadStatus: model.ModelLoadStatusLoaded,
		}, uuid.New())

		Expect(err).NotTo(HaveOccurred())
		Expect(repo.status).To(Equal(model.ModelStatusCandidate))
		Expect(result.Status).To(Equal(model.ModelStatusCandidate))
	})

	It("records serving failed as a failed model", func() {
		modelRecord := validModel()
		repo := &modelRepositoryStub{readModel: modelRecord}
		uc := app.NewModelRegistryUsecase(repo, &modelUnitOfWorkStub{}, modelEventBuilder())
		idempotencyKey := uuid.New()

		result, err := uc.RecordModelServingStatus(context.Background(), &model.ServedModelStatus{
			ModelID:           modelRecord.ModelID,
			ServingTarget:     "vllm-local",
			ServingModel:      "movie-ranker-v1",
			ServingLoadStatus: model.ModelLoadStatusFailed,
			FailureReason:     "adapter load failed",
		}, idempotencyKey)

		Expect(err).NotTo(HaveOccurred())
		Expect(repo.status).To(Equal(model.ModelStatusFailed))
		Expect(repo.loadStatus).To(Equal(model.ModelLoadStatusFailed))
		Expect(repo.servingKey).To(Equal(idempotencyKey))
		Expect(result.Status).To(Equal(model.ModelStatusFailed))
	})

	It("records failed training as a failed model", func() {
		repo := &modelRepositoryStub{}
		uc := app.NewModelRegistryUsecase(repo, &modelUnitOfWorkStub{}, modelEventBuilder())
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
		UserID:            uuid.New(),
		OrgID:             uuid.New(),
		TrainingRunID:     uuid.New(),
		DatasetID:         uuid.New(),
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
		MetricsMetadata:   metricsMetadata(0.91, 0.90, 0.89),
	}
}

func metricsMetadata(faithfulness float64, answerRelevancy float64, contextPrecision float64) string {
	return fmt.Sprintf(`{"passed":true,"metrics":{"faithfulness":%.2f,"answer_relevancy":%.2f,"context_precision":%.2f},"thresholds":{"faithfulness":0.8,"answer_relevancy":0.8,"context_precision":0.8},"report_uri":"s3://local-dev-bucket/evaluations/run.json","evaluator_name":"ragas","evaluator_version":"ragas-v1","metric_suite":"rag","eval_dataset_uri":"s3://evals/held-out.jsonl","eval_dataset_mode":"labeled"}`, faithfulness, answerRelevancy, contextPrecision)
}

func evidenceMetricsMetadata() string {
	return `{"passed":true,"metrics":{"faithfulness":0.91,"answer_relevancy":0.90,"context_precision":0.89},"thresholds":{"faithfulness":0.8,"answer_relevancy":0.8,"context_precision":0.8},"report_uri":"s3://local-dev-bucket/evaluations/run.json","evaluator_name":"ragas","evaluator_version":"ragas-v1","metric_suite":"rag","eval_dataset_uri":"s3://evals/held-out.jsonl","eval_dataset_mode":"labeled","deepchecks_passed":true,"deepchecks_report_uri":"s3://evals/deepchecks.html","evidently_passed":true,"evidently_report_uri":"s3://evals/evidently.html"}`
}
