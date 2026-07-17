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

	"lib/shared_lib/authz"
	"lib/shared_lib/ctxutil"
	msgConn "lib/shared_lib/messaging"
	transport "lib/shared_lib/transport"
	shareduow "lib/shared_lib/uow"
	"lib/shared_lib/userevents"

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
	servingChanged     bool
	servingChangedSet  bool
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

func (s *modelRepositoryStub) UpdateServingStatus(_ context.Context, _ pgx.Tx, _ uuid.UUID, status model.ModelStatus, loadStatus model.ModelLoadStatus, servingTarget string, servingModel string, servingProtocol model.ServingProtocol, failureReason string, idempotencyKey uuid.UUID) (*model.Model, bool, error) {
	s.status = status
	s.loadStatus = loadStatus
	s.servingKey = idempotencyKey
	s.failure = failureReason
	if s.readModel != nil {
		s.readModel.Status = status
		s.readModel.ServingLoadStatus = loadStatus
		s.readModel.ServingTarget = servingTarget
		s.readModel.ServingModel = servingModel
		s.readModel.ServingProtocol = servingProtocol
		s.readModel.FailureReason = failureReason
	}
	changed := true
	if s.servingChangedSet {
		changed = s.servingChanged
	}
	return s.readModel, changed, s.updateErr
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

type effectiveBaseRepositoryStub struct {
	recorded              *model.EffectiveBaseVersion
	read                  *model.EffectiveBaseVersion
	readEffectiveBaseID   string
	readFoundationModelID uuid.UUID
	err                   error
}

func (s *effectiveBaseRepositoryStub) RecordEffectiveBase(_ context.Context, _ pgx.Tx, effectiveBase *model.EffectiveBaseVersion) (*model.EffectiveBaseVersion, error) {
	s.recorded = effectiveBase
	if s.err != nil {
		return nil, s.err
	}
	record := *effectiveBase
	if record.EffectiveBaseID == "" {
		record.EffectiveBaseID = "sha256-recorded-effective-base"
	}
	return &record, nil
}

func (s *effectiveBaseRepositoryStub) ReadByID(_ context.Context, effectiveBaseID string) (*model.EffectiveBaseVersion, error) {
	s.readEffectiveBaseID = effectiveBaseID
	if s.err != nil {
		return nil, s.err
	}
	return s.read, nil
}

func (s *effectiveBaseRepositoryStub) ReadLatestByFoundationModelID(_ context.Context, modelID uuid.UUID) (*model.EffectiveBaseVersion, error) {
	s.readFoundationModelID = modelID
	if s.err != nil {
		return nil, s.err
	}
	return s.read, nil
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
		registeredModel.DatasetID = uuid.Nil
		registeredModel.ModelKind = model.ModelKindBase

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

	It("rejects a candidate when metrics metadata cannot be mapped to gate metrics", func() {
		candidate := validModel()
		candidate.Status = model.ModelStatusCandidate
		candidate.MetricsMetadata = `{"passed":true}`
		repo := &modelRepositoryStub{readModel: candidate}
		deployer := &modelServingDeployerStub{}
		uc := app.NewModelRegistryUsecase(repo, &modelUnitOfWorkStub{}, modelEventBuilder(), app.WithModelServingDeployer(deployer))

		result, err := uc.PromoteCandidate(context.Background(), candidate.ModelID)

		Expect(err).NotTo(HaveOccurred())
		Expect(result.Status).To(Equal(model.ModelStatusFailed))
		Expect(result.PromotionDecision).To(ContainSubstring("PROMOTION_REJECTED"))
		Expect(result.FailureReason).To(ContainSubstring("metrics metadata must include metrics"))
		Expect(deployer.servedModel).To(BeNil())
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

	It("records an effective base when a base model serving runtime loads", func() {
		modelRecord := validModel()
		modelRecord.ModelKind = model.ModelKindBase
		modelRecord.AdapterURI = ""
		modelRecord.AdapterRank = 0
		modelRecord.ArtifactFormat = "GGUF"
		modelRecord.ArtifactChecksum = "sha256:base-artifact"
		modelRecord.ServingLoadStatus = model.ModelLoadStatusNotLoaded
		repo := &modelRepositoryStub{readModel: modelRecord}
		effectiveBases := &effectiveBaseRepositoryStub{}
		uc := app.NewModelRegistryUsecase(repo, &modelUnitOfWorkStub{}, modelEventBuilder(), app.WithEffectiveBaseRepository(effectiveBases))

		_, err := uc.RecordModelServingStatus(context.Background(), &model.ServedModelStatus{
			ModelID:           modelRecord.ModelID,
			ServingTarget:     "http://vllm-runtime",
			ServingModel:      "base-mistral",
			ServingProtocol:   model.ServingProtocolOpenAIChatCompletions,
			ServingLoadStatus: model.ModelLoadStatusLoaded,
		}, uuid.New())

		Expect(err).NotTo(HaveOccurred())
		Expect(effectiveBases.recorded).NotTo(BeNil())
		Expect(effectiveBases.recorded.EffectiveBaseID).NotTo(BeEmpty())
		Expect(effectiveBases.recorded.FoundationModelID).To(Equal(modelRecord.ModelID))
		Expect(effectiveBases.recorded.DescriptorSchemaVersion).To(Equal(model.EffectiveBaseDescriptorSchemaVersion))
		Expect(effectiveBases.recorded.FoundationChecksum).To(Equal("sha256:base-artifact"))
		Expect(effectiveBases.recorded.Descriptor).To(MatchJSON(`{
			"descriptor_schema_version": 1,
			"foundation_model_id": "` + modelRecord.ModelID.String() + `",
			"artifact_uri": "` + modelRecord.ArtifactLocation + `",
			"artifact_format": "GGUF",
			"foundation_checksum": "sha256:base-artifact",
			"serving_protocol": "OPENAI_CHAT_COMPLETIONS",
			"serving_model": "base-mistral"
		}`))
	})

	It("does not record an effective base for a fine tuned adapter", func() {
		modelRecord := validModel()
		modelRecord.ModelKind = model.ModelKindFineTuned
		modelRecord.AdapterURI = "s3://local-dev-bucket/models/adapter"
		repo := &modelRepositoryStub{readModel: modelRecord}
		effectiveBases := &effectiveBaseRepositoryStub{}
		uc := app.NewModelRegistryUsecase(repo, &modelUnitOfWorkStub{}, modelEventBuilder(), app.WithEffectiveBaseRepository(effectiveBases))

		_, err := uc.RecordModelServingStatus(context.Background(), &model.ServedModelStatus{
			ModelID:           modelRecord.ModelID,
			ServingTarget:     "http://vllm-runtime",
			ServingModel:      "adapter-ranker",
			ServingProtocol:   model.ServingProtocolOpenAIChatCompletions,
			ServingLoadStatus: model.ModelLoadStatusLoaded,
		}, uuid.New())

		Expect(err).NotTo(HaveOccurred())
		Expect(effectiveBases.recorded).To(BeNil())
	})

	It("reads an effective base by digest", func() {
		readRecord := &model.EffectiveBaseVersion{
			EffectiveBaseID:         "sha256-effective-base",
			FoundationModelID:       uuid.New(),
			DescriptorSchemaVersion: model.EffectiveBaseDescriptorSchemaVersion,
			FoundationChecksum:      "sha256:base",
			Descriptor:              `{"descriptor_schema_version":1}`,
		}
		effectiveBases := &effectiveBaseRepositoryStub{read: readRecord}
		uc := app.NewModelRegistryUsecase(&modelRepositoryStub{}, &modelUnitOfWorkStub{}, modelEventBuilder(), app.WithEffectiveBaseRepository(effectiveBases))

		result, err := uc.ReadEffectiveBase(context.Background(), uuid.New(), "sha256-effective-base")

		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(readRecord))
		Expect(effectiveBases.readEffectiveBaseID).To(Equal("sha256-effective-base"))
	})

	It("reads the latest effective base for a model", func() {
		modelID := uuid.New()
		readRecord := &model.EffectiveBaseVersion{
			EffectiveBaseID:         "sha256-effective-base",
			FoundationModelID:       modelID,
			DescriptorSchemaVersion: model.EffectiveBaseDescriptorSchemaVersion,
			FoundationChecksum:      "sha256:base",
			Descriptor:              `{"descriptor_schema_version":1}`,
		}
		effectiveBases := &effectiveBaseRepositoryStub{read: readRecord}
		uc := app.NewModelRegistryUsecase(&modelRepositoryStub{}, &modelUnitOfWorkStub{}, modelEventBuilder(), app.WithEffectiveBaseRepository(effectiveBases))

		result, err := uc.ReadLatestEffectiveBaseForModel(context.Background(), uuid.New(), modelID)

		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(readRecord))
		Expect(effectiveBases.readFoundationModelID).To(Equal(modelID))
	})

	It("publishes a user event when serving status loads", func() {
		modelRecord := validModel()
		modelRecord.ServingLoadStatus = model.ModelLoadStatusNotLoaded
		repo := &modelRepositoryStub{readModel: modelRecord}
		publisher := userevents.NewRecordingPublisher()
		uc := app.NewModelRegistryUsecase(repo, &modelUnitOfWorkStub{}, modelEventBuilder(), app.WithUserEventPublisher(publisher))

		_, err := uc.RecordModelServingStatus(context.Background(), &model.ServedModelStatus{
			ModelID:           modelRecord.ModelID,
			ServingLoadStatus: model.ModelLoadStatusLoaded,
		}, uuid.New())

		Expect(err).NotTo(HaveOccurred())
		events := publisher.Events()
		Expect(events).To(HaveLen(1))
		Expect(events[0].EventType).To(Equal(userevents.EventTypeModelServingLoaded))
		Expect(events[0].Severity).To(Equal(userevents.SeveritySuccess))
		Expect(events[0].UserID).To(Equal(modelRecord.UserID.String()))
		Expect(events[0].OrgID).To(Equal(modelRecord.OrgID.String()))
		Expect(events[0].RequiredPermission).To(Equal(authz.PermissionModelRead))
		Expect(events[0].Status.State).To(Equal(model.ModelLoadStatusLoaded.String()))
		Expect(events[0].Status.PreviousState).To(Equal(model.ModelLoadStatusNotLoaded.String()))
	})

	It("publishes a classified user event when serving status fails", func() {
		modelRecord := validModel()
		modelRecord.ServingLoadStatus = model.ModelLoadStatusNotLoaded
		repo := &modelRepositoryStub{readModel: modelRecord}
		publisher := userevents.NewRecordingPublisher()
		uc := app.NewModelRegistryUsecase(repo, &modelUnitOfWorkStub{}, modelEventBuilder(), app.WithUserEventPublisher(publisher))

		_, err := uc.RecordModelServingStatus(context.Background(), &model.ServedModelStatus{
			ModelID:           modelRecord.ModelID,
			ServingLoadStatus: model.ModelLoadStatusFailed,
			FailureReason:     "Ollama did not infer a usable chat model from GGUF metadata",
		}, uuid.New())

		Expect(err).NotTo(HaveOccurred())
		events := publisher.Events()
		Expect(events).To(HaveLen(1))
		Expect(events[0].EventType).To(Equal(userevents.EventTypeModelServingFailed))
		Expect(events[0].Severity).To(Equal(userevents.SeverityError))
		Expect(events[0].RequiredPermission).To(Equal(authz.PermissionModelRead))
		Expect(events[0].Error).NotTo(BeNil())
		Expect(events[0].Error.Code).To(Equal(userevents.ErrorCodeModelServingChatDefinitionUnusable))
		Expect(events[0].Message).To(Equal("The model could not be exposed as a chat model."))
	})

	It("does not publish a user event when serving status is unchanged", func() {
		modelRecord := validModel()
		repo := &modelRepositoryStub{readModel: modelRecord, servingChangedSet: true, servingChanged: false}
		publisher := userevents.NewRecordingPublisher()
		uc := app.NewModelRegistryUsecase(repo, &modelUnitOfWorkStub{}, modelEventBuilder(), app.WithUserEventPublisher(publisher))

		_, err := uc.RecordModelServingStatus(context.Background(), &model.ServedModelStatus{
			ModelID:           modelRecord.ModelID,
			ServingLoadStatus: model.ModelLoadStatusLoaded,
		}, uuid.New())

		Expect(err).NotTo(HaveOccurred())
		Expect(publisher.Events()).To(BeEmpty())
	})

	It("does not publish a user event when serving status update fails", func() {
		modelRecord := validModel()
		repo := &modelRepositoryStub{readModel: modelRecord, updateErr: fmt.Errorf("db failed")}
		publisher := userevents.NewRecordingPublisher()
		uc := app.NewModelRegistryUsecase(repo, &modelUnitOfWorkStub{}, modelEventBuilder(), app.WithUserEventPublisher(publisher))

		_, err := uc.RecordModelServingStatus(context.Background(), &model.ServedModelStatus{
			ModelID:           modelRecord.ModelID,
			ServingLoadStatus: model.ModelLoadStatusLoaded,
		}, uuid.New())

		Expect(err).To(HaveOccurred())
		Expect(publisher.Events()).To(BeEmpty())
	})

	It("does not fail the serving status update when user event publishing fails", func() {
		modelRecord := validModel()
		repo := &modelRepositoryStub{readModel: modelRecord}
		publisher := userevents.NewRecordingPublisher()
		publisher.SetError(fmt.Errorf("redis unavailable"))
		uc := app.NewModelRegistryUsecase(repo, &modelUnitOfWorkStub{}, modelEventBuilder(), app.WithUserEventPublisher(publisher))

		result, err := uc.RecordModelServingStatus(context.Background(), &model.ServedModelStatus{
			ModelID:           modelRecord.ModelID,
			ServingLoadStatus: model.ModelLoadStatusLoaded,
		}, uuid.New())

		Expect(err).NotTo(HaveOccurred())
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

	It("does not publish an inference endpoint for tenant base models without a provenance dataset", func() {
		modelRecord := validModel()
		modelRecord.ModelKind = model.ModelKindBase
		modelRecord.DatasetID = uuid.Nil
		endpoints := &publishedEndpointRepositoryStub{}
		repo := &modelRepositoryStub{}
		uc := app.NewModelRegistryUsecase(repo, &modelUnitOfWorkStub{}, modelEventBuilder(), app.WithPublishedEndpointRepository(endpoints))

		_, err := uc.RecordModelArtifactIngested(context.Background(), modelRecord, uuid.New())

		Expect(err).NotTo(HaveOccurred())
		Expect(endpoints.endpoint).To(BeNil())
	})

	It("publishes an inference endpoint for tenant-scoped base model artifacts", func() {
		modelRecord := validModel()
		modelRecord.ModelKind = model.ModelKindBase
		modelRecord.Source = model.ModelSourceUpload
		modelRecord.Status = model.ModelStatusEvaluated
		modelRecord.ServingLoadStatus = model.ModelLoadStatusLoaded
		endpoints := &publishedEndpointRepositoryStub{}
		repo := &modelRepositoryStub{}
		uc := app.NewModelRegistryUsecase(repo, &modelUnitOfWorkStub{}, modelEventBuilder(), app.WithPublishedEndpointRepository(endpoints))

		_, err := uc.RecordModelArtifactIngested(context.Background(), modelRecord, uuid.New())

		Expect(err).NotTo(HaveOccurred())
		Expect(repo.createdModel.UserID).To(Equal(modelRecord.UserID))
		Expect(repo.createdModel.OrgID).To(Equal(modelRecord.OrgID))
		Expect(repo.createdModel.DatasetID).To(Equal(modelRecord.DatasetID))
		Expect(endpoints.endpoint).NotTo(BeNil())
		Expect(endpoints.endpoint.OrgID).To(Equal(modelRecord.OrgID))
		Expect(endpoints.endpoint.ModelID).To(Equal(modelRecord.ModelID))
		Expect(endpoints.endpoint.DatasetID).To(Equal(modelRecord.DatasetID))
		Expect(endpoints.endpoint.Status).To(Equal(model.PublishedEndpointStatusReady))
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
		Expect(repo.createdModel.ModelKind).To(Equal(model.ModelKindFineTuned))
		Expect(repo.createdModel.Source).To(Equal(model.ModelSourceTraining))
		Expect(repo.createdModel.ServingLoadStatus).To(Equal(model.ModelLoadStatusNotLoaded))
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
