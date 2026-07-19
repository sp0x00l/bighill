package app_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"agent_registry_service/pkg/app"
	"agent_registry_service/pkg/domain"
	"agent_registry_service/pkg/domain/model"
	msgConn "lib/shared_lib/messaging"
	shareduow "lib/shared_lib/uow"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestAgentRegistryUsecase(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Agent registry app unit test suite")
}

type registryRepositoryStub struct {
	ensuredLineage        string
	upsertedVersion       *model.AgentSpecVersion
	upsertedBinding       *model.AgentEndpointBinding
	specVersion           *model.AgentSpecVersion
	championState         *model.AgentChampionState
	endpointBindings      []*model.AgentEndpointBinding
	createdGoldenTasks    []*model.GoldenTask
	leakConflicts         []model.GoldenTaskLeakConflict
	listGoldenTasks       []*model.GoldenTask
	labels                []*model.AgentRunLabel
	trajectoryDataset     *model.AgentTrajectoryDataset
	agentAdapter          *model.AgentAdapter
	champion              *model.AgentChampionState
	championReport        *model.AgentEvalReport
	recordedEvalReport    *model.AgentEvalReport
	recordedLabel         *model.AgentRunLabel
	recordedDataset       *model.AgentTrajectoryDataset
	recordedAdapter       *model.AgentAdapter
	evalReport            *model.AgentEvalReport
	readSpecErr           error
	recordErr             error
	listBindingsErr       error
	upsertVersionErr      error
	upsertBindingErr      error
	ensureLineageErr      error
	createGoldenTaskErr   error
	leakConflictErr       error
	listGoldenTasksErr    error
	recordEvalErr         error
	readEvalErr           error
	readDatasetErr        error
	readAdapterErr        error
	readChampionErr       error
	readChampionReportErr error
	recordedChampionSpec  string
	completedTraining     model.AgentAdapterTrainingCompletion
	failedTraining        model.AgentAdapterTrainingFailure
}

func (s *registryRepositoryStub) EnsureLineage(_ context.Context, _ pgx.Tx, _ uuid.UUID, agentLineage string, _ uuid.UUID) error {
	s.ensuredLineage = agentLineage
	return s.ensureLineageErr
}

func (s *registryRepositoryStub) UpsertAgentSpecVersion(_ context.Context, _ pgx.Tx, version *model.AgentSpecVersion) (*model.AgentSpecVersion, error) {
	s.upsertedVersion = version
	return version, s.upsertVersionErr
}

func (s *registryRepositoryStub) UpsertEndpointBinding(_ context.Context, _ pgx.Tx, binding *model.AgentEndpointBinding) (*model.AgentEndpointBinding, error) {
	s.upsertedBinding = binding
	return binding, s.upsertBindingErr
}

func (s *registryRepositoryStub) ReadSpecVersion(context.Context, uuid.UUID, string) (*model.AgentSpecVersion, error) {
	if s.readSpecErr != nil {
		return nil, s.readSpecErr
	}
	return s.specVersion, nil
}

func (s *registryRepositoryStub) RecordChampionState(_ context.Context, _ pgx.Tx, state *model.AgentChampionState) (*model.AgentChampionState, error) {
	s.recordedChampionSpec = state.ChampionAgentSpecHash
	if s.recordErr != nil {
		return nil, s.recordErr
	}
	if s.championState != nil {
		return s.championState, nil
	}
	return state, nil
}

func (s *registryRepositoryStub) ListEndpointBindings(context.Context, uuid.UUID, string) ([]*model.AgentEndpointBinding, error) {
	return s.endpointBindings, s.listBindingsErr
}

func (s *registryRepositoryStub) CreateGoldenTask(_ context.Context, _ pgx.Tx, task *model.GoldenTask) (*model.GoldenTask, error) {
	if s.createGoldenTaskErr != nil {
		return nil, s.createGoldenTaskErr
	}
	if task.TaskID == uuid.Nil {
		task.TaskID = uuid.New()
	}
	task.CreatedAt = time.Now().UTC()
	s.createdGoldenTasks = append(s.createdGoldenTasks, task)
	return task, nil
}

func (s *registryRepositoryStub) FindGoldenTaskLeakConflicts(context.Context, pgx.Tx, *model.GoldenTask) ([]model.GoldenTaskLeakConflict, error) {
	return s.leakConflicts, s.leakConflictErr
}

func (s *registryRepositoryStub) ListGoldenTasks(context.Context, model.ListGoldenTasksCommand) ([]*model.GoldenTask, error) {
	return s.listGoldenTasks, s.listGoldenTasksErr
}

func (s *registryRepositoryStub) RecordAgentRunLabel(_ context.Context, _ pgx.Tx, label *model.AgentRunLabel) (*model.AgentRunLabel, error) {
	s.recordedLabel = label
	if label.LabelID == uuid.Nil {
		label.LabelID = uuid.New()
	}
	return label, nil
}

func (s *registryRepositoryStub) ListAgentRunLabels(context.Context, model.ListAgentRunLabelsCommand) ([]*model.AgentRunLabel, error) {
	return s.labels, nil
}

func (s *registryRepositoryStub) RecordTrajectoryDataset(_ context.Context, _ pgx.Tx, dataset *model.AgentTrajectoryDataset) (*model.AgentTrajectoryDataset, error) {
	s.recordedDataset = dataset
	if dataset.DatasetID == uuid.Nil {
		dataset.DatasetID = uuid.New()
	}
	return dataset, nil
}

func (s *registryRepositoryStub) ReadTrajectoryDataset(context.Context, uuid.UUID, uuid.UUID) (*model.AgentTrajectoryDataset, error) {
	return s.trajectoryDataset, s.readDatasetErr
}

func (s *registryRepositoryStub) RecordAgentAdapter(_ context.Context, _ pgx.Tx, adapter *model.AgentAdapter) (*model.AgentAdapter, error) {
	s.recordedAdapter = adapter
	if adapter.AdapterID == uuid.Nil {
		adapter.AdapterID = uuid.New()
	}
	return adapter, nil
}

func (s *registryRepositoryStub) ReadAgentAdapter(context.Context, uuid.UUID, uuid.UUID) (*model.AgentAdapter, error) {
	return s.agentAdapter, s.readAdapterErr
}

func (s *registryRepositoryStub) CompleteAgentAdapterTraining(_ context.Context, _ pgx.Tx, completion model.AgentAdapterTrainingCompletion) (*model.AgentAdapter, error) {
	s.completedTraining = completion
	if s.agentAdapter == nil {
		s.agentAdapter = &model.AgentAdapter{
			OrgID:         completion.OrgID,
			TrainingRunID: completion.TrainingRunID,
		}
	}
	s.agentAdapter.ServingModelID = completion.ServingModelID
	s.agentAdapter.AdapterURI = completion.AdapterURI
	s.agentAdapter.AdapterChecksum = completion.AdapterChecksum
	s.agentAdapter.TrainingProvider = completion.TrainingProvider
	s.agentAdapter.Status = model.AgentAdapterStatusCandidate
	return s.agentAdapter, nil
}

func (s *registryRepositoryStub) FailAgentAdapterTraining(_ context.Context, _ pgx.Tx, failure model.AgentAdapterTrainingFailure) (*model.AgentAdapter, error) {
	s.failedTraining = failure
	if s.agentAdapter == nil {
		s.agentAdapter = &model.AgentAdapter{
			OrgID:         failure.OrgID,
			TrainingRunID: failure.TrainingRunID,
		}
	}
	s.agentAdapter.Status = model.AgentAdapterStatusFailed
	return s.agentAdapter, nil
}

func (s *registryRepositoryStub) UpdateAgentAdapterPromotion(_ context.Context, _ pgx.Tx, adapterID uuid.UUID, status model.AgentAdapterStatus, promotionPassed bool) (*model.AgentAdapter, error) {
	if s.agentAdapter == nil {
		return nil, s.readAdapterErr
	}
	s.agentAdapter.AdapterID = adapterID
	s.agentAdapter.Status = status
	s.agentAdapter.PromotionPassed = promotionPassed
	return s.agentAdapter, nil
}

func (s *registryRepositoryStub) ReadChampionState(context.Context, uuid.UUID, string) (*model.AgentChampionState, error) {
	return s.champion, s.readChampionErr
}

func (s *registryRepositoryStub) ReadLatestEvalReportForAdapter(context.Context, uuid.UUID, uuid.UUID) (*model.AgentEvalReport, error) {
	return s.championReport, s.readChampionReportErr
}

func (s *registryRepositoryStub) RecordAgentEvalReport(_ context.Context, _ pgx.Tx, report *model.AgentEvalReport) (*model.AgentEvalReport, error) {
	s.recordedEvalReport = report
	if s.recordEvalErr != nil {
		return nil, s.recordEvalErr
	}
	if report.ReportID == uuid.Nil {
		report.ReportID = uuid.New()
	}
	if report.EvaluatedAt.IsZero() {
		report.EvaluatedAt = time.Now().UTC()
	}
	return report, nil
}

func (s *registryRepositoryStub) ReadAgentEvalReport(context.Context, uuid.UUID, uuid.UUID) (*model.AgentEvalReport, error) {
	return s.evalReport, s.readEvalErr
}

type registryUnitOfWorkStub struct {
	messages []msgConn.OutboundMessage
	err      error
}

func (s *registryUnitOfWorkStub) Do(ctx context.Context, fn shareduow.TxFunc) error {
	if s.err != nil {
		return s.err
	}
	return fn(ctx, nil, func(msg msgConn.OutboundMessage) error {
		s.messages = append(s.messages, msg)
		return nil
	})
}

type inferenceVerifierStub struct {
	spec          *model.AgentSpecRef
	endpoint      *model.EndpointRef
	trajectory    *model.AgentTrajectoryRef
	readSpecHash  string
	readEndpoint  uuid.UUID
	readRunID     uuid.UUID
	specErr       error
	endpointErr   error
	trajectoryErr error
}

func (s *inferenceVerifierStub) ReadAgentSpec(_ context.Context, _ uuid.UUID, _ uuid.UUID, agentSpecHash string) (*model.AgentSpecRef, error) {
	s.readSpecHash = agentSpecHash
	return s.spec, s.specErr
}

func (s *inferenceVerifierStub) ReadEndpoint(_ context.Context, _ uuid.UUID, _ uuid.UUID, endpointID uuid.UUID) (*model.EndpointRef, error) {
	s.readEndpoint = endpointID
	return s.endpoint, s.endpointErr
}

func (s *inferenceVerifierStub) ReadAgentTrajectory(_ context.Context, _ uuid.UUID, _ uuid.UUID, runID uuid.UUID) (*model.AgentTrajectoryRef, error) {
	s.readRunID = runID
	return s.trajectory, s.trajectoryErr
}

type registryEventBuilderStub struct{}

func (registryEventBuilderStub) AgentChampionUpdatedMessage(state *model.AgentChampionState, binding *model.AgentEndpointBinding) shareduow.OutboundMessage {
	return msgConn.OutboundMessage{
		Topic: "agent_registry",
		Message: msgConn.Message{
			ResourceKey: binding.EndpointID,
			MsgType:     msgConn.MsgTypeAgentChampionUpdated,
			Payload:     []byte(state.ChampionAgentSpecHash),
		},
		DispatchKey: binding.EndpointID.String() + ":" + state.DecisionID.String(),
	}
}

var _ = Describe("AgentRegistryUsecase", func() {
	var (
		ctx                context.Context
		orgID              uuid.UUID
		userID             uuid.UUID
		modelID            uuid.UUID
		endpointID         uuid.UUID
		hash               string
		lineage            string
		repo               *registryRepositoryStub
		uow                *registryUnitOfWorkStub
		verifier           *inferenceVerifierStub
		uc                 app.AgentRegistryUsecase
		runner             *agentTaskRunnerStub
		trainingDispatcher *agentAdapterTrainingDispatcherStub
	)

	BeforeEach(func() {
		ctx = context.Background()
		orgID = uuid.New()
		userID = uuid.New()
		modelID = uuid.New()
		endpointID = uuid.New()
		hash = "sha256-agent-spec"
		lineage = "support-agent"
		repo = &registryRepositoryStub{}
		uow = &registryUnitOfWorkStub{}
		verifier = &inferenceVerifierStub{
			spec:     &model.AgentSpecRef{OrgID: orgID, AgentSpecHash: hash, AgentLineage: lineage, ModelID: modelID},
			endpoint: &model.EndpointRef{OrgID: orgID, EndpointID: endpointID},
			trajectory: &model.AgentTrajectoryRef{
				RunID:            uuid.New(),
				OrgID:            orgID,
				UserID:           userID,
				EndpointID:       endpointID,
				AgentSpecHash:    hash,
				ToolsetHash:      "sha256-toolset",
				EffectiveBaseID:  "sha256-base",
				DataSnapshotHash: "sha256-data",
				Status:           "COMPLETED",
				StopReason:       "FINAL_ANSWER",
			},
		}
		runner = &agentTaskRunnerStub{}
		trainingDispatcher = &agentAdapterTrainingDispatcherStub{result: &model.AgentAdapterTrainingResult{
			TrainingRunID:    uuid.New(),
			ServingModelID:   uuid.New(),
			AdapterURI:       "agent-adapter://trained",
			AdapterChecksum:  "sha256-adapter",
			TrainingProvider: "deterministic-agent-training",
		}}
		uc = app.NewAgentRegistryUsecase(repo, uow, verifier, registryEventBuilderStub{}, runner, trainingDispatcher)
	})

	It("registers a core-owned agent spec hash after inference verifies lineage", func() {
		version, err := uc.RegisterAgentSpecVersion(ctx, model.RegisterAgentSpecVersionCommand{
			OrgID:         orgID,
			UserID:        userID,
			AgentLineage:  lineage,
			AgentSpecHash: hash,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(version.AgentSpecHash).To(Equal(hash))
		Expect(version.ModelID).To(Equal(modelID))
		Expect(verifier.readSpecHash).To(Equal(hash))
		Expect(repo.ensuredLineage).To(Equal(lineage))
		Expect(repo.upsertedVersion.RegisteredByUserID).To(Equal(userID))
	})

	It("rejects registering a hash whose verified lineage differs", func() {
		verifier.spec.AgentLineage = "other-agent"

		version, err := uc.RegisterAgentSpecVersion(ctx, model.RegisterAgentSpecVersionCommand{
			OrgID:         orgID,
			UserID:        userID,
			AgentLineage:  lineage,
			AgentSpecHash: hash,
		})

		Expect(version).To(BeNil())
		Expect(errors.Is(err, domain.ErrAgentRegistryValidation)).To(BeTrue())
		Expect(repo.upsertedVersion).To(BeNil())
	})

	It("registers an endpoint binding after inference verifies the endpoint", func() {
		binding, err := uc.RegisterEndpointBinding(ctx, model.RegisterEndpointBindingCommand{
			OrgID:        orgID,
			UserID:       userID,
			AgentLineage: lineage,
			EndpointID:   endpointID,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(binding.EndpointID).To(Equal(endpointID))
		Expect(verifier.readEndpoint).To(Equal(endpointID))
		Expect(repo.upsertedBinding.AgentLineage).To(Equal(lineage))
	})

	It("promotes a registered spec and emits champion updates for bound endpoints", func() {
		decisionID := uuid.New()
		decidedAt := time.Date(2026, 7, 18, 10, 0, 0, 0, time.UTC)
		secondEndpoint := uuid.New()
		repo.specVersion = &model.AgentSpecVersion{OrgID: orgID, AgentLineage: lineage, AgentSpecHash: hash, ModelID: modelID}
		repo.endpointBindings = []*model.AgentEndpointBinding{
			{OrgID: orgID, AgentLineage: lineage, EndpointID: endpointID, CreatedByUserID: userID},
			{OrgID: orgID, AgentLineage: lineage, EndpointID: secondEndpoint, CreatedByUserID: userID},
		}

		state, err := uc.PromoteSpecChampion(ctx, model.PromoteSpecChampionCommand{
			OrgID:         orgID,
			UserID:        userID,
			AgentLineage:  lineage,
			AgentSpecHash: hash,
			DecisionID:    decisionID,
			DecidedAt:     decidedAt,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(state.ChampionAgentSpecHash).To(Equal(hash))
		Expect(state.DecisionID).To(Equal(decisionID))
		Expect(state.DecidedAt).To(Equal(decidedAt))
		Expect(repo.recordedChampionSpec).To(Equal(hash))
		Expect(uow.messages).To(HaveLen(2))
		Expect(uow.messages[0].Message.ResourceKey).To(Equal(endpointID))
		Expect(uow.messages[1].Message.ResourceKey).To(Equal(secondEndpoint))
	})

	It("fails closed when promoting an unregistered spec hash", func() {
		repo.readSpecErr = domain.ErrAgentVersionNotFound

		state, err := uc.PromoteSpecChampion(ctx, model.PromoteSpecChampionCommand{
			OrgID:         orgID,
			UserID:        userID,
			AgentLineage:  lineage,
			AgentSpecHash: hash,
		})

		Expect(state).To(BeNil())
		Expect(errors.Is(err, domain.ErrAgentVersionNotFound)).To(BeTrue())
		Expect(uow.messages).To(BeEmpty())
	})

	It("imports golden tasks with service-computed fingerprints", func() {
		tasks, err := uc.ImportGoldenTasks(ctx, model.ImportGoldenTasksCommand{
			OrgID:        orgID,
			UserID:       userID,
			AgentLineage: lineage,
			Split:        model.GoldenTaskSplitSeedTrain,
			SplitVersion: 1,
			Tasks: []model.GoldenTaskInput{{
				GroupKey:               "contract-42",
				Prompt:                 "  Who signed   the Contract? ",
				ExpectedAnswer:         "Alice",
				ExpectedAnswerRubricID: "rubric-answer-v1",
			}},
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(tasks).To(HaveLen(1))
		Expect(repo.ensuredLineage).To(Equal(lineage))
		Expect(repo.createdGoldenTasks[0].NormalizedPromptHash).NotTo(BeEmpty())
		Expect(repo.createdGoldenTasks[0].ContentFingerprint).To(Equal(repo.createdGoldenTasks[0].NormalizedPromptHash))
		Expect(repo.createdGoldenTasks[0].CreatedByUserID).To(Equal(userID))
	})

	It("rejects golden tasks whose fingerprint or group overlaps another split", func() {
		repo.leakConflicts = []model.GoldenTaskLeakConflict{{
			TaskID:             uuid.New(),
			Split:              model.GoldenTaskSplitPromotionHoldout,
			GroupKey:           "contract-42",
			ContentFingerprint: "fingerprint",
		}}

		tasks, err := uc.ImportGoldenTasks(ctx, model.ImportGoldenTasksCommand{
			OrgID:        orgID,
			UserID:       userID,
			AgentLineage: lineage,
			Split:        model.GoldenTaskSplitSeedTrain,
			SplitVersion: 1,
			Tasks: []model.GoldenTaskInput{{
				GroupKey:               "contract-42",
				Prompt:                 "Who signed the contract?",
				ExpectedAnswer:         "Alice",
				ExpectedAnswerRubricID: "rubric-answer-v1",
			}},
		})

		Expect(tasks).To(BeEmpty())
		Expect(errors.Is(err, domain.ErrGoldenTaskLeak)).To(BeTrue())
		Expect(repo.createdGoldenTasks).To(BeEmpty())
	})

	It("lists golden tasks through the repository reader", func() {
		taskID := uuid.New()
		repo.listGoldenTasks = []*model.GoldenTask{{
			TaskID:                 taskID,
			OrgID:                  orgID,
			AgentLineage:           lineage,
			Split:                  model.GoldenTaskSplitPromotionHoldout,
			SplitVersion:           1,
			Prompt:                 "Who signed the contract?",
			ExpectedAnswer:         "Alice",
			ExpectedAnswerRubricID: "rubric-answer-v1",
		}}

		tasks, err := uc.ListGoldenTasks(ctx, model.ListGoldenTasksCommand{
			OrgID:        orgID,
			AgentLineage: lineage,
			Split:        model.GoldenTaskSplitPromotionHoldout,
			SplitVersion: 1,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(tasks).To(HaveLen(1))
		Expect(tasks[0].TaskID).To(Equal(taskID))
	})

	It("labels agent runs with the tuple read from inference", func() {
		runID := uuid.New()
		verifier.trajectory.RunID = runID

		label, err := uc.LabelAgentRun(ctx, model.LabelAgentRunCommand{
			OrgID:              orgID,
			UserID:             userID,
			RunID:              runID,
			AgentLineage:       lineage,
			Prompt:             "Who signed the contract?",
			Evaluator:          "human-reviewer",
			TaskSuccess:        true,
			ToolSelectionScore: 1,
			Groundedness:       1,
			Confidence:         0.95,
			LabelSource:        "human",
			RubricVersion:      "rubric-v1",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(label.LabelID).NotTo(Equal(uuid.Nil))
		Expect(verifier.readRunID).To(Equal(runID))
		Expect(repo.recordedLabel.AgentSpecHash).To(Equal(hash))
		Expect(repo.recordedLabel.ToolsetHash).To(Equal("sha256-toolset"))
		Expect(repo.recordedLabel.EffectiveBaseID).To(Equal("sha256-base"))
		Expect(repo.recordedLabel.DataSnapshotHash).To(Equal("sha256-data"))
		Expect(repo.recordedLabel.ContentFingerprint).NotTo(BeEmpty())
	})

	It("builds trajectory datasets from non-holdout labels only", func() {
		leakedFingerprint := "holdout-fingerprint"
		eligible := &model.AgentRunLabel{
			LabelID:            uuid.New(),
			RunID:              uuid.New(),
			OrgID:              orgID,
			AgentLineage:       lineage,
			AgentSpecHash:      hash,
			ToolsetHash:        "sha256-toolset",
			EffectiveBaseID:    "sha256-base",
			DataSnapshotHash:   "sha256-data",
			ContentFingerprint: "train-fingerprint",
			TaskSuccess:        true,
			ToolSelectionScore: 1,
			Groundedness:       1,
			Confidence:         0.95,
		}
		repo.labels = []*model.AgentRunLabel{
			eligible,
			{
				LabelID:            uuid.New(),
				RunID:              uuid.New(),
				OrgID:              orgID,
				AgentLineage:       lineage,
				AgentSpecHash:      hash,
				ToolsetHash:        "sha256-toolset",
				EffectiveBaseID:    "sha256-base",
				DataSnapshotHash:   "sha256-data",
				ContentFingerprint: leakedFingerprint,
			},
		}
		repo.listGoldenTasks = []*model.GoldenTask{{
			TaskID:             uuid.New(),
			OrgID:              orgID,
			AgentLineage:       lineage,
			Split:              model.GoldenTaskSplitPromotionHoldout,
			SplitVersion:       1,
			ContentFingerprint: leakedFingerprint,
		}}

		dataset, err := uc.BuildTrajectoryDataset(ctx, model.BuildTrajectoryDatasetCommand{
			OrgID:              orgID,
			UserID:             userID,
			AgentLineage:       lineage,
			GoldenSplitVersion: 1,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(dataset.LabelCount).To(Equal(1))
		Expect(dataset.ContentHash).NotTo(BeEmpty())
		Expect(dataset.DatasetURI).To(ContainSubstring(dataset.ContentHash))
		Expect(dataset.AgentSpecHash).To(Equal(hash))
		Expect(string(dataset.Manifest)).To(ContainSubstring(eligible.LabelID.String()))
		Expect(string(dataset.Manifest)).NotTo(ContainSubstring(leakedFingerprint))
	})

	It("rejects trajectory dataset builds when labels span incomparable tuples", func() {
		repo.labels = []*model.AgentRunLabel{
			{LabelID: uuid.New(), RunID: uuid.New(), OrgID: orgID, AgentLineage: lineage, AgentSpecHash: hash, ToolsetHash: "a", EffectiveBaseID: "base", DataSnapshotHash: "data", ContentFingerprint: "one"},
			{LabelID: uuid.New(), RunID: uuid.New(), OrgID: orgID, AgentLineage: lineage, AgentSpecHash: hash, ToolsetHash: "b", EffectiveBaseID: "base", DataSnapshotHash: "data", ContentFingerprint: "two"},
		}

		dataset, err := uc.BuildTrajectoryDataset(ctx, model.BuildTrajectoryDatasetCommand{
			OrgID:              orgID,
			UserID:             userID,
			AgentLineage:       lineage,
			GoldenSplitVersion: 1,
		})

		Expect(dataset).To(BeNil())
		Expect(errors.Is(err, domain.ErrAgentTrainingFailed)).To(BeTrue())
	})

	It("trains candidate adapters from a trajectory dataset tuple", func() {
		datasetID := uuid.New()
		repo.trajectoryDataset = &model.AgentTrajectoryDataset{
			DatasetID:          datasetID,
			OrgID:              orgID,
			AgentLineage:       lineage,
			GoldenSplitVersion: 1,
			ContentHash:        "sha256-dataset",
			DatasetURI:         "agent-registry://trajectory-datasets/sha256-dataset",
			EffectiveBaseID:    "sha256-base",
			AgentSpecHash:      hash,
			ToolsetHash:        "sha256-toolset",
			DataSnapshotHash:   "sha256-data",
		}
		repo.specVersion = &model.AgentSpecVersion{OrgID: orgID, AgentLineage: lineage, AgentSpecHash: hash, ModelID: modelID}

		adapter, err := uc.DispatchAgentAdapterTraining(ctx, model.DispatchAgentAdapterTrainingCommand{
			OrgID:        orgID,
			UserID:       userID,
			AgentLineage: lineage,
			DatasetID:    datasetID,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(adapter.Status).To(Equal(model.AgentAdapterStatusCandidate))
		Expect(adapter.ServingModelID).To(Equal(trainingDispatcher.result.ServingModelID))
		Expect(adapter.TrainedAgainstAgentSpecHash).To(Equal(hash))
		Expect(trainingDispatcher.request.DatasetURI).To(Equal(repo.trajectoryDataset.DatasetURI))
		Expect(repo.recordedAdapter.TrainedAgainstEffectiveBaseID).To(Equal("sha256-base"))
	})

	It("records completed agent adapter training as a candidate with real artifact identity", func() {
		trainingRunID := uuid.New()
		servingModelID := uuid.New()

		adapter, err := uc.RecordAgentAdapterTrainingCompleted(ctx, model.AgentAdapterTrainingCompletion{
			OrgID:            orgID,
			TrainingRunID:    trainingRunID,
			ServingModelID:   servingModelID,
			AdapterURI:       "s3://local-dev-bucket/agent-adapters/trained",
			AdapterChecksum:  "sha256-trained-adapter",
			TrainingProvider: "training-service",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(adapter.Status).To(Equal(model.AgentAdapterStatusCandidate))
		Expect(adapter.ServingModelID).To(Equal(servingModelID))
		Expect(adapter.AdapterURI).To(Equal("s3://local-dev-bucket/agent-adapters/trained"))
		Expect(adapter.AdapterChecksum).To(Equal("sha256-trained-adapter"))
		Expect(adapter.TrainingProvider).To(Equal("training-service"))
		Expect(repo.completedTraining.TrainingRunID).To(Equal(trainingRunID))
	})

	It("records failed agent adapter training without promoting or fabricating artifacts", func() {
		trainingRunID := uuid.New()

		adapter, err := uc.RecordAgentAdapterTrainingFailed(ctx, model.AgentAdapterTrainingFailure{
			OrgID:         orgID,
			TrainingRunID: trainingRunID,
			FailureReason: "ray job failed",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(adapter.Status).To(Equal(model.AgentAdapterStatusFailed))
		Expect(adapter.ServingModelID).To(Equal(uuid.Nil))
		Expect(adapter.AdapterURI).To(BeEmpty())
		Expect(adapter.AdapterChecksum).To(BeEmpty())
		Expect(repo.failedTraining.TrainingRunID).To(Equal(trainingRunID))
		Expect(repo.failedTraining.FailureReason).To(Equal("ray job failed"))
	})

	It("evaluates adapter candidates through the adapter serving model without promoting", func() {
		adapterID := uuid.New()
		servingModelID := uuid.New()
		taskID := uuid.New()
		repo.agentAdapter = &model.AgentAdapter{
			AdapterID:                        adapterID,
			OrgID:                            orgID,
			AgentLineage:                     lineage,
			ServingModelID:                   servingModelID,
			TrainedAgainstAgentSpecHash:      hash,
			TrainedAgainstRubricVersion:      "trajectory_answer_contains_v1",
			TrainedAgainstGoldenSplitVersion: 1,
			Status:                           model.AgentAdapterStatusCandidate,
		}
		repo.specVersion = &model.AgentSpecVersion{OrgID: orgID, AgentLineage: lineage, AgentSpecHash: hash, ModelID: modelID}
		repo.endpointBindings = []*model.AgentEndpointBinding{{OrgID: orgID, AgentLineage: lineage, EndpointID: endpointID, CreatedByUserID: userID}}
		repo.listGoldenTasks = []*model.GoldenTask{{TaskID: taskID, OrgID: orgID, AgentLineage: lineage, Split: model.GoldenTaskSplitPromotionHoldout, SplitVersion: 1, Prompt: "Who signed the contract?", ExpectedAnswer: "Alice", ExpectedAnswerRubricID: "rubric-answer-v1"}}
		runner.results = []model.AgentTaskRunResult{{
			RunID:                uuid.New(),
			Status:               "COMPLETED",
			StopReason:           "FINAL_ANSWER",
			Answer:               "Alice signed the contract.",
			GroundedContextCount: 1,
			GroundedContextTexts: []string{"Alice signed the contract for BigHill."},
			ToolInvocations:      []model.AgentTaskToolInvocation{{ToolName: "search_knowledge"}},
		}}

		report, err := uc.EvaluateAdapterCandidate(ctx, model.EvaluateAdapterCandidateCommand{
			OrgID:               orgID,
			UserID:              userID,
			AgentLineage:        lineage,
			AdapterID:           adapterID,
			EndpointID:          endpointID,
			SplitVersion:        1,
			MinGroundednessRate: 1,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(report.Passed).To(BeTrue())
		Expect(report.AdapterID).To(Equal(adapterID))
		Expect(report.PromotedDecisionID).To(Equal(uuid.Nil))
		Expect(runner.commands).To(HaveLen(1))
		Expect(runner.commands[0].ServingModelID).To(Equal(servingModelID))
		Expect(repo.agentAdapter.Status).To(Equal(model.AgentAdapterStatusEvaluated))
		Expect(uow.messages).To(BeEmpty())
	})

	It("promotes adapter candidates only with compatible passing reports and emits serving bindings", func() {
		adapterID := uuid.New()
		servingModelID := uuid.New()
		reportID := uuid.New()
		repo.agentAdapter = &model.AgentAdapter{
			AdapterID:                        adapterID,
			OrgID:                            orgID,
			AgentLineage:                     lineage,
			ServingModelID:                   servingModelID,
			TrainedAgainstAgentSpecHash:      hash,
			TrainedAgainstRubricVersion:      "trajectory_answer_contains_v1",
			TrainedAgainstGoldenSplitVersion: 1,
			Status:                           model.AgentAdapterStatusEvaluated,
		}
		repo.evalReport = &model.AgentEvalReport{
			ReportID:         reportID,
			OrgID:            orgID,
			AgentLineage:     lineage,
			AgentSpecHash:    hash,
			AdapterID:        adapterID,
			SplitVersion:     1,
			RubricVersion:    "trajectory_answer_contains_v1",
			TaskSuccessRate:  1,
			ToolSuccessRate:  1,
			GroundednessRate: 1,
			Passed:           true,
		}
		repo.endpointBindings = []*model.AgentEndpointBinding{{OrgID: orgID, AgentLineage: lineage, EndpointID: endpointID, CreatedByUserID: userID}}

		adapter, err := uc.PromoteAgentAdapter(ctx, model.PromoteAgentAdapterCommand{
			OrgID:        orgID,
			UserID:       userID,
			AgentLineage: lineage,
			AdapterID:    adapterID,
			ReportID:     reportID,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(adapter.Status).To(Equal(model.AgentAdapterStatusPromoted))
		Expect(adapter.PromotionPassed).To(BeTrue())
		Expect(repo.recordedChampionSpec).To(Equal(hash))
		Expect(uow.messages).To(HaveLen(1))
	})

	It("rejects adapter promotions that regress against the current champion report", func() {
		adapterID := uuid.New()
		championAdapterID := uuid.New()
		reportID := uuid.New()
		repo.agentAdapter = &model.AgentAdapter{
			AdapterID:                        adapterID,
			OrgID:                            orgID,
			AgentLineage:                     lineage,
			ServingModelID:                   uuid.New(),
			TrainedAgainstAgentSpecHash:      hash,
			TrainedAgainstRubricVersion:      "trajectory_answer_contains_v1",
			TrainedAgainstGoldenSplitVersion: 1,
			Status:                           model.AgentAdapterStatusEvaluated,
		}
		repo.evalReport = &model.AgentEvalReport{
			ReportID:         reportID,
			OrgID:            orgID,
			AgentLineage:     lineage,
			AgentSpecHash:    hash,
			AdapterID:        adapterID,
			SplitVersion:     1,
			RubricVersion:    "trajectory_answer_contains_v1",
			TaskSuccessRate:  0.8,
			ToolSuccessRate:  1,
			GroundednessRate: 1,
			Passed:           true,
		}
		repo.champion = &model.AgentChampionState{
			OrgID:             orgID,
			AgentLineage:      lineage,
			ChampionAdapterID: championAdapterID,
		}
		repo.championReport = &model.AgentEvalReport{
			ReportID:         uuid.New(),
			OrgID:            orgID,
			AgentLineage:     lineage,
			AgentSpecHash:    hash,
			AdapterID:        championAdapterID,
			SplitVersion:     1,
			RubricVersion:    "trajectory_answer_contains_v1",
			TaskSuccessRate:  0.95,
			ToolSuccessRate:  1,
			GroundednessRate: 1,
			Passed:           true,
		}

		adapter, err := uc.PromoteAgentAdapter(ctx, model.PromoteAgentAdapterCommand{
			OrgID:        orgID,
			UserID:       userID,
			AgentLineage: lineage,
			AdapterID:    adapterID,
			ReportID:     reportID,
		})

		Expect(adapter).To(BeNil())
		Expect(errors.Is(err, domain.ErrAgentPromotionFailed)).To(BeTrue())
		Expect(repo.recordedChampionSpec).To(BeEmpty())
		Expect(uow.messages).To(BeEmpty())
	})

	It("evaluates promotion holdout tasks and promotes a spec only after the report passes", func() {
		taskID := uuid.New()
		runID := uuid.New()
		repo.specVersion = &model.AgentSpecVersion{OrgID: orgID, AgentLineage: lineage, AgentSpecHash: hash, ModelID: modelID}
		repo.endpointBindings = []*model.AgentEndpointBinding{{OrgID: orgID, AgentLineage: lineage, EndpointID: endpointID, CreatedByUserID: userID}}
		repo.listGoldenTasks = []*model.GoldenTask{{
			TaskID:                 taskID,
			OrgID:                  orgID,
			AgentLineage:           lineage,
			Split:                  model.GoldenTaskSplitPromotionHoldout,
			SplitVersion:           1,
			Prompt:                 "Who signed the contract?",
			ExpectedAnswer:         "Alice",
			ExpectedAnswerRubricID: "rubric-answer-v1",
		}}
		runner.results = []model.AgentTaskRunResult{{
			RunID:                runID,
			Status:               "COMPLETED",
			StopReason:           "FINAL_ANSWER",
			Answer:               "Alice signed the contract.",
			GroundedContextCount: 1,
			GroundedContextTexts: []string{"Alice signed the contract for BigHill."},
			ToolInvocations: []model.AgentTaskToolInvocation{{
				ToolName: "search_knowledge",
			}},
		}}

		report, err := uc.EvaluateSpecChampion(ctx, model.EvaluateSpecChampionCommand{
			OrgID:               orgID,
			UserID:              userID,
			AgentLineage:        lineage,
			AgentSpecHash:       hash,
			EndpointID:          endpointID,
			SplitVersion:        1,
			MinGroundednessRate: 1,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(report.Passed).To(BeTrue())
		Expect(report.PromotedDecisionID).NotTo(Equal(uuid.Nil))
		Expect(repo.recordedEvalReport).NotTo(BeNil())
		Expect(repo.recordedEvalReport.TaskSuccessRate).To(Equal(1.0))
		Expect(repo.recordedEvalReport.ToolSuccessRate).To(Equal(1.0))
		Expect(repo.recordedEvalReport.GroundednessRate).To(Equal(1.0))
		Expect(repo.recordedChampionSpec).To(Equal(hash))
		Expect(runner.commands).To(HaveLen(1))
		Expect(runner.commands[0].AgentSpecHash).To(Equal(hash))
		Expect(runner.commands[0].TaskID).To(Equal(taskID))
	})

	It("does not pass promotion holdout tasks when the final answer misses the expected answer", func() {
		taskID := uuid.New()
		repo.specVersion = &model.AgentSpecVersion{OrgID: orgID, AgentLineage: lineage, AgentSpecHash: hash, ModelID: modelID}
		repo.endpointBindings = []*model.AgentEndpointBinding{{OrgID: orgID, AgentLineage: lineage, EndpointID: endpointID, CreatedByUserID: userID}}
		repo.listGoldenTasks = []*model.GoldenTask{{
			TaskID:                 taskID,
			OrgID:                  orgID,
			AgentLineage:           lineage,
			Split:                  model.GoldenTaskSplitPromotionHoldout,
			SplitVersion:           1,
			Prompt:                 "Who signed the contract?",
			ExpectedAnswer:         "Alice",
			ExpectedAnswerRubricID: "rubric-answer-v1",
		}}
		runner.results = []model.AgentTaskRunResult{{
			RunID:                uuid.New(),
			Status:               "COMPLETED",
			StopReason:           "FINAL_ANSWER",
			Answer:               "Bob signed the contract.",
			GroundedContextCount: 1,
			GroundedContextTexts: []string{"Alice signed the contract for BigHill."},
			ToolInvocations: []model.AgentTaskToolInvocation{{
				ToolName: "search_knowledge",
			}},
		}}

		report, err := uc.EvaluateSpecChampion(ctx, model.EvaluateSpecChampionCommand{
			OrgID:         orgID,
			UserID:        userID,
			AgentLineage:  lineage,
			AgentSpecHash: hash,
			EndpointID:    endpointID,
			SplitVersion:  1,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(report.Passed).To(BeFalse())
		Expect(report.TaskResults[0].TaskSuccess).To(BeFalse())
		Expect(report.TaskResults[0].FailureReason).To(ContainSubstring("answer did not match"))
		Expect(repo.recordedChampionSpec).To(BeEmpty())
	})

	It("does not pass promotion holdout tasks when retrieved context does not support the expected answer", func() {
		taskID := uuid.New()
		repo.specVersion = &model.AgentSpecVersion{OrgID: orgID, AgentLineage: lineage, AgentSpecHash: hash, ModelID: modelID}
		repo.endpointBindings = []*model.AgentEndpointBinding{{OrgID: orgID, AgentLineage: lineage, EndpointID: endpointID, CreatedByUserID: userID}}
		repo.listGoldenTasks = []*model.GoldenTask{{
			TaskID:                 taskID,
			OrgID:                  orgID,
			AgentLineage:           lineage,
			Split:                  model.GoldenTaskSplitPromotionHoldout,
			SplitVersion:           1,
			Prompt:                 "Who signed the contract?",
			ExpectedAnswer:         "Alice",
			ExpectedAnswerRubricID: "rubric-answer-v1",
		}}
		runner.results = []model.AgentTaskRunResult{{
			RunID:                uuid.New(),
			Status:               "COMPLETED",
			StopReason:           "FINAL_ANSWER",
			Answer:               "Alice signed the contract.",
			GroundedContextCount: 1,
			GroundedContextTexts: []string{"Bob sent the contract to legal."},
			ToolInvocations: []model.AgentTaskToolInvocation{{
				ToolName: "search_knowledge",
			}},
		}}

		report, err := uc.EvaluateSpecChampion(ctx, model.EvaluateSpecChampionCommand{
			OrgID:         orgID,
			UserID:        userID,
			AgentLineage:  lineage,
			AgentSpecHash: hash,
			EndpointID:    endpointID,
			SplitVersion:  1,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(report.Passed).To(BeFalse())
		Expect(report.TaskResults[0].TaskSuccess).To(BeTrue())
		Expect(report.TaskResults[0].Groundedness).To(BeFalse())
		Expect(report.TaskResults[0].FailureReason).To(ContainSubstring("no grounded context returned"))
		Expect(repo.recordedChampionSpec).To(BeEmpty())
	})

	It("records a failing eval report without promoting the spec", func() {
		taskID := uuid.New()
		repo.specVersion = &model.AgentSpecVersion{OrgID: orgID, AgentLineage: lineage, AgentSpecHash: hash, ModelID: modelID}
		repo.endpointBindings = []*model.AgentEndpointBinding{{OrgID: orgID, AgentLineage: lineage, EndpointID: endpointID, CreatedByUserID: userID}}
		repo.listGoldenTasks = []*model.GoldenTask{{
			TaskID:                 taskID,
			OrgID:                  orgID,
			AgentLineage:           lineage,
			Split:                  model.GoldenTaskSplitPromotionHoldout,
			SplitVersion:           1,
			Prompt:                 "Who signed the contract?",
			ExpectedAnswer:         "Alice",
			ExpectedAnswerRubricID: "rubric-answer-v1",
		}}
		runner.results = []model.AgentTaskRunResult{{
			RunID:      uuid.New(),
			Status:     "FAILED",
			StopReason: "TOOL_ERROR",
			ToolInvocations: []model.AgentTaskToolInvocation{{
				ToolName:  "search_knowledge",
				ErrorType: "PERMANENT",
			}},
		}}

		report, err := uc.EvaluateSpecChampion(ctx, model.EvaluateSpecChampionCommand{
			OrgID:         orgID,
			UserID:        userID,
			AgentLineage:  lineage,
			AgentSpecHash: hash,
			EndpointID:    endpointID,
			SplitVersion:  1,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(report.Passed).To(BeFalse())
		Expect(report.PromotedDecisionID).To(Equal(uuid.Nil))
		Expect(repo.recordedEvalReport.TaskSuccessRate).To(Equal(0.0))
		Expect(repo.recordedChampionSpec).To(BeEmpty())
		Expect(uow.messages).To(BeEmpty())
	})

	It("records runner failures as failed task results without fabricating run ids", func() {
		taskID := uuid.New()
		repo.specVersion = &model.AgentSpecVersion{OrgID: orgID, AgentLineage: lineage, AgentSpecHash: hash, ModelID: modelID}
		repo.endpointBindings = []*model.AgentEndpointBinding{{OrgID: orgID, AgentLineage: lineage, EndpointID: endpointID, CreatedByUserID: userID}}
		repo.listGoldenTasks = []*model.GoldenTask{{
			TaskID:                 taskID,
			OrgID:                  orgID,
			AgentLineage:           lineage,
			Split:                  model.GoldenTaskSplitPromotionHoldout,
			SplitVersion:           1,
			Prompt:                 "Who signed the contract?",
			ExpectedAnswer:         "Alice",
			ExpectedAnswerRubricID: "rubric-answer-v1",
		}}
		runner.errs = []error{errors.New("agent workflow failed before run creation")}

		report, err := uc.EvaluateSpecChampion(ctx, model.EvaluateSpecChampionCommand{
			OrgID:         orgID,
			UserID:        userID,
			AgentLineage:  lineage,
			AgentSpecHash: hash,
			EndpointID:    endpointID,
			SplitVersion:  1,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(report.Passed).To(BeFalse())
		Expect(report.TaskResults).To(HaveLen(1))
		Expect(report.TaskResults[0].RunID).To(Equal(uuid.Nil))
		Expect(report.TaskResults[0].Status).To(Equal("FAILED"))
		Expect(report.TaskResults[0].StopReason).To(Equal("RUNTIME_ERROR"))
		Expect(report.TaskResults[0].FailureReason).To(ContainSubstring("agent workflow failed"))
		Expect(repo.recordedChampionSpec).To(BeEmpty())
		Expect(uow.messages).To(BeEmpty())
	})
})

type agentTaskRunnerStub struct {
	results  []model.AgentTaskRunResult
	errs     []error
	commands []model.AgentTaskRunCommand
}

func (s *agentTaskRunnerStub) RunAgentTask(_ context.Context, command model.AgentTaskRunCommand) (model.AgentTaskRunResult, error) {
	s.commands = append(s.commands, command)
	if len(s.errs) > 0 {
		err := s.errs[0]
		s.errs = s.errs[1:]
		if len(s.results) == 0 {
			return model.AgentTaskRunResult{}, err
		}
		result := s.results[0]
		s.results = s.results[1:]
		return result, err
	}
	if len(s.results) == 0 {
		return model.AgentTaskRunResult{}, nil
	}
	result := s.results[0]
	s.results = s.results[1:]
	return result, nil
}

type agentAdapterTrainingDispatcherStub struct {
	request model.AgentAdapterTrainingRequest
	result  *model.AgentAdapterTrainingResult
	err     error
}

func (s *agentAdapterTrainingDispatcherStub) DispatchAgentAdapterTraining(_ context.Context, request model.AgentAdapterTrainingRequest) (*model.AgentAdapterTrainingResult, error) {
	s.request = request
	return s.result, s.err
}
