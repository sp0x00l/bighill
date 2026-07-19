package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"agent_registry_service/pkg/domain"
	"agent_registry_service/pkg/domain/model"
	serializers "lib/shared_lib/serializer"
	shareduow "lib/shared_lib/uow"
	"lib/shared_lib/userevents"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	log "github.com/sirupsen/logrus"
)

const (
	agentEvalRubricVersion = "trajectory_answer_contains_v1"

	agentEvalStatusCompleted  = "COMPLETED"
	agentEvalStatusFailed     = "FAILED"
	agentEvalStopFinalAnswer  = "FINAL_ANSWER"
	agentEvalStopRuntimeError = "RUNTIME_ERROR"

	defaultMinTaskSuccessRate  = 1
	defaultMinToolSuccessRate  = 1
	defaultMinGroundednessRate = 1

	trajectoryDatasetFormat     = "AGENT_TRAJECTORY_JSON"
	trajectoryDatasetSchema     = "agent_trajectory_dataset_v1"
	trajectoryDatasetURIPrefix  = "agent-registry://trajectory-datasets/"
	defaultAgentTrainingProfile = "agent-sft-dpo-fast@v1"
)

type AgentRegistryUsecase interface {
	RegisterAgentSpecVersion(ctx context.Context, command model.RegisterAgentSpecVersionCommand) (*model.AgentSpecVersion, error)
	RegisterEndpointBinding(ctx context.Context, command model.RegisterEndpointBindingCommand) (*model.AgentEndpointBinding, error)
	PromoteSpecChampion(ctx context.Context, command model.PromoteSpecChampionCommand) (*model.AgentChampionState, error)
	ImportGoldenTasks(ctx context.Context, command model.ImportGoldenTasksCommand) ([]*model.GoldenTask, error)
	ListGoldenTasks(ctx context.Context, command model.ListGoldenTasksCommand) ([]*model.GoldenTask, error)
	LabelAgentRun(ctx context.Context, command model.LabelAgentRunCommand) (*model.AgentRunLabel, error)
	ListAgentRunLabels(ctx context.Context, command model.ListAgentRunLabelsCommand) ([]*model.AgentRunLabel, error)
	BuildTrajectoryDataset(ctx context.Context, command model.BuildTrajectoryDatasetCommand) (*model.AgentTrajectoryDataset, error)
	ReadTrajectoryDataset(ctx context.Context, orgID uuid.UUID, datasetID uuid.UUID) (*model.AgentTrajectoryDataset, error)
	DispatchAgentAdapterTraining(ctx context.Context, command model.DispatchAgentAdapterTrainingCommand) (*model.AgentAdapter, error)
	RecordAgentAdapterTrainingCompleted(ctx context.Context, completion model.AgentAdapterTrainingCompletion) (*model.AgentAdapter, error)
	RecordAgentAdapterTrainingFailed(ctx context.Context, failure model.AgentAdapterTrainingFailure) (*model.AgentAdapter, error)
	ReadAgentAdapter(ctx context.Context, orgID uuid.UUID, adapterID uuid.UUID) (*model.AgentAdapter, error)
	EvaluateAdapterCandidate(ctx context.Context, command model.EvaluateAdapterCandidateCommand) (*model.AgentEvalReport, error)
	PromoteAgentAdapter(ctx context.Context, command model.PromoteAgentAdapterCommand) (*model.AgentAdapter, error)
	EvaluateSpecChampion(ctx context.Context, command model.EvaluateSpecChampionCommand) (*model.AgentEvalReport, error)
	ReadAgentEvalReport(ctx context.Context, orgID uuid.UUID, reportID uuid.UUID) (*model.AgentEvalReport, error)
}

type agentRegistryUsecase struct {
	repository         AgentRegistryRepository
	unitOfWork         AgentRegistryUnitOfWork
	inferenceVerifier  InferenceVerifier
	eventBuilder       AgentRegistryEventBuilder
	taskRunner         AgentTaskRunner
	trainingDispatcher AgentAdapterTrainingDispatcher
}

func NewAgentRegistryUsecase(repository AgentRegistryRepository, unitOfWork AgentRegistryUnitOfWork, inferenceVerifier InferenceVerifier, eventBuilder AgentRegistryEventBuilder, taskRunner AgentTaskRunner, trainingDispatcher AgentAdapterTrainingDispatcher) AgentRegistryUsecase {
	log.Trace("NewAgentRegistryUsecase")

	return &agentRegistryUsecase{
		repository:         repository,
		unitOfWork:         unitOfWork,
		inferenceVerifier:  inferenceVerifier,
		eventBuilder:       eventBuilder,
		taskRunner:         taskRunner,
		trainingDispatcher: trainingDispatcher,
	}
}

func (u *agentRegistryUsecase) RegisterAgentSpecVersion(ctx context.Context, command model.RegisterAgentSpecVersionCommand) (*model.AgentSpecVersion, error) {
	log.Trace("agentRegistryUsecase RegisterAgentSpecVersion")

	spec, err := u.inferenceVerifier.ReadAgentSpec(ctx, command.OrgID, command.UserID, command.AgentSpecHash)
	if err != nil {
		return nil, err
	}
	if spec.AgentLineage != command.AgentLineage {
		return nil, domain.ErrAgentRegistryValidation.Extend("agent spec lineage does not match registry lineage")
	}
	version := &model.AgentSpecVersion{
		OrgID:              command.OrgID,
		AgentLineage:       command.AgentLineage,
		AgentSpecHash:      command.AgentSpecHash,
		ModelID:            spec.ModelID,
		RegisteredByUserID: command.UserID,
	}
	var out *model.AgentSpecVersion
	err = u.unitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx, _ shareduow.EnqueueFunc) error {
		if err := u.repository.EnsureLineage(ctx, tx, command.OrgID, command.AgentLineage, command.UserID); err != nil {
			return err
		}
		record, err := u.repository.UpsertAgentSpecVersion(ctx, tx, version)
		if err != nil {
			return err
		}
		out = record
		return nil
	})
	return out, err
}

func (u *agentRegistryUsecase) RegisterEndpointBinding(ctx context.Context, command model.RegisterEndpointBindingCommand) (*model.AgentEndpointBinding, error) {
	log.Trace("agentRegistryUsecase RegisterEndpointBinding")

	if _, err := u.inferenceVerifier.ReadEndpoint(ctx, command.OrgID, command.UserID, command.EndpointID); err != nil {
		return nil, err
	}
	binding := &model.AgentEndpointBinding{
		OrgID:           command.OrgID,
		AgentLineage:    command.AgentLineage,
		EndpointID:      command.EndpointID,
		CreatedByUserID: command.UserID,
	}
	var out *model.AgentEndpointBinding
	err := u.unitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx, _ shareduow.EnqueueFunc) error {
		if err := u.repository.EnsureLineage(ctx, tx, command.OrgID, command.AgentLineage, command.UserID); err != nil {
			return err
		}
		record, err := u.repository.UpsertEndpointBinding(ctx, tx, binding)
		if err != nil {
			return err
		}
		out = record
		return nil
	})
	return out, err
}

func (u *agentRegistryUsecase) PromoteSpecChampion(ctx context.Context, command model.PromoteSpecChampionCommand) (*model.AgentChampionState, error) {
	log.Trace("agentRegistryUsecase PromoteSpecChampion")

	version, err := u.repository.ReadSpecVersion(ctx, command.OrgID, command.AgentSpecHash)
	if err != nil {
		return nil, err
	}
	if version.AgentLineage != command.AgentLineage {
		return nil, domain.ErrAgentRegistryValidation.Extend("registered agent spec lineage does not match champion lineage")
	}
	bindings, err := u.repository.ListEndpointBindings(ctx, command.OrgID, command.AgentLineage)
	if err != nil {
		return nil, err
	}
	state := agentChampionStateFromCommand(command)
	err = u.unitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx, enqueue shareduow.EnqueueFunc) error {
		var err error
		state, err = u.recordChampionStateAndEvents(ctx, tx, enqueue, state, bindings)
		return err
	})
	return state, err
}

func (u *agentRegistryUsecase) ImportGoldenTasks(ctx context.Context, command model.ImportGoldenTasksCommand) ([]*model.GoldenTask, error) {
	log.Trace("agentRegistryUsecase ImportGoldenTasks")

	out := []*model.GoldenTask{}
	err := u.unitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx, _ shareduow.EnqueueFunc) error {
		if err := u.repository.EnsureLineage(ctx, tx, command.OrgID, command.AgentLineage, command.UserID); err != nil {
			return err
		}
		for _, input := range command.Tasks {
			normalizedPrompt := normalizeGoldenPrompt(input.Prompt)
			task := &model.GoldenTask{
				OrgID:                    command.OrgID,
				AgentLineage:             command.AgentLineage,
				Split:                    command.Split,
				SplitVersion:             command.SplitVersion,
				GroupKey:                 strings.TrimSpace(input.GroupKey),
				Prompt:                   input.Prompt,
				NormalizedPromptHash:     userevents.SHA256String(normalizedPrompt),
				ContentFingerprint:       goldenTaskContentFingerprint(normalizedPrompt),
				NearDuplicateFingerprint: goldenTaskNearDuplicateFingerprint(normalizedPrompt),
				ExpectedToolPlanHash:     strings.TrimSpace(input.ExpectedToolPlanHash),
				ExpectedAnswer:           strings.TrimSpace(input.ExpectedAnswer),
				ExpectedAnswerRubricID:   strings.TrimSpace(input.ExpectedAnswerRubricID),
				LabelsHash:               strings.TrimSpace(input.LabelsHash),
				CreatedByUserID:          command.UserID,
			}
			conflicts, err := u.repository.FindGoldenTaskLeakConflicts(ctx, tx, task)
			if err != nil {
				return err
			}
			if len(conflicts) > 0 {
				return domain.ErrGoldenTaskLeak.Extend(fmt.Sprintf("task fingerprint or group_key overlaps split %s", conflicts[0].Split.String()))
			}
			record, err := u.repository.CreateGoldenTask(ctx, tx, task)
			if err != nil {
				return err
			}
			out = append(out, record)
		}
		return nil
	})
	return out, err
}

func (u *agentRegistryUsecase) ListGoldenTasks(ctx context.Context, command model.ListGoldenTasksCommand) ([]*model.GoldenTask, error) {
	log.Trace("agentRegistryUsecase ListGoldenTasks")

	return u.repository.ListGoldenTasks(ctx, command)
}

func (u *agentRegistryUsecase) LabelAgentRun(ctx context.Context, command model.LabelAgentRunCommand) (*model.AgentRunLabel, error) {
	log.Trace("agentRegistryUsecase LabelAgentRun")

	trajectory, err := u.inferenceVerifier.ReadAgentTrajectory(ctx, command.OrgID, command.UserID, command.RunID)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(trajectory.AgentSpecHash) == "" || strings.TrimSpace(trajectory.ToolsetHash) == "" ||
		strings.TrimSpace(trajectory.EffectiveBaseID) == "" || strings.TrimSpace(trajectory.DataSnapshotHash) == "" {
		return nil, domain.ErrAgentRegistryValidation.Extend("agent trajectory tuple is incomplete")
	}
	normalizedPrompt := normalizeGoldenPrompt(command.Prompt)
	label := &model.AgentRunLabel{
		OrgID:                    command.OrgID,
		RunID:                    command.RunID,
		AgentLineage:             command.AgentLineage,
		AgentSpecHash:            trajectory.AgentSpecHash,
		ToolsetHash:              trajectory.ToolsetHash,
		EffectiveBaseID:          trajectory.EffectiveBaseID,
		DataSnapshotHash:         trajectory.DataSnapshotHash,
		ContentFingerprint:       goldenTaskContentFingerprint(normalizedPrompt),
		NearDuplicateFingerprint: goldenTaskNearDuplicateFingerprint(normalizedPrompt),
		Evaluator:                command.Evaluator,
		TaskSuccess:              command.TaskSuccess,
		ToolSelectionScore:       command.ToolSelectionScore,
		Groundedness:             command.Groundedness,
		PolicyViolations:         command.PolicyViolations,
		Confidence:               command.Confidence,
		LabelSource:              command.LabelSource,
		RubricVersion:            command.RubricVersion,
		CreatedByUserID:          command.UserID,
	}
	var out *model.AgentRunLabel
	err = u.unitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx, _ shareduow.EnqueueFunc) error {
		if err := u.repository.EnsureLineage(ctx, tx, command.OrgID, command.AgentLineage, command.UserID); err != nil {
			return err
		}
		record, err := u.repository.RecordAgentRunLabel(ctx, tx, label)
		if err != nil {
			return err
		}
		out = record
		return nil
	})
	return out, err
}

func (u *agentRegistryUsecase) ListAgentRunLabels(ctx context.Context, command model.ListAgentRunLabelsCommand) ([]*model.AgentRunLabel, error) {
	log.Trace("agentRegistryUsecase ListAgentRunLabels")

	return u.repository.ListAgentRunLabels(ctx, command)
}

func (u *agentRegistryUsecase) BuildTrajectoryDataset(ctx context.Context, command model.BuildTrajectoryDatasetCommand) (*model.AgentTrajectoryDataset, error) {
	log.Trace("agentRegistryUsecase BuildTrajectoryDataset")

	labels, err := u.repository.ListAgentRunLabels(ctx, model.ListAgentRunLabelsCommand{
		OrgID:        command.OrgID,
		AgentLineage: command.AgentLineage,
	})
	if err != nil {
		return nil, err
	}
	holdout, err := u.repository.ListGoldenTasks(ctx, model.ListGoldenTasksCommand{
		OrgID:        command.OrgID,
		AgentLineage: command.AgentLineage,
		Split:        model.GoldenTaskSplitPromotionHoldout,
		SplitVersion: command.GoldenSplitVersion,
	})
	if err != nil {
		return nil, err
	}
	filtered := filterTrainingLabels(labels, holdout)
	if len(filtered) == 0 {
		return nil, domain.ErrAgentTrainingFailed.Extend("no non-holdout labels are eligible for training")
	}
	tuple, err := commonTrainingTuple(filtered)
	if err != nil {
		return nil, err
	}
	manifest, contentHash, err := trajectoryDatasetManifest(filtered, command.GoldenSplitVersion)
	if err != nil {
		return nil, err
	}
	dataset := &model.AgentTrajectoryDataset{
		OrgID:              command.OrgID,
		AgentLineage:       command.AgentLineage,
		GoldenSplitVersion: command.GoldenSplitVersion,
		ContentHash:        contentHash,
		DatasetURI:         trajectoryDatasetURIPrefix + contentHash,
		Format:             trajectoryDatasetFormat,
		LabelCount:         len(filtered),
		Manifest:           manifest,
		EffectiveBaseID:    tuple.EffectiveBaseID,
		AgentSpecHash:      tuple.AgentSpecHash,
		ToolsetHash:        tuple.ToolsetHash,
		DataSnapshotHash:   tuple.DataSnapshotHash,
		CreatedByUserID:    command.UserID,
	}
	var out *model.AgentTrajectoryDataset
	err = u.unitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx, _ shareduow.EnqueueFunc) error {
		if err := u.repository.EnsureLineage(ctx, tx, command.OrgID, command.AgentLineage, command.UserID); err != nil {
			return err
		}
		record, err := u.repository.RecordTrajectoryDataset(ctx, tx, dataset)
		if err != nil {
			return err
		}
		out = record
		return nil
	})
	return out, err
}

func (u *agentRegistryUsecase) ReadTrajectoryDataset(ctx context.Context, orgID uuid.UUID, datasetID uuid.UUID) (*model.AgentTrajectoryDataset, error) {
	log.Trace("agentRegistryUsecase ReadTrajectoryDataset")

	return u.repository.ReadTrajectoryDataset(ctx, orgID, datasetID)
}

func (u *agentRegistryUsecase) DispatchAgentAdapterTraining(ctx context.Context, command model.DispatchAgentAdapterTrainingCommand) (*model.AgentAdapter, error) {
	log.Trace("agentRegistryUsecase DispatchAgentAdapterTraining")

	if u.trainingDispatcher == nil {
		return nil, domain.ErrAgentTrainingFailed.Extend("agent training dispatcher is not configured")
	}
	dataset, err := u.repository.ReadTrajectoryDataset(ctx, command.OrgID, command.DatasetID)
	if err != nil {
		return nil, err
	}
	if dataset.AgentLineage != command.AgentLineage {
		return nil, domain.ErrAgentRegistryValidation.Extend("trajectory dataset lineage does not match adapter lineage")
	}
	version, err := u.repository.ReadSpecVersion(ctx, command.OrgID, dataset.AgentSpecHash)
	if err != nil {
		return nil, err
	}
	if version.AgentLineage != command.AgentLineage {
		return nil, domain.ErrAgentRegistryValidation.Extend("trajectory dataset spec lineage does not match adapter lineage")
	}
	profile := strings.TrimSpace(command.TrainingProfile)
	if profile == "" {
		profile = defaultAgentTrainingProfile
	}
	training, err := u.trainingDispatcher.DispatchAgentAdapterTraining(ctx, model.AgentAdapterTrainingRequest{
		OrgID:            command.OrgID,
		UserID:           command.UserID,
		AgentLineage:     command.AgentLineage,
		DatasetID:        dataset.DatasetID,
		DatasetURI:       dataset.DatasetURI,
		ContentHash:      dataset.ContentHash,
		SourceModelID:    version.ModelID,
		TrainingProfile:  profile,
		EffectiveBaseID:  dataset.EffectiveBaseID,
		AgentSpecHash:    dataset.AgentSpecHash,
		ToolsetHash:      dataset.ToolsetHash,
		DataSnapshotHash: dataset.DataSnapshotHash,
	})
	if err != nil {
		return nil, err
	}
	if training.TrainingRunID == uuid.Nil || strings.TrimSpace(training.TrainingProvider) == "" {
		return nil, domain.ErrAgentTrainingFailed.Extend("agent training provider returned an incomplete training dispatch")
	}
	status := model.AgentAdapterStatusTraining
	if strings.TrimSpace(training.AdapterURI) != "" || strings.TrimSpace(training.AdapterChecksum) != "" {
		if training.ServingModelID == uuid.Nil || strings.TrimSpace(training.AdapterURI) == "" || strings.TrimSpace(training.AdapterChecksum) == "" {
			return nil, domain.ErrAgentTrainingFailed.Extend("agent training provider returned an incomplete adapter artifact")
		}
		status = model.AgentAdapterStatusCandidate
	}
	adapter := &model.AgentAdapter{
		OrgID:                            command.OrgID,
		AgentLineage:                     command.AgentLineage,
		DatasetID:                        dataset.DatasetID,
		TrainingRunID:                    training.TrainingRunID,
		ServingModelID:                   training.ServingModelID,
		AdapterURI:                       training.AdapterURI,
		AdapterChecksum:                  training.AdapterChecksum,
		TrainingProvider:                 training.TrainingProvider,
		TrainedAgainstEffectiveBaseID:    dataset.EffectiveBaseID,
		TrainedAgainstAgentSpecHash:      dataset.AgentSpecHash,
		TrainedAgainstToolsetHash:        dataset.ToolsetHash,
		TrainedAgainstDataSnapshotHash:   dataset.DataSnapshotHash,
		TrainedAgainstRubricVersion:      agentEvalRubricVersion,
		TrainedAgainstGoldenSplitVersion: dataset.GoldenSplitVersion,
		Status:                           status,
		CreatedByUserID:                  command.UserID,
	}
	var out *model.AgentAdapter
	err = u.unitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx, _ shareduow.EnqueueFunc) error {
		record, err := u.repository.RecordAgentAdapter(ctx, tx, adapter)
		if err != nil {
			return err
		}
		out = record
		return nil
	})
	return out, err
}

func (u *agentRegistryUsecase) RecordAgentAdapterTrainingCompleted(ctx context.Context, completion model.AgentAdapterTrainingCompletion) (*model.AgentAdapter, error) {
	log.Trace("agentRegistryUsecase RecordAgentAdapterTrainingCompleted")

	if completion.TrainingRunID == uuid.Nil || completion.OrgID == uuid.Nil || completion.ServingModelID == uuid.Nil ||
		strings.TrimSpace(completion.AdapterURI) == "" || strings.TrimSpace(completion.AdapterChecksum) == "" ||
		strings.TrimSpace(completion.TrainingProvider) == "" {
		return nil, domain.ErrAgentTrainingFailed.Extend("agent adapter training completion is incomplete")
	}
	var out *model.AgentAdapter
	err := u.unitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx, _ shareduow.EnqueueFunc) error {
		record, err := u.repository.CompleteAgentAdapterTraining(ctx, tx, completion)
		if err != nil {
			return err
		}
		out = record
		return nil
	})
	return out, err
}

func (u *agentRegistryUsecase) RecordAgentAdapterTrainingFailed(ctx context.Context, failure model.AgentAdapterTrainingFailure) (*model.AgentAdapter, error) {
	log.Trace("agentRegistryUsecase RecordAgentAdapterTrainingFailed")

	if failure.TrainingRunID == uuid.Nil || failure.OrgID == uuid.Nil {
		return nil, domain.ErrAgentTrainingFailed.Extend("agent adapter training failure is incomplete")
	}
	var out *model.AgentAdapter
	err := u.unitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx, _ shareduow.EnqueueFunc) error {
		record, err := u.repository.FailAgentAdapterTraining(ctx, tx, failure)
		if err != nil {
			return err
		}
		out = record
		return nil
	})
	return out, err
}

func (u *agentRegistryUsecase) ReadAgentAdapter(ctx context.Context, orgID uuid.UUID, adapterID uuid.UUID) (*model.AgentAdapter, error) {
	log.Trace("agentRegistryUsecase ReadAgentAdapter")

	return u.repository.ReadAgentAdapter(ctx, orgID, adapterID)
}

func (u *agentRegistryUsecase) EvaluateAdapterCandidate(ctx context.Context, command model.EvaluateAdapterCandidateCommand) (*model.AgentEvalReport, error) {
	log.Trace("agentRegistryUsecase EvaluateAdapterCandidate")

	adapter, err := u.repository.ReadAgentAdapter(ctx, command.OrgID, command.AdapterID)
	if err != nil {
		return nil, err
	}
	if adapter.AgentLineage != command.AgentLineage {
		return nil, domain.ErrAgentRegistryValidation.Extend("adapter lineage does not match eval lineage")
	}
	if adapter.Status != model.AgentAdapterStatusCandidate && adapter.Status != model.AgentAdapterStatusEvaluated {
		return nil, domain.ErrAgentEvalFailed.Extend("adapter must be trained before evaluation")
	}
	report, err := u.evaluateCandidate(ctx, model.EvaluateSpecChampionCommand{
		OrgID:               command.OrgID,
		UserID:              command.UserID,
		AgentLineage:        command.AgentLineage,
		AgentSpecHash:       adapter.TrainedAgainstAgentSpecHash,
		AdapterID:           adapter.AdapterID,
		EndpointID:          command.EndpointID,
		SplitVersion:        command.SplitVersion,
		MinTaskSuccessRate:  command.MinTaskSuccessRate,
		MinToolSuccessRate:  command.MinToolSuccessRate,
		MinGroundednessRate: command.MinGroundednessRate,
	}, adapter.ServingModelID, false)
	if err != nil {
		return nil, err
	}
	status := model.AgentAdapterStatusRejected
	if report.Passed {
		status = model.AgentAdapterStatusEvaluated
	}
	err = u.unitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx, _ shareduow.EnqueueFunc) error {
		_, err := u.repository.UpdateAgentAdapterPromotion(ctx, tx, adapter.AdapterID, status, false)
		return err
	})
	if err != nil {
		return nil, err
	}
	return report, nil
}

func (u *agentRegistryUsecase) PromoteAgentAdapter(ctx context.Context, command model.PromoteAgentAdapterCommand) (*model.AgentAdapter, error) {
	log.Trace("agentRegistryUsecase PromoteAgentAdapter")

	adapter, err := u.repository.ReadAgentAdapter(ctx, command.OrgID, command.AdapterID)
	if err != nil {
		return nil, err
	}
	if adapter.Status != model.AgentAdapterStatusEvaluated {
		return nil, domain.ErrAgentPromotionFailed.Extend("adapter must pass evaluation before promotion")
	}
	report, err := u.repository.ReadAgentEvalReport(ctx, command.OrgID, command.ReportID)
	if err != nil {
		return nil, err
	}
	if !agentAdapterCompatibleWithReport(adapter, report) {
		return nil, domain.ErrAgentPromotionFailed.Extend("adapter and eval report tuples are incompatible")
	}
	if !report.Passed {
		return nil, domain.ErrAgentPromotionFailed.Extend("adapter eval report did not pass")
	}
	if err := u.enforceChampionNoRegression(ctx, adapter, report, command.MinDelta); err != nil {
		return nil, err
	}
	bindings, err := u.repository.ListEndpointBindings(ctx, command.OrgID, command.AgentLineage)
	if err != nil {
		return nil, err
	}
	var out *model.AgentAdapter
	err = u.unitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx, enqueue shareduow.EnqueueFunc) error {
		record, err := u.repository.UpdateAgentAdapterPromotion(ctx, tx, adapter.AdapterID, model.AgentAdapterStatusPromoted, true)
		if err != nil {
			return err
		}
		out = record
		state := &model.AgentChampionState{
			OrgID:                 command.OrgID,
			AgentLineage:          command.AgentLineage,
			ChampionAgentSpecHash: adapter.TrainedAgainstAgentSpecHash,
			ChampionAdapterID:     adapter.AdapterID,
			ServingModelID:        adapter.ServingModelID,
			DecisionID:            uuid.New(),
			DecidedBy:             command.UserID,
			DecidedAt:             time.Now().UTC(),
		}
		_, err = u.recordChampionStateAndEvents(ctx, tx, enqueue, state, bindings)
		return err
	})
	return out, err
}

func (u *agentRegistryUsecase) EvaluateSpecChampion(ctx context.Context, command model.EvaluateSpecChampionCommand) (*model.AgentEvalReport, error) {
	log.Trace("agentRegistryUsecase EvaluateSpecChampion")

	return u.evaluateCandidate(ctx, command, uuid.Nil, true)
}

func (u *agentRegistryUsecase) evaluateCandidate(ctx context.Context, command model.EvaluateSpecChampionCommand, servingModelID uuid.UUID, autoPromote bool) (*model.AgentEvalReport, error) {
	log.Trace("agentRegistryUsecase evaluateCandidate")

	version, err := u.repository.ReadSpecVersion(ctx, command.OrgID, command.AgentSpecHash)
	if err != nil {
		return nil, err
	}
	if version.AgentLineage != command.AgentLineage {
		return nil, domain.ErrAgentRegistryValidation.Extend("registered agent spec lineage does not match eval lineage")
	}
	bindings, err := u.repository.ListEndpointBindings(ctx, command.OrgID, command.AgentLineage)
	if err != nil {
		return nil, err
	}
	if !agentEndpointBound(bindings, command.EndpointID) {
		return nil, domain.ErrEndpointUnavailable.Extend("endpoint is not bound to agent lineage")
	}
	tasks, err := u.repository.ListGoldenTasks(ctx, model.ListGoldenTasksCommand{
		OrgID:        command.OrgID,
		AgentLineage: command.AgentLineage,
		Split:        model.GoldenTaskSplitPromotionHoldout,
		SplitVersion: command.SplitVersion,
	})
	if err != nil {
		return nil, err
	}
	if len(tasks) == 0 {
		return nil, domain.ErrAgentEvalFailed.Extend("promotion holdout has no golden tasks")
	}
	taskResults := make([]*model.AgentEvalTaskResult, 0, len(tasks))
	for _, task := range tasks {
		run, err := u.taskRunner.RunAgentTask(ctx, model.AgentTaskRunCommand{
			OrgID:          command.OrgID,
			UserID:         command.UserID,
			EndpointID:     command.EndpointID,
			AgentSpecHash:  command.AgentSpecHash,
			ServingModelID: servingModelID,
			TaskID:         task.TaskID,
			QueryText:      task.Prompt,
		})
		taskResults = append(taskResults, scoreGoldenTaskRun(task, run, err))
	}
	report := agentEvalReportFromTaskResults(command, taskResults)
	err = u.unitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx, enqueue shareduow.EnqueueFunc) error {
		if report.Passed && autoPromote {
			report.PromotedDecisionID = uuid.New()
		}
		record, err := u.repository.RecordAgentEvalReport(ctx, tx, report)
		if err != nil {
			return err
		}
		report = record
		if report.Passed && autoPromote {
			state := &model.AgentChampionState{
				OrgID:                 command.OrgID,
				AgentLineage:          command.AgentLineage,
				ChampionAgentSpecHash: command.AgentSpecHash,
				DecisionID:            report.PromotedDecisionID,
				DecidedBy:             command.UserID,
				DecidedAt:             report.EvaluatedAt,
			}
			_, err := u.recordChampionStateAndEvents(ctx, tx, enqueue, state, bindings)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return report, nil
}

func (u *agentRegistryUsecase) recordChampionStateAndEvents(ctx context.Context, tx pgx.Tx, enqueue shareduow.EnqueueFunc, state *model.AgentChampionState, bindings []*model.AgentEndpointBinding) (*model.AgentChampionState, error) {
	record, err := u.repository.RecordChampionState(ctx, tx, state)
	if err != nil {
		return nil, err
	}
	for _, binding := range bindings {
		if err := enqueue(u.eventBuilder.AgentChampionUpdatedMessage(record, binding)); err != nil {
			return nil, err
		}
	}
	return record, nil
}

func agentChampionStateFromCommand(command model.PromoteSpecChampionCommand) *model.AgentChampionState {
	decidedAt := command.DecidedAt
	if decidedAt.IsZero() {
		decidedAt = time.Now().UTC()
	}
	decisionID := command.DecisionID
	if decisionID == uuid.Nil {
		decisionID = uuid.New()
	}
	return &model.AgentChampionState{
		OrgID:                 command.OrgID,
		AgentLineage:          command.AgentLineage,
		ChampionAgentSpecHash: command.AgentSpecHash,
		DecisionID:            decisionID,
		DecidedBy:             command.UserID,
		DecidedAt:             decidedAt,
	}
}

func (u *agentRegistryUsecase) ReadAgentEvalReport(ctx context.Context, orgID uuid.UUID, reportID uuid.UUID) (*model.AgentEvalReport, error) {
	log.Trace("agentRegistryUsecase ReadAgentEvalReport")

	return u.repository.ReadAgentEvalReport(ctx, orgID, reportID)
}

func agentEndpointBound(bindings []*model.AgentEndpointBinding, endpointID uuid.UUID) bool {
	for _, binding := range bindings {
		if binding != nil && binding.EndpointID == endpointID {
			return true
		}
	}
	return false
}

func scoreGoldenTaskRun(task *model.GoldenTask, run model.AgentTaskRunResult, runErr error) *model.AgentEvalTaskResult {
	result := &model.AgentEvalTaskResult{
		TaskID:     task.TaskID,
		RunID:      run.RunID,
		Status:     strings.TrimSpace(run.Status),
		StopReason: strings.TrimSpace(run.StopReason),
	}
	if runErr != nil {
		result.Status = agentEvalStatusFailed
		result.StopReason = agentEvalStopRuntimeError
		result.FailureReason = runErr.Error()
		return result
	}
	completedWithFinalAnswer := strings.EqualFold(result.Status, agentEvalStatusCompleted) && strings.EqualFold(result.StopReason, agentEvalStopFinalAnswer)
	answerMatchesTask := answerMatchesExpected(run.Answer, task.ExpectedAnswer)
	result.TaskSuccess = completedWithFinalAnswer && answerMatchesTask
	result.ToolSuccess = agentEvalToolSuccess(run.ToolInvocations)
	result.Groundedness = answerMatchesTask && expectedAnswerSupportedByContext(task.ExpectedAnswer, run.GroundedContextTexts)
	if !completedWithFinalAnswer {
		result.FailureReason = "agent run did not complete with final answer"
	} else if !answerMatchesTask {
		result.FailureReason = "agent answer did not match the golden task expected answer"
	}
	if !result.ToolSuccess {
		result.FailureReason = strings.TrimSpace(strings.Join([]string{result.FailureReason, "tool invocation failed"}, "; "))
	}
	if !result.Groundedness {
		result.FailureReason = strings.TrimSpace(strings.Join([]string{result.FailureReason, "no grounded context returned"}, "; "))
	}
	return result
}

func answerMatchesExpected(answer string, expected string) bool {
	normalizedAnswer := normalizeEvalAnswer(answer)
	normalizedExpected := normalizeEvalAnswer(expected)
	return normalizedAnswer != "" && normalizedExpected != "" && strings.Contains(normalizedAnswer, normalizedExpected)
}

func expectedAnswerSupportedByContext(expected string, contexts []string) bool {
	normalizedExpected := normalizeEvalAnswer(expected)
	if normalizedExpected == "" {
		return false
	}
	for _, context := range contexts {
		if strings.Contains(normalizeEvalAnswer(context), normalizedExpected) {
			return true
		}
	}
	return false
}

func normalizeEvalAnswer(value string) string {
	fields := strings.FieldsFunc(strings.ToLower(strings.TrimSpace(value)), func(r rune) bool {
		return !(r >= 'a' && r <= 'z') && !(r >= '0' && r <= '9')
	})
	return strings.Join(fields, " ")
}

func agentEvalToolSuccess(invocations []model.AgentTaskToolInvocation) bool {
	for _, invocation := range invocations {
		if strings.TrimSpace(invocation.ErrorType) != "" && !strings.EqualFold(strings.TrimSpace(invocation.ErrorType), "UNKNOWN") {
			return false
		}
	}
	return true
}

func agentEvalReportFromTaskResults(command model.EvaluateSpecChampionCommand, results []*model.AgentEvalTaskResult) *model.AgentEvalReport {
	taskCount := len(results)
	taskSuccesses := 0
	toolSuccesses := 0
	grounded := 0
	for _, result := range results {
		if result.TaskSuccess {
			taskSuccesses++
		}
		if result.ToolSuccess {
			toolSuccesses++
		}
		if result.Groundedness {
			grounded++
		}
	}
	taskSuccessRate := ratio(taskSuccesses, taskCount)
	toolSuccessRate := ratio(toolSuccesses, taskCount)
	groundednessRate := ratio(grounded, taskCount)
	minTaskSuccess := command.MinTaskSuccessRate
	if minTaskSuccess == 0 {
		minTaskSuccess = defaultMinTaskSuccessRate
	}
	minToolSuccess := command.MinToolSuccessRate
	if minToolSuccess == 0 {
		minToolSuccess = defaultMinToolSuccessRate
	}
	minGroundedness := command.MinGroundednessRate
	if minGroundedness == 0 {
		minGroundedness = defaultMinGroundednessRate
	}
	passed := taskSuccessRate >= minTaskSuccess && toolSuccessRate >= minToolSuccess && groundednessRate >= minGroundedness
	reason := "candidate passes promotion holdout"
	if !passed {
		reason = fmt.Sprintf("candidate failed promotion holdout thresholds: task_success=%.4f tool_success=%.4f groundedness=%.4f", taskSuccessRate, toolSuccessRate, groundednessRate)
	}
	return &model.AgentEvalReport{
		OrgID:            command.OrgID,
		AgentLineage:     command.AgentLineage,
		AgentSpecHash:    command.AgentSpecHash,
		AdapterID:        command.AdapterID,
		EndpointID:       command.EndpointID,
		Split:            model.GoldenTaskSplitPromotionHoldout,
		SplitVersion:     command.SplitVersion,
		RubricVersion:    agentEvalRubricVersion,
		TaskCount:        taskCount,
		TaskSuccessRate:  taskSuccessRate,
		ToolSuccessRate:  toolSuccessRate,
		GroundednessRate: groundednessRate,
		Passed:           passed,
		GateReason:       reason,
		EvaluatedBy:      command.UserID,
		TaskResults:      results,
	}
}

func ratio(numerator int, denominator int) float64 {
	if denominator == 0 {
		return 0
	}
	return float64(numerator) / float64(denominator)
}

func normalizeGoldenPrompt(prompt string) string {
	return strings.Join(strings.Fields(strings.ToLower(strings.TrimSpace(prompt))), " ")
}

func goldenTaskContentFingerprint(normalizedPrompt string) string {
	return userevents.SHA256String(normalizedPrompt)
}

func goldenTaskNearDuplicateFingerprint(normalizedPrompt string) string {
	tokens := strings.Fields(normalizedPrompt)
	if len(tokens) == 0 {
		return goldenTaskContentFingerprint(normalizedPrompt)
	}
	seen := map[string]struct{}{}
	values := make([]string, 0, len(tokens))
	for _, token := range tokens {
		token = strings.Trim(token, ".,!?;:'\"()[]{}")
		if len(token) < 3 {
			continue
		}
		if _, skip := goldenTaskNearDuplicateStopWords[token]; skip {
			continue
		}
		if _, ok := seen[token]; ok {
			continue
		}
		seen[token] = struct{}{}
		values = append(values, token)
	}
	if len(values) == 0 {
		return goldenTaskContentFingerprint(normalizedPrompt)
	}
	sort.Strings(values)
	return userevents.SHA256String(strings.Join(values, " "))
}

var goldenTaskNearDuplicateStopWords = map[string]struct{}{
	"and": {}, "are": {}, "can": {}, "did": {}, "does": {}, "for": {}, "from": {},
	"has": {}, "have": {}, "how": {}, "into": {}, "the": {}, "this": {}, "that": {},
	"was": {}, "what": {}, "when": {}, "where": {}, "which": {}, "who": {}, "why": {},
	"with": {}, "you": {}, "your": {},
}

type trainingTuple struct {
	AgentSpecHash    string
	ToolsetHash      string
	EffectiveBaseID  string
	DataSnapshotHash string
}

type trajectoryDatasetManifestDoc struct {
	SchemaVersion      string                           `json:"schema_version"`
	GoldenSplitVersion int                              `json:"golden_split_version"`
	Labels             []trajectoryDatasetManifestLabel `json:"labels"`
}

type trajectoryDatasetManifestLabel struct {
	LabelID            string  `json:"label_id"`
	RunID              string  `json:"run_id"`
	ContentFingerprint string  `json:"content_fingerprint"`
	TaskSuccess        bool    `json:"task_success"`
	ToolSelectionScore float64 `json:"tool_selection_score"`
	Groundedness       float64 `json:"groundedness"`
	Confidence         float64 `json:"confidence"`
}

func filterTrainingLabels(labels []*model.AgentRunLabel, holdout []*model.GoldenTask) []*model.AgentRunLabel {
	holdoutFingerprints := map[string]struct{}{}
	holdoutNearFingerprints := map[string]struct{}{}
	for _, task := range holdout {
		if task == nil {
			continue
		}
		if fingerprint := strings.TrimSpace(task.ContentFingerprint); fingerprint != "" {
			holdoutFingerprints[fingerprint] = struct{}{}
		}
		if fingerprint := strings.TrimSpace(task.NearDuplicateFingerprint); fingerprint != "" {
			holdoutNearFingerprints[fingerprint] = struct{}{}
		}
	}
	out := make([]*model.AgentRunLabel, 0, len(labels))
	for _, label := range labels {
		if label == nil {
			continue
		}
		if _, leaked := holdoutFingerprints[strings.TrimSpace(label.ContentFingerprint)]; leaked {
			continue
		}
		if fingerprint := strings.TrimSpace(label.NearDuplicateFingerprint); fingerprint != "" {
			if _, leaked := holdoutNearFingerprints[fingerprint]; leaked {
				continue
			}
		}
		out = append(out, label)
	}
	return out
}

func commonTrainingTuple(labels []*model.AgentRunLabel) (trainingTuple, error) {
	if len(labels) == 0 {
		return trainingTuple{}, domain.ErrAgentTrainingFailed.Extend("no labels are available for training")
	}
	var first *model.AgentRunLabel
	for _, label := range labels {
		if label != nil {
			first = label
			break
		}
	}
	if first == nil {
		return trainingTuple{}, domain.ErrAgentTrainingFailed.Extend("no labels are available for training")
	}
	tuple := trainingTuple{
		AgentSpecHash:    strings.TrimSpace(first.AgentSpecHash),
		ToolsetHash:      strings.TrimSpace(first.ToolsetHash),
		EffectiveBaseID:  strings.TrimSpace(first.EffectiveBaseID),
		DataSnapshotHash: strings.TrimSpace(first.DataSnapshotHash),
	}
	if tuple.AgentSpecHash == "" || tuple.ToolsetHash == "" || tuple.EffectiveBaseID == "" || tuple.DataSnapshotHash == "" {
		return trainingTuple{}, domain.ErrAgentTrainingFailed.Extend("label tuple is incomplete")
	}
	for _, label := range labels {
		if label == nil {
			continue
		}
		if strings.TrimSpace(label.AgentSpecHash) != tuple.AgentSpecHash ||
			strings.TrimSpace(label.ToolsetHash) != tuple.ToolsetHash ||
			strings.TrimSpace(label.EffectiveBaseID) != tuple.EffectiveBaseID ||
			strings.TrimSpace(label.DataSnapshotHash) != tuple.DataSnapshotHash {
			return trainingTuple{}, domain.ErrAgentTrainingFailed.Extend("labels are not comparable for a single training dataset")
		}
	}
	return tuple, nil
}

func trajectoryDatasetManifest(labels []*model.AgentRunLabel, splitVersion int) (json.RawMessage, string, error) {
	doc := trajectoryDatasetManifestDoc{
		SchemaVersion:      trajectoryDatasetSchema,
		GoldenSplitVersion: splitVersion,
		Labels:             make([]trajectoryDatasetManifestLabel, 0, len(labels)),
	}
	for _, label := range labels {
		if label == nil {
			continue
		}
		doc.Labels = append(doc.Labels, trajectoryDatasetManifestLabel{
			LabelID:            label.LabelID.String(),
			RunID:              label.RunID.String(),
			ContentFingerprint: label.ContentFingerprint,
			TaskSuccess:        label.TaskSuccess,
			ToolSelectionScore: label.ToolSelectionScore,
			Groundedness:       label.Groundedness,
			Confidence:         label.Confidence,
		})
	}
	raw, err := serializers.NewJSONSerializer().Serialize(doc)
	if err != nil {
		return nil, "", err
	}
	return json.RawMessage(raw), userevents.SHA256String(string(raw)), nil
}

func agentAdapterCompatibleWithReport(adapter *model.AgentAdapter, report *model.AgentEvalReport) bool {
	if adapter == nil || report == nil {
		return false
	}
	return report.AdapterID == adapter.AdapterID &&
		strings.TrimSpace(report.AgentSpecHash) == strings.TrimSpace(adapter.TrainedAgainstAgentSpecHash) &&
		report.SplitVersion == adapter.TrainedAgainstGoldenSplitVersion &&
		strings.TrimSpace(report.RubricVersion) == strings.TrimSpace(adapter.TrainedAgainstRubricVersion)
}

func (u *agentRegistryUsecase) enforceChampionNoRegression(ctx context.Context, adapter *model.AgentAdapter, candidate *model.AgentEvalReport, minDelta float64) error {
	state, err := u.repository.ReadChampionState(ctx, adapter.OrgID, adapter.AgentLineage)
	if err != nil {
		if errors.Is(err, domain.ErrAgentChampionNotFound) {
			return nil
		}
		return err
	}
	if state == nil {
		return nil
	}
	if state.ChampionAdapterID == uuid.Nil {
		return nil
	}
	champion, err := u.repository.ReadLatestEvalReportForAdapter(ctx, adapter.OrgID, state.ChampionAdapterID)
	if err != nil {
		if errors.Is(err, domain.ErrAgentEvalFailed) {
			return nil
		}
		return err
	}
	if candidate.TaskSuccessRate+minDelta < champion.TaskSuccessRate ||
		candidate.ToolSuccessRate+minDelta < champion.ToolSuccessRate ||
		candidate.GroundednessRate+minDelta < champion.GroundednessRate {
		return domain.ErrAgentPromotionFailed.Extend("adapter eval regresses against champion")
	}
	return nil
}
