package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"model_registry_service/pkg/domain"
	"model_registry_service/pkg/domain/model"

	"lib/shared_lib/authz"
	"lib/shared_lib/ctxutil"
	transport "lib/shared_lib/transport"
	shareduow "lib/shared_lib/uow"
	usecasetrace "lib/shared_lib/usecasetrace"
	"lib/shared_lib/userevents"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
)

type ModelRegistryUsecase interface {
	RegisterModel(ctx context.Context, registeredModel *model.Model, idempotencyKey uuid.UUID) (*model.Model, error)
	ReadModelSystem(ctx context.Context, modelID uuid.UUID) (*model.Model, error)
	ReadModelForUser(ctx context.Context, userID uuid.UUID, modelID uuid.UUID) (*model.Model, error)
	ListModels(ctx context.Context, userID uuid.UUID, pagination transport.Pagination, filter model.ListFilter) ([]*model.Model, int, error)
	MarkModelReady(ctx context.Context, modelID uuid.UUID, artifactLocation string) (*model.Model, error)
	MarkModelFailed(ctx context.Context, modelID uuid.UUID, failureReason string) (*model.Model, error)
	RecordModelTrainingCompleted(ctx context.Context, trainedModel *model.Model, idempotencyKey uuid.UUID) (*model.Model, error)
	RecordModelTrainingFailed(ctx context.Context, failedModel *model.Model, idempotencyKey uuid.UUID) (*model.Model, error)
	RecordModelArtifactIngested(ctx context.Context, ingestedModel *model.Model, idempotencyKey uuid.UUID) (*model.Model, error)
	RecordModelServingStatus(ctx context.Context, servedModelStatus *model.ServedModelStatus, idempotencyKey uuid.UUID) (*model.Model, error)
	RecordPromotionReportReady(ctx context.Context, report model.PromotionReportResult, idempotencyKey uuid.UUID) (*model.Model, error)
	PromoteCandidate(ctx context.Context, modelID uuid.UUID) (*model.Model, error)
}

type modelRegistryUsecase struct {
	repo               ModelRepository
	endpointRepository PublishedEndpointRepository
	unitOfWork         ModelUnitOfWorkAdapter
	eventBuilder       ModelEventBuilder
	servingDeployer    ModelServingDeployer
	userEventPublisher UserEventPublisher
	gatePolicy         model.GatePolicy
}

type ModelRegistryOption func(*modelRegistryUsecase)

func WithModelServingDeployer(deployer ModelServingDeployer) ModelRegistryOption {
	log.Trace("WithModelServingDeployer")

	return func(u *modelRegistryUsecase) {
		u.servingDeployer = deployer
	}
}

func WithPromotionGatePolicy(policy model.GatePolicy) ModelRegistryOption {
	log.Trace("WithPromotionGatePolicy")

	return func(u *modelRegistryUsecase) {
		u.gatePolicy = policy
	}
}

func WithPublishedEndpointRepository(repository PublishedEndpointRepository) ModelRegistryOption {
	log.Trace("WithPublishedEndpointRepository")

	return func(u *modelRegistryUsecase) {
		u.endpointRepository = repository
	}
}

func WithUserEventPublisher(publisher UserEventPublisher) ModelRegistryOption {
	log.Trace("WithUserEventPublisher")

	return func(u *modelRegistryUsecase) {
		u.userEventPublisher = publisher
	}
}

func NewModelRegistryUsecase(repo ModelRepository, unitOfWork ModelUnitOfWorkAdapter, eventBuilder ModelEventBuilder, opts ...ModelRegistryOption) ModelRegistryUsecase {
	log.Trace("NewModelRegistryUsecase")

	u := &modelRegistryUsecase{
		repo:               repo,
		unitOfWork:         unitOfWork,
		eventBuilder:       eventBuilder,
		userEventPublisher: userevents.NewNoopPublisher(),
		gatePolicy:         model.DefaultGatePolicy(),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(u)
		}
	}
	return u
}

func (u *modelRegistryUsecase) RegisterModel(ctx context.Context, registeredModel *model.Model, idempotencyKey uuid.UUID) (out *model.Model, err error) {
	log.Trace("ModelRegistryUsecase RegisterModel")

	ctx, span := usecasetrace.StartSpan(ctx, "model_registry_service/app", "model.register",
		attribute.String("idempotency_key", idempotencyKey.String()),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	if registeredModel != nil {
		ctx = contextForModel(ctx, registeredModel)
	}
	out, err = u.createModel(ctx, registeredModel, idempotencyKey)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (u *modelRegistryUsecase) ReadModelSystem(ctx context.Context, modelID uuid.UUID) (out *model.Model, err error) {
	log.Trace("ModelRegistryUsecase ReadModelSystem")

	ctx = ctxutil.WithSystemContext(ctx)
	ctx, span := usecasetrace.StartSpan(ctx, "model_registry_service/app", "model.read",
		attribute.String("model_id", modelID.String()),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	return u.repo.ReadByID(ctx, modelID)
}

func (u *modelRegistryUsecase) ReadModelForUser(ctx context.Context, userID uuid.UUID, modelID uuid.UUID) (out *model.Model, err error) {
	log.Trace("ModelRegistryUsecase ReadModelForUser")

	ctx = ensureActorContext(ctx, userID)
	ctx, span := usecasetrace.StartSpan(ctx, "model_registry_service/app", "model.read_for_user",
		attribute.String("user_id", userID.String()),
		attribute.String("model_id", modelID.String()),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	return u.repo.ReadByID(ctx, modelID)
}

func (u *modelRegistryUsecase) ListModels(ctx context.Context, userID uuid.UUID, pagination transport.Pagination, filter model.ListFilter) (out []*model.Model, total int, err error) {
	log.Trace("ModelRegistryUsecase ListModels")

	ctx = ensureActorContext(ctx, userID)
	ctx, span := usecasetrace.StartSpan(ctx, "model_registry_service/app", "model.list",
		attribute.String("user_id", userID.String()),
		attribute.Int("page", pagination.Page),
		attribute.Int("limit", pagination.Limit),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	return u.repo.List(ctx, pagination, filter)
}

func (u *modelRegistryUsecase) MarkModelReady(ctx context.Context, modelID uuid.UUID, artifactLocation string) (out *model.Model, err error) {
	log.Trace("ModelRegistryUsecase MarkModelReady")

	ctx = ctxutil.WithSystemContext(ctx)
	ctx, span := usecasetrace.StartSpan(ctx, "model_registry_service/app", "model.mark_ready",
		attribute.String("model_id", modelID.String()),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	out, err = u.updateModelStatus(ctx, modelID, model.ModelStatusReady, artifactLocation, "")
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (u *modelRegistryUsecase) MarkModelFailed(ctx context.Context, modelID uuid.UUID, failureReason string) (out *model.Model, err error) {
	log.Trace("ModelRegistryUsecase MarkModelFailed")

	ctx = ctxutil.WithSystemContext(ctx)
	ctx, span := usecasetrace.StartSpan(ctx, "model_registry_service/app", "model.mark_failed",
		attribute.String("model_id", modelID.String()),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	out, err = u.updateModelStatus(ctx, modelID, model.ModelStatusFailed, "", failureReason)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (u *modelRegistryUsecase) RecordModelTrainingCompleted(ctx context.Context, trainedModel *model.Model, idempotencyKey uuid.UUID) (out *model.Model, err error) {
	log.Trace("ModelRegistryUsecase RecordModelTrainingCompleted")

	ctx, span := usecasetrace.StartSpan(ctx, "model_registry_service/app", "model.record_training_completed",
		attribute.String("idempotency_key", idempotencyKey.String()),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	trainedModel.Status = model.ModelStatusCandidate
	trainedModel.ModelKind = model.ModelKindFineTuned
	trainedModel.Source = model.ModelSourceTraining
	trainedModel.ServingLoadStatus = model.ModelLoadStatusNotLoaded
	ctx = contextForModel(ctx, trainedModel)
	champion, err := u.repo.ReadChampion(ctx, model.LineageForModel(trainedModel))
	if err != nil && !errors.Is(err, domain.ErrModelNotFound) {
		return nil, err
	}
	out, err = u.createCandidateModel(ctx, trainedModel, champion, idempotencyKey)
	if err != nil {
		if errors.Is(err, domain.ErrModelExists) {
			return u.repo.ReadByTrainingRunID(ctx, trainedModel.TrainingRunID)
		}
		return nil, err
	}
	return out, nil
}

func (u *modelRegistryUsecase) RecordModelTrainingFailed(ctx context.Context, failedModel *model.Model, idempotencyKey uuid.UUID) (out *model.Model, err error) {
	log.Trace("ModelRegistryUsecase RecordModelTrainingFailed")

	ctx, span := usecasetrace.StartSpan(ctx, "model_registry_service/app", "model.record_training_failed",
		attribute.String("idempotency_key", idempotencyKey.String()),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	failedModel.Status = model.ModelStatusFailed
	failedModel.ModelKind = model.ModelKindFineTuned
	failedModel.Source = model.ModelSourceTraining
	failedModel.ServingLoadStatus = model.ModelLoadStatusNotLoaded
	ctx = contextForModel(ctx, failedModel)
	out, err = u.createModel(ctx, failedModel, idempotencyKey)
	if err != nil {
		if errors.Is(err, domain.ErrModelExists) {
			return u.repo.ReadByTrainingRunID(ctx, failedModel.TrainingRunID)
		}
		return nil, err
	}
	return out, nil
}

func (u *modelRegistryUsecase) RecordModelArtifactIngested(ctx context.Context, ingestedModel *model.Model, idempotencyKey uuid.UUID) (out *model.Model, err error) {
	log.Trace("ModelRegistryUsecase RecordModelArtifactIngested")

	ctx, span := usecasetrace.StartSpan(ctx, "model_registry_service/app", "model.record_artifact_ingested",
		attribute.String("idempotency_key", idempotencyKey.String()),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	ingestedModel.Status = model.ModelStatusEvaluated
	ctx = contextForModel(ctx, ingestedModel)
	if ingestedModel.ServingLoadStatus == model.ModelLoadStatusLoaded {
		ingestedModel.Status = model.ModelStatusReady
	}
	if ingestedModel.MetricsMetadata == "" {
		ingestedModel.MetricsMetadata = "{}"
	}
	out, err = u.createModel(ctx, ingestedModel, idempotencyKey)
	if err != nil {
		if errors.Is(err, domain.ErrModelExists) {
			out, err = u.repo.ReadByID(ctx, ingestedModel.ModelID)
			if err != nil {
				return nil, err
			}
			return out, u.ensureServedModel(ctx, out)
		}
		return nil, err
	}
	return out, u.ensureServedModel(ctx, out)
}

func (u *modelRegistryUsecase) RecordModelServingStatus(ctx context.Context, servedModelStatus *model.ServedModelStatus, idempotencyKey uuid.UUID) (out *model.Model, err error) {
	log.Trace("ModelRegistryUsecase RecordModelServingStatus")

	ctx = ctxutil.WithSystemContext(ctx)
	ctx, span := usecasetrace.StartSpan(ctx, "model_registry_service/app", "model.record_serving_status",
		attribute.String("idempotency_key", idempotencyKey.String()),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	status := model.ModelStatusEvaluated
	failureReason := ""
	existing, readErr := u.repo.ReadByID(ctx, servedModelStatus.ModelID)
	if readErr != nil && !errors.Is(readErr, domain.ErrModelNotFound) {
		return nil, readErr
	}
	switch servedModelStatus.ServingLoadStatus {
	case model.ModelLoadStatusLoaded:
		status = model.ModelStatusReady
	case model.ModelLoadStatusFailed:
		status = model.ModelStatusFailed
		failureReason = servedModelStatus.FailureReason
	}
	if existing != nil && existing.Status == model.ModelStatusCandidate && status != model.ModelStatusFailed {
		status = model.ModelStatusCandidate
	}
	previousServingStatus := model.ModelLoadStatusNotLoaded
	if existing != nil {
		previousServingStatus = existing.ServingLoadStatus
	}
	out, changed, err := u.updateServingStatus(ctx, servedModelStatus, status, failureReason, idempotencyKey)
	if err != nil {
		return nil, err
	}
	if changed {
		u.publishModelServingUserEvent(ctx, out, previousServingStatus)
	}
	return out, nil
}

func (u *modelRegistryUsecase) RecordPromotionReportReady(ctx context.Context, report model.PromotionReportResult, idempotencyKey uuid.UUID) (out *model.Model, err error) {
	log.Trace("ModelRegistryUsecase RecordPromotionReportReady")

	ctx, span := usecasetrace.StartSpan(ctx, "model_registry_service/app", "model.record_promotion_report_ready",
		attribute.String("idempotency_key", idempotencyKey.String()),
		attribute.String("model_id", report.ModelID.String()),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	ctx = ctxutil.WithSystemContext(ctx)
	candidate, err := u.repo.ReadByID(ctx, report.ModelID)
	if err != nil {
		return nil, err
	}
	if report.UserID != uuid.Nil && candidate.UserID != report.UserID {
		return nil, fmt.Errorf("%w: promotion report user id does not match candidate", domain.ErrValidationFailed)
	}
	if report.OrgID != uuid.Nil && candidate.OrgID != report.OrgID {
		return nil, fmt.Errorf("%w: promotion report org id does not match candidate", domain.ErrValidationFailed)
	}
	if report.TrainingRunID != uuid.Nil && candidate.TrainingRunID != report.TrainingRunID {
		return nil, fmt.Errorf("%w: promotion report training run id does not match candidate", domain.ErrValidationFailed)
	}
	if candidate.Status == model.ModelStatusEvaluated || candidate.Status == model.ModelStatusReady {
		return candidate, u.ensureServedModel(contextForModel(ctx, candidate), candidate)
	}
	if candidate.Status == model.ModelStatusFailed {
		return candidate, nil
	}
	if candidate.Status != model.ModelStatusCandidate {
		return nil, fmt.Errorf("%w: model %s is not a candidate", domain.ErrValidationFailed, report.ModelID)
	}
	if report.FailureReason != "" {
		decision := model.PromotionDecisionReason(model.PromotionDecisionOutcomeRejected, report.FailureReason)
		return u.recordPromotionDecision(contextForModel(ctx, candidate), candidate, model.ModelStatusFailed, report, decision, report.FailureReason)
	}

	decision, err := u.evaluateCandidatePromotion(contextForModel(ctx, candidate), candidate, &report)
	if err != nil {
		return nil, err
	}
	if !decision.Promote {
		promotionDecision := model.PromotionDecisionReason(model.PromotionDecisionOutcomeRejected, decision.Reason)
		return u.recordPromotionDecision(contextForModel(ctx, candidate), candidate, model.ModelStatusFailed, report, promotionDecision, decision.Reason)
	}
	report.Deltas = decision.Deltas
	out, err = u.recordPromotionDecision(contextForModel(ctx, candidate), candidate, model.ModelStatusEvaluated, report, model.PromotionDecisionReason(model.PromotionDecisionOutcomeAccepted, decision.Reason), "")
	if err != nil {
		return nil, err
	}
	return out, u.ensureServedModel(contextForModel(ctx, out), out)
}

func (u *modelRegistryUsecase) PromoteCandidate(ctx context.Context, modelID uuid.UUID) (out *model.Model, err error) {
	log.Trace("ModelRegistryUsecase PromoteCandidate")

	ctx = ctxutil.WithSystemContext(ctx)
	ctx, span := usecasetrace.StartSpan(ctx, "model_registry_service/app", "model.promote_candidate",
		attribute.String("model_id", modelID.String()),
	)
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	candidate, err := u.repo.ReadByID(ctx, modelID)
	if err != nil {
		return nil, err
	}
	if candidate.Status == model.ModelStatusEvaluated || candidate.Status == model.ModelStatusReady {
		return candidate, u.ensureServedModel(contextForModel(ctx, candidate), candidate)
	}
	if candidate.Status == model.ModelStatusFailed {
		return candidate, nil
	}
	if candidate.Status != model.ModelStatusCandidate {
		return nil, fmt.Errorf("%w: model %s is not a candidate", domain.ErrValidationFailed, modelID)
	}

	decision, err := u.evaluateCandidatePromotion(contextForModel(ctx, candidate), candidate, nil)
	if err != nil {
		return nil, err
	}
	if !decision.Promote {
		report := model.PromotionReportResult{ModelID: candidate.ModelID, TrainingRunID: candidate.TrainingRunID}
		return u.recordPromotionDecision(contextForModel(ctx, candidate), candidate, model.ModelStatusFailed, report, model.PromotionDecisionReason(model.PromotionDecisionOutcomeRejected, decision.Reason), decision.Reason)
	}
	report := model.PromotionReportResult{ModelID: candidate.ModelID, TrainingRunID: candidate.TrainingRunID, Deltas: decision.Deltas}
	out, err = u.recordPromotionDecision(contextForModel(ctx, candidate), candidate, model.ModelStatusEvaluated, report, model.PromotionDecisionReason(model.PromotionDecisionOutcomeAccepted, decision.Reason), "")
	if err != nil {
		return nil, err
	}
	return out, u.ensureServedModel(contextForModel(ctx, out), out)
}

func (u *modelRegistryUsecase) evaluateCandidatePromotion(ctx context.Context, candidate *model.Model, report *model.PromotionReportResult) (model.GateDecision, error) {
	log.Trace("ModelRegistryUsecase evaluateCandidatePromotion")

	candidateMetrics, err := parseEvalMetrics(candidate.MetricsMetadata)
	if err != nil {
		return model.GateDecision{Reason: err.Error(), Deltas: map[string]float64{}}, nil
	}
	champion, err := u.repo.ReadChampion(ctx, model.LineageForModel(candidate))
	if err != nil && !errors.Is(err, domain.ErrModelNotFound) {
		return model.GateDecision{}, err
	}
	var championMetrics *model.EvalMetrics
	if champion != nil {
		championMetrics, err = parseEvalMetrics(champion.MetricsMetadata)
		if err != nil {
			return model.GateDecision{Reason: "champion metrics invalid: " + err.Error(), Deltas: map[string]float64{}}, nil
		}
	}
	var evidence *model.PromotionReport
	if report != nil {
		evidence = &model.PromotionReport{
			DeepchecksPassed: report.DeepchecksPassed,
			EvidentlyPassed:  report.EvidentlyPassed,
		}
	}
	decision := model.EvaluatePromotion(candidateMetrics, championMetrics, evidence, u.gatePolicy)
	if decision.Promote && strings.HasPrefix(decision.Reason, "champion metrics incomparable; floor-only:") {
		lineage := model.LineageForModel(candidate)
		log.WithFields(log.Fields{
			"model_id":                   candidate.ModelID,
			"lineage":                    lineage.Name,
			"candidate_eval_dataset_uri": strings.TrimSpace(candidateMetrics.EvalDatasetURI),
			"champion_eval_dataset_uri":  strings.TrimSpace(championMetrics.EvalDatasetURI),
			"metric_suite":               strings.TrimSpace(candidateMetrics.MetricSuite),
			"evaluator_version":          strings.TrimSpace(candidateMetrics.EvaluatorVersion),
		}).Warn("promotion fell through to floor-only; champion/challenger eval sets incomparable")
	}
	return decision, nil
}

func (u *modelRegistryUsecase) ensureServedModel(ctx context.Context, registeredModel *model.Model) error {
	log.Trace("ModelRegistryUsecase ensureServedModel")

	if u.servingDeployer == nil || registeredModel.ServingLoadStatus == model.ModelLoadStatusLoaded || registeredModel.Status == model.ModelStatusFailed {
		return nil
	}
	return u.servingDeployer.EnsureServedModel(ctx, registeredModel)
}

func (u *modelRegistryUsecase) createModel(ctx context.Context, registeredModel *model.Model, idempotencyKey uuid.UUID) (*model.Model, error) {
	log.Trace("ModelRegistryUsecase createModel")

	var modelRecord *model.Model
	err := u.unitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx, enqueue shareduow.EnqueueFunc) error {
		created, err := u.repo.Create(ctx, tx, registeredModel, idempotencyKey)
		if err != nil {
			return err
		}
		if err := u.upsertPublishedEndpoint(ctx, tx, created); err != nil {
			return err
		}
		if err := enqueue(u.eventBuilder.ModelUpdatedMessage(created)); err != nil {
			return fmt.Errorf("enqueue model updated: %w", err)
		}
		modelRecord = created
		return nil
	})
	return modelRecord, err
}

func (u *modelRegistryUsecase) createCandidateModel(ctx context.Context, registeredModel *model.Model, champion *model.Model, idempotencyKey uuid.UUID) (*model.Model, error) {
	log.Trace("ModelRegistryUsecase createCandidateModel")

	var modelRecord *model.Model
	err := u.unitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx, enqueue shareduow.EnqueueFunc) error {
		created, err := u.repo.Create(ctx, tx, registeredModel, idempotencyKey)
		if err != nil {
			return err
		}
		if err := u.upsertPublishedEndpoint(ctx, tx, created); err != nil {
			return err
		}
		if err := enqueue(u.eventBuilder.ModelUpdatedMessage(created)); err != nil {
			return fmt.Errorf("enqueue model updated: %w", err)
		}
		if err := enqueue(u.eventBuilder.PromotionRequestedMessage(created, champion)); err != nil {
			return fmt.Errorf("enqueue promotion requested: %w", err)
		}
		modelRecord = created
		return nil
	})
	return modelRecord, err
}

func (u *modelRegistryUsecase) recordPromotionDecision(ctx context.Context, candidate *model.Model, status model.ModelStatus, report model.PromotionReportResult, promotionDecision string, failureReason string) (*model.Model, error) {
	log.Trace("ModelRegistryUsecase recordPromotionDecision")

	deltas, err := promotionDeltasJSON(report.Deltas)
	if err != nil {
		return nil, err
	}
	var modelRecord *model.Model
	err = u.unitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx, enqueue shareduow.EnqueueFunc) error {
		updated, err := u.repo.UpdatePromotionDecision(ctx, tx, candidate.ModelID, status, report.PromotionReportURI, deltas, promotionDecision, failureReason)
		if err != nil {
			return err
		}
		if err := u.upsertPublishedEndpoint(ctx, tx, updated); err != nil {
			return err
		}
		if err := enqueue(u.eventBuilder.ModelUpdatedMessage(updated)); err != nil {
			return fmt.Errorf("enqueue model updated: %w", err)
		}
		modelRecord = updated
		return nil
	})
	return modelRecord, err
}

func (u *modelRegistryUsecase) updateModelStatus(ctx context.Context, modelID uuid.UUID, status model.ModelStatus, artifactLocation string, failureReason string) (*model.Model, error) {
	log.Trace("ModelRegistryUsecase updateModelStatus")

	var modelRecord *model.Model
	err := u.unitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx, enqueue shareduow.EnqueueFunc) error {
		updated, err := u.repo.UpdateStatus(ctx, tx, modelID, status, artifactLocation, failureReason)
		if err != nil {
			return err
		}
		if err := u.upsertPublishedEndpoint(ctx, tx, updated); err != nil {
			return err
		}
		if err := enqueue(u.eventBuilder.ModelUpdatedMessage(updated)); err != nil {
			return fmt.Errorf("enqueue model updated: %w", err)
		}
		modelRecord = updated
		return nil
	})
	return modelRecord, err
}

func (u *modelRegistryUsecase) updateServingStatus(ctx context.Context, servedModelStatus *model.ServedModelStatus, status model.ModelStatus, failureReason string, idempotencyKey uuid.UUID) (*model.Model, bool, error) {
	log.Trace("ModelRegistryUsecase updateServingStatus")

	var modelRecord *model.Model
	var statusChanged bool
	err := u.unitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx, enqueue shareduow.EnqueueFunc) error {
		updated, changed, err := u.repo.UpdateServingStatus(ctx, tx, servedModelStatus.ModelID, status, servedModelStatus.ServingLoadStatus, servedModelStatus.ServingTarget, servedModelStatus.ServingModel, servedModelStatus.ServingProtocol, failureReason, idempotencyKey)
		if err != nil {
			return err
		}
		if err := u.upsertPublishedEndpoint(ctx, tx, updated); err != nil {
			return err
		}
		if changed {
			if err := enqueue(u.eventBuilder.ModelUpdatedMessage(updated)); err != nil {
				return fmt.Errorf("enqueue model updated: %w", err)
			}
		}
		modelRecord = updated
		statusChanged = changed
		return nil
	})
	return modelRecord, statusChanged, err
}

func (u *modelRegistryUsecase) publishModelServingUserEvent(ctx context.Context, modelRecord *model.Model, previousServingStatus model.ModelLoadStatus) {
	log.Trace("ModelRegistryUsecase publishModelServingUserEvent")

	if modelRecord == nil {
		return
	}
	eventType := userevents.EventTypeModelStatusUpdated
	severity := userevents.SeverityInfo
	title := "Model status updated"
	message := "The model serving status changed."
	var errorDetail *userevents.ErrorDetail

	switch modelRecord.ServingLoadStatus {
	case model.ModelLoadStatusLoaded:
		eventType = userevents.EventTypeModelServingLoaded
		severity = userevents.SeveritySuccess
		title = "Model ready"
		message = "The model is ready for inference."
	case model.ModelLoadStatusFailed:
		eventType = userevents.EventTypeModelServingFailed
		severity = userevents.SeverityError
		title = "Model serving failed"
		classified := userevents.ClassifyError(userevents.ClassificationInput{
			Service:          "model_registry_service",
			Operation:        "record_model_serving_status",
			ResourceType:     userevents.ResourceTypeModel,
			RawFailureReason: modelRecord.FailureReason,
		})
		errorDetail = &classified
		message = classified.Message
	}

	event := userevents.Event{
		EventID: userevents.DeterministicEventID(
			userevents.ResourceTypeModel,
			modelRecord.ModelID.String(),
			"serving",
			modelRecord.ServingLoadStatus.String(),
			userevents.HashString(modelRecord.FailureReason),
		),
		SourceService:      "model_registry_service",
		EventType:          eventType,
		Severity:           severity,
		RequiredPermission: authz.PermissionModelRead,
		UserID:             optionalEventUUID(modelRecord.UserID),
		OrgID:              optionalEventUUID(modelRecord.OrgID),
		Resource:           userevents.NewResource(userevents.ResourceTypeModel, modelRecord.ModelID, modelRecord.Name, "/models/"+modelRecord.ModelID.String()),
		Status: userevents.Status{
			State:         modelRecord.ServingLoadStatus.String(),
			PreviousState: previousServingStatus.String(),
			Phase:         userevents.StatusPhaseServing,
		},
		Title:       title,
		Message:     message,
		ActionLabel: "View model",
		ActionHref:  "/models/" + modelRecord.ModelID.String(),
		Error:       errorDetail,
	}
	if err := u.userEventPublisher.Publish(ctx, event); err != nil {
		userevents.LogPublishFailure(ctx, err, event)
	}
}

func optionalEventUUID(id uuid.UUID) string {
	log.Trace("optionalEventUUID")

	if id == uuid.Nil {
		return ""
	}
	return id.String()
}

func (u *modelRegistryUsecase) upsertPublishedEndpoint(ctx context.Context, tx pgx.Tx, modelRecord *model.Model) error {
	log.Trace("ModelRegistryUsecase upsertPublishedEndpoint")

	if u.endpointRepository == nil || modelRecord == nil {
		return nil
	}
	if modelRecord.OrgID == uuid.Nil || modelRecord.UserID == uuid.Nil {
		return fmt.Errorf("%w: published endpoint requires org_id and user_id", domain.ErrValidationFailed)
	}
	if modelRecord.DatasetID == uuid.Nil {
		return nil
	}
	status := model.PublishedEndpointStatusDisabled
	if modelRecord.Status == model.ModelStatusReady && modelRecord.ServingLoadStatus == model.ModelLoadStatusLoaded {
		status = model.PublishedEndpointStatusReady
	}
	return u.endpointRepository.UpsertEndpoint(ctx, tx, &model.PublishedEndpoint{
		OrgID:           modelRecord.OrgID,
		ModelID:         modelRecord.ModelID,
		DatasetID:       modelRecord.DatasetID,
		Status:          status,
		DisplayName:     modelRecord.Name,
		CreatedByUserID: modelRecord.UserID,
	})
}

func contextForModel(ctx context.Context, modelRecord *model.Model) context.Context {
	log.Trace("contextForModel")

	if modelRecord.UserID == uuid.Nil && modelRecord.OrgID == uuid.Nil {
		return ctx
	}
	return ctxutil.WithActorOrg(ctx, modelRecord.UserID, modelRecord.OrgID)
}

func ensureActorContext(ctx context.Context, userID uuid.UUID) context.Context {
	log.Trace("ensureActorContext")

	if _, ok := ctxutil.TenantID(ctx); ok {
		return ctx
	}
	return ctxutil.WithTenantID(ctx, userID)
}

func promotionDeltasJSON(deltas map[string]float64) (string, error) {
	log.Trace("promotionDeltasJSON")

	if len(deltas) == 0 {
		return "{}", nil
	}
	raw, err := json.Marshal(deltas)
	if err != nil {
		return "", fmt.Errorf("%w: marshal promotion deltas: %w", domain.ErrValidationFailed, err)
	}
	return string(raw), nil
}

type evalMetricsMetadataJSON struct {
	Passed           bool               `json:"passed"`
	Metrics          map[string]float64 `json:"metrics"`
	Thresholds       map[string]float64 `json:"thresholds"`
	ReportURI        string             `json:"report_uri"`
	EvaluatorName    string             `json:"evaluator_name"`
	EvaluatorVersion string             `json:"evaluator_version"`
	MetricSuite      string             `json:"metric_suite"`
	EvalDatasetURI   string             `json:"eval_dataset_uri"`
	EvalDatasetMode  string             `json:"eval_dataset_mode"`
	DeepchecksPassed bool               `json:"deepchecks_passed,omitempty"`
	DeepchecksURI    string             `json:"deepchecks_report_uri,omitempty"`
	EvidentlyPassed  bool               `json:"evidently_passed,omitempty"`
	EvidentlyURI     string             `json:"evidently_report_uri,omitempty"`
}

func parseEvalMetrics(metricsMetadata string) (*model.EvalMetrics, error) {
	log.Trace("parseEvalMetrics")

	metadata := strings.TrimSpace(metricsMetadata)
	if metadata == "" {
		return nil, fmt.Errorf("metrics metadata is required")
	}
	var record evalMetricsMetadataJSON
	if err := json.Unmarshal([]byte(metadata), &record); err != nil {
		return nil, fmt.Errorf("parse metrics metadata: %w", err)
	}
	if len(record.Metrics) == 0 {
		return nil, fmt.Errorf("metrics metadata must include metrics")
	}
	return &model.EvalMetrics{
		Passed:           record.Passed,
		Metrics:          record.Metrics,
		Thresholds:       record.Thresholds,
		ReportURI:        record.ReportURI,
		EvaluatorName:    record.EvaluatorName,
		EvaluatorVersion: record.EvaluatorVersion,
		MetricSuite:      record.MetricSuite,
		EvalDatasetURI:   record.EvalDatasetURI,
		EvalDatasetMode:  record.EvalDatasetMode,
		DeepchecksPassed: record.DeepchecksPassed,
		DeepchecksURI:    record.DeepchecksURI,
		EvidentlyPassed:  record.EvidentlyPassed,
		EvidentlyURI:     record.EvidentlyURI,
	}, nil
}
