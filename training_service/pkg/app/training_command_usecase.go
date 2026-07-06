package app

import (
	"context"
	"fmt"
	"strings"

	"training_service/pkg/domain"
	"training_service/pkg/domain/model"

	"lib/shared_lib/ctxutil"
	sharedDomain "lib/shared_lib/domain"
	usecasetrace "lib/shared_lib/usecasetrace"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
)

const (
	modelStatusReady = "READY"

	tableFormatParquet                = "PARQUET"
	stateFeatureMaterialized          = "FEATURE_MATERIALIZED"
	stateEmbeddingsMaterialized       = "EMBEDDINGS_MATERIALIZED"
	defaultTrainingRunStatusURLPrefix = "/v1/private/training-runs/"
)

type TrainingCommandUsecase interface {
	StartTrainingRun(ctx context.Context, command model.StartTrainingRunCommand) (*model.TrainingRunStartResult, error)
	ReadTrainingRun(ctx context.Context, trainingRunID string) (*model.TrainingRunStatusResult, error)
}

type trainingCommandUsecase struct {
	starter         TrainingWorkflowStarter
	statusReader    TrainingWorkflowStatusReader
	datasetResolver DatasetResolver
	modelResolver   ModelResolver
	profileCatalog  TrainingProfileCatalog
}

func NewTrainingCommandUsecase(starter TrainingWorkflowStarter, statusReader TrainingWorkflowStatusReader, datasetResolver DatasetResolver, modelResolver ModelResolver, profileCatalog TrainingProfileCatalog) TrainingCommandUsecase {
	log.Trace("NewTrainingCommandUsecase")

	return &trainingCommandUsecase{
		starter:         starter,
		statusReader:    statusReader,
		datasetResolver: datasetResolver,
		modelResolver:   modelResolver,
		profileCatalog:  profileCatalog,
	}
}

func (u *trainingCommandUsecase) StartTrainingRun(ctx context.Context, command model.StartTrainingRunCommand) (out *model.TrainingRunStartResult, err error) {
	log.Trace("TrainingCommandUsecase StartTrainingRun")

	ctx, span := usecasetrace.StartSpan(ctx, "training_service/app", "training.start_training_run")
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	userID, ok := ctxutil.TenantID(ctx)
	if !ok {
		return nil, domain.ErrValidationFailed.Extend("user id is required")
	}
	datasetID, err := uuid.Parse(strings.TrimSpace(command.DatasetID))
	if err != nil || datasetID == uuid.Nil {
		return nil, domain.ErrValidationFailed.Extend("dataset id is required")
	}
	sourceModelID, err := uuid.Parse(strings.TrimSpace(command.SourceModelID))
	if err != nil || sourceModelID == uuid.Nil {
		return nil, domain.ErrValidationFailed.Extend("source model id is required")
	}
	idempotencyKey := strings.TrimSpace(command.IdempotencyKey)
	if idempotencyKey == "" {
		return nil, domain.ErrValidationFailed.Extend("idempotency key is required")
	}
	span.SetAttributes(
		attribute.String("user_id", userID.String()),
		attribute.String("dataset_id", datasetID.String()),
		attribute.String("source_model_id", sourceModelID.String()),
	)

	datasetRef, err := u.datasetResolver.ResolveMaterializedDataset(ctx, userID, datasetID)
	if err != nil {
		return nil, err
	}
	sourceModel, err := u.modelResolver.ResolveTrainableModel(ctx, userID, sourceModelID)
	if err != nil {
		return nil, err
	}
	if err := validateResolvedTrainingInputs(datasetRef, sourceModel); err != nil {
		return nil, err
	}
	trainingProfile, evaluationProfile, err := u.resolveProfiles(ctx, command)
	if err != nil {
		return nil, err
	}

	request := u.trainingRunRequest(idempotencyKey, userID, datasetRef, sourceModel, trainingProfile, evaluationProfile)
	if err := u.starter.StartTrainingWorkflow(ctx, request); err != nil {
		return nil, fmt.Errorf("%w: start training workflow: %w", domain.ErrTrainModel, err)
	}
	return &model.TrainingRunStartResult{
		TrainingRunID: request.TrainingRunID,
		StatusURL:     defaultTrainingRunStatusURLPrefix + request.TrainingRunID,
	}, nil
}

func (u *trainingCommandUsecase) ReadTrainingRun(ctx context.Context, trainingRunID string) (out *model.TrainingRunStatusResult, err error) {
	log.Trace("TrainingCommandUsecase ReadTrainingRun")

	ctx, span := usecasetrace.StartSpan(ctx, "training_service/app", "training.read_training_run")
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	parsedTrainingRunID, err := uuid.Parse(strings.TrimSpace(trainingRunID))
	if err != nil || parsedTrainingRunID == uuid.Nil {
		return nil, domain.ErrValidationFailed.Extend("training run id is required")
	}
	span.SetAttributes(attribute.String("training_run_id", parsedTrainingRunID.String()))
	return u.statusReader.ReadTrainingWorkflowStatus(ctx, parsedTrainingRunID.String())
}

func (u *trainingCommandUsecase) resolveProfiles(ctx context.Context, command model.StartTrainingRunCommand) (model.TrainingProfile, string, error) {
	log.Trace("trainingCommandUsecase resolveProfiles")

	trainingProfile, err := u.profileCatalog.ResolveTrainingProfile(ctx, command.TrainingProfile)
	if err != nil {
		return model.TrainingProfile{}, "", err
	}
	evaluationProfile, err := u.profileCatalog.ResolveEvaluationProfile(ctx, command.EvaluationProfile)
	if err != nil {
		return model.TrainingProfile{}, "", err
	}
	return trainingProfile, evaluationProfile, nil
}

func (u *trainingCommandUsecase) trainingRunRequest(idempotencyKey string, userID uuid.UUID, datasetRef model.MaterializedDatasetRef, sourceModel model.SourceModelRef, trainingProfile model.TrainingProfile, evaluationProfile string) model.TrainingRunRequest {
	log.Trace("trainingCommandUsecase trainingRunRequest")

	trainingRunID := uuid.NewSHA1(uuid.NameSpaceURL, []byte(strings.Join([]string{
		"training",
		userID.String(),
		strings.TrimSpace(datasetRef.DatasetID),
		strings.TrimSpace(sourceModel.ModelID),
		idempotencyKey,
	}, ":")))
	baseModel := strings.TrimSpace(sourceModel.BaseModel)
	if baseModel == "" {
		baseModel = strings.TrimSpace(sourceModel.Name)
	}
	request := model.TrainingRunRequest{
		TrainingRunID:          trainingRunID.String(),
		UserID:                 userID.String(),
		DatasetID:              strings.TrimSpace(datasetRef.DatasetID),
		DatasetVersion:         strings.TrimSpace(datasetRef.DatasetVersion),
		FeatureSnapshotID:      strings.TrimSpace(datasetRef.FeatureSnapshotID),
		DatasetURI:             strings.TrimSpace(datasetRef.DatasetURI),
		SourceModelID:          strings.TrimSpace(sourceModel.ModelID),
		SourceArtifactURI:      strings.TrimSpace(sourceModel.ArtifactLocation),
		SourceModelKind:        strings.TrimSpace(sourceModel.ModelKind),
		SourceArtifactChecksum: strings.TrimSpace(sourceModel.ArtifactChecksum),
		ModelName:              strings.TrimSpace(datasetRef.TableName),
		ModelVersion:           trainingModelVersion(sourceModel),
		BaseModel:              baseModel,
		EvaluationProfile:      evaluationProfile,
		TrainingProfile:        trainingProfile,
	}
	if sharedDomain.ToModelKind(sourceModel.ModelKind) == sharedDomain.ModelKindFineTuned {
		request.ParentModelID = strings.TrimSpace(sourceModel.ModelID)
		request.ParentModelVersion = fmt.Sprintf("%d", sourceModel.ModelVersion)
		request.ParentAdapterURI = strings.TrimSpace(sourceModel.AdapterURI)
	}
	return request
}

func validateResolvedTrainingInputs(datasetRef model.MaterializedDatasetRef, sourceModel model.SourceModelRef) error {
	log.Trace("validateResolvedTrainingInputs")

	state := strings.TrimSpace(datasetRef.ProcessingState)
	if state != stateFeatureMaterialized && state != stateEmbeddingsMaterialized {
		return domain.ErrValidationFailed.Extend("dataset is not materialized")
	}
	if strings.TrimSpace(datasetRef.TableFormat) != tableFormatParquet {
		return domain.ErrValidationFailed.Extend("training requires a parquet dataset")
	}
	if strings.TrimSpace(datasetRef.FeatureSnapshotID) == "" {
		return domain.ErrValidationFailed.Extend("feature snapshot id is required")
	}
	if strings.TrimSpace(datasetRef.DatasetURI) == "" {
		return domain.ErrValidationFailed.Extend("dataset uri is required")
	}
	if strings.TrimSpace(datasetRef.TableName) == "" {
		return domain.ErrValidationFailed.Extend("dataset table name is required")
	}
	if strings.TrimSpace(sourceModel.Status) != modelStatusReady {
		return domain.ErrValidationFailed.Extend("source model is not ready")
	}
	kind := sharedDomain.ToModelKind(sourceModel.ModelKind)
	if !sharedDomain.IsKnownModelKind(kind) {
		return domain.ErrValidationFailed.Extend("source model is not trainable")
	}
	if strings.TrimSpace(sourceModel.Name) == "" {
		return domain.ErrValidationFailed.Extend("source model name is required")
	}
	if strings.TrimSpace(sourceModel.ArtifactLocation) == "" {
		return domain.ErrValidationFailed.Extend("source artifact uri is required")
	}
	if kind == sharedDomain.ModelKindFineTuned && strings.TrimSpace(sourceModel.AdapterURI) == "" {
		return domain.ErrValidationFailed.Extend("fine tuned source model adapter uri is required")
	}
	return nil
}

func trainingModelVersion(sourceModel model.SourceModelRef) string {
	log.Trace("trainingModelVersion")

	if sharedDomain.ToModelKind(sourceModel.ModelKind) == sharedDomain.ModelKindFineTuned {
		return fmt.Sprintf("%d", sourceModel.ModelVersion+1)
	}
	return "1"
}
