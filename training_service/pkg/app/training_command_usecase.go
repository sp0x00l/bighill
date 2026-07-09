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
	defaultTrainingRunStatusURLPrefix = "/v1/private/training-runs/"
)

type TrainingCommandUsecase interface {
	StartTrainingRun(ctx context.Context, command model.StartTrainingRunCommand) (*model.TrainingRunStartResult, error)
	ReadTrainingRun(ctx context.Context, trainingRunID uuid.UUID) (*model.TrainingRunStatusResult, error)
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

	userID, _ := ctxutil.TenantID(ctx)
	orgID, _ := ctxutil.OrgID(ctx)
	span.SetAttributes(
		attribute.String("user_id", userID.String()),
		attribute.String("org_id", orgID.String()),
		attribute.String("dataset_id", command.DatasetID.String()),
		attribute.String("source_model_id", command.SourceModelID.String()),
	)

	datasetRef, err := u.datasetResolver.ResolveMaterializedDataset(ctx, userID, orgID, command.DatasetID)
	if err != nil {
		return nil, err
	}
	sourceModel, err := u.modelResolver.ResolveTrainableModel(ctx, userID, orgID, command.SourceModelID)
	if err != nil {
		return nil, err
	}
	trainingProfile, evaluationProfile, err := u.resolveProfiles(ctx, command)
	if err != nil {
		return nil, err
	}

	request := u.trainingRunRequest(command.IdempotencyKey.String(), userID, orgID, datasetRef, sourceModel, trainingProfile, evaluationProfile)
	if err := u.starter.StartTrainingWorkflow(ctx, request); err != nil {
		return nil, fmt.Errorf("%w: start training workflow: %w", domain.ErrTrainModel, err)
	}
	return &model.TrainingRunStartResult{
		TrainingRunID: request.TrainingRunID,
		StatusURL:     defaultTrainingRunStatusURLPrefix + request.TrainingRunID,
	}, nil
}

func (u *trainingCommandUsecase) ReadTrainingRun(ctx context.Context, trainingRunID uuid.UUID) (out *model.TrainingRunStatusResult, err error) {
	log.Trace("TrainingCommandUsecase ReadTrainingRun")

	ctx, span := usecasetrace.StartSpan(ctx, "training_service/app", "training.read_training_run")
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	span.SetAttributes(attribute.String("training_run_id", trainingRunID.String()))
	return u.statusReader.ReadTrainingWorkflowStatus(ctx, trainingRunID.String())
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

func (u *trainingCommandUsecase) trainingRunRequest(idempotencyKey string, userID uuid.UUID, orgID uuid.UUID, datasetRef model.MaterializedDatasetRef, sourceModel model.SourceModelRef, trainingProfile model.TrainingProfile, evaluationProfile string) model.TrainingRunRequest {
	log.Trace("trainingCommandUsecase trainingRunRequest")

	trainingRunID := uuid.NewSHA1(uuid.NameSpaceURL, []byte(strings.Join([]string{
		"training",
		orgID.String(),
		userID.String(),
		datasetRef.DatasetID,
		sourceModel.ModelID,
		idempotencyKey,
	}, ":")))
	baseModel := sourceModel.BaseModel
	if baseModel == "" {
		baseModel = sourceModel.Name
	}
	request := model.TrainingRunRequest{
		TrainingRunID:          trainingRunID.String(),
		UserID:                 userID.String(),
		OrgID:                  orgID.String(),
		DatasetID:              datasetRef.DatasetID,
		DatasetVersion:         datasetRef.DatasetVersion,
		FeatureSnapshotID:      datasetRef.FeatureSnapshotID,
		DatasetURI:             datasetRef.DatasetURI,
		SourceModelID:          sourceModel.ModelID,
		SourceArtifactURI:      sourceModel.ArtifactLocation,
		SourceModelKind:        sourceModel.ModelKind,
		SourceArtifactChecksum: sourceModel.ArtifactChecksum,
		ModelName:              datasetRef.TableName,
		ModelVersion:           trainingModelVersion(sourceModel),
		BaseModel:              baseModel,
		EvaluationProfile:      evaluationProfile,
		TrainingProfile:        trainingProfile,
	}
	if sharedDomain.ToModelKind(sourceModel.ModelKind) == sharedDomain.ModelKindFineTuned {
		request.ParentModelID = sourceModel.ModelID
		request.ParentModelVersion = fmt.Sprintf("%d", sourceModel.ModelVersion)
		request.ParentAdapterURI = sourceModel.AdapterURI
	}
	return request
}

func trainingModelVersion(sourceModel model.SourceModelRef) string {
	log.Trace("trainingModelVersion")

	if sharedDomain.ToModelKind(sourceModel.ModelKind) == sharedDomain.ModelKindFineTuned {
		return fmt.Sprintf("%d", sourceModel.ModelVersion+1)
	}
	return "1"
}
