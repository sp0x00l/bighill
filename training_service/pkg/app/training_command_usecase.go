package app

import (
	"context"
	"encoding/json"
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
	StartDPOTrainingRun(ctx context.Context, command model.StartDPOTrainingRunCommand) (*model.TrainingRunStartResult, error)
	StartAgentAdapterTrainingRun(ctx context.Context, command model.StartAgentAdapterTrainingRunCommand) (*model.TrainingRunStartResult, error)
	ReadTrainingRun(ctx context.Context, trainingRunID uuid.UUID) (*model.TrainingRunStatusResult, error)
}

type trainingCommandUsecase struct {
	starter                   TrainingWorkflowStarter
	statusReader              TrainingWorkflowStatusReader
	datasetResolver           DatasetResolver
	modelResolver             ModelResolver
	preferenceDatasetResolver PreferenceDatasetResolver
	profileCatalog            TrainingProfileCatalog
}

type TrainingCommandOption func(*trainingCommandUsecase)

func WithPreferenceDatasetResolver(resolver PreferenceDatasetResolver) TrainingCommandOption {
	log.Trace("WithPreferenceDatasetResolver")

	return func(u *trainingCommandUsecase) {
		u.preferenceDatasetResolver = resolver
	}
}

func NewTrainingCommandUsecase(starter TrainingWorkflowStarter, statusReader TrainingWorkflowStatusReader, datasetResolver DatasetResolver, modelResolver ModelResolver, profileCatalog TrainingProfileCatalog, opts ...TrainingCommandOption) TrainingCommandUsecase {
	log.Trace("NewTrainingCommandUsecase")

	u := &trainingCommandUsecase{
		starter:         starter,
		statusReader:    statusReader,
		datasetResolver: datasetResolver,
		modelResolver:   modelResolver,
		profileCatalog:  profileCatalog,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(u)
		}
	}
	return u
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
	trainingProfile, evaluationProfile, err := u.resolveProfiles(ctx, command.TrainingProfile, command.EvaluationProfile)
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

func (u *trainingCommandUsecase) StartDPOTrainingRun(ctx context.Context, command model.StartDPOTrainingRunCommand) (out *model.TrainingRunStartResult, err error) {
	log.Trace("TrainingCommandUsecase StartDPOTrainingRun")

	ctx, span := usecasetrace.StartSpan(ctx, "training_service/app", "training.start_dpo_training_run")
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	userID, _ := ctxutil.TenantID(ctx)
	orgID, _ := ctxutil.OrgID(ctx)
	span.SetAttributes(
		attribute.String("user_id", userID.String()),
		attribute.String("org_id", orgID.String()),
		attribute.String("preference_dataset_id", command.PreferenceDatasetID.String()),
	)

	preferenceDataset, err := u.preferenceDatasetResolver.ResolvePreferenceDataset(ctx, userID, orgID, command.PreferenceDatasetID)
	if err != nil {
		return nil, err
	}
	trainingProfile, evaluationProfile, err := u.resolveProfiles(ctx, command.TrainingProfile, command.EvaluationProfile)
	if err != nil {
		return nil, err
	}
	request := u.dpoTrainingRunRequest(command.IdempotencyKey.String(), userID, orgID, preferenceDataset, trainingProfile, evaluationProfile)
	if err := u.starter.StartTrainingWorkflow(ctx, request); err != nil {
		return nil, fmt.Errorf("%w: start DPO training workflow: %w", domain.ErrTrainModel, err)
	}
	return &model.TrainingRunStartResult{
		TrainingRunID: request.TrainingRunID,
		StatusURL:     defaultTrainingRunStatusURLPrefix + request.TrainingRunID,
	}, nil
}

func (u *trainingCommandUsecase) StartAgentAdapterTrainingRun(ctx context.Context, command model.StartAgentAdapterTrainingRunCommand) (out *model.TrainingRunStartResult, err error) {
	log.Trace("TrainingCommandUsecase StartAgentAdapterTrainingRun")

	ctx, span := usecasetrace.StartSpan(ctx, "training_service/app", "training.start_agent_adapter_training_run")
	defer usecasetrace.EndSpanOnReturn(ctx, span, &err)

	userID, _ := ctxutil.TenantID(ctx)
	orgID, _ := ctxutil.OrgID(ctx)
	span.SetAttributes(
		attribute.String("user_id", userID.String()),
		attribute.String("org_id", orgID.String()),
		attribute.String("agent_trajectory_dataset_id", command.DatasetID.String()),
		attribute.String("source_model_id", command.SourceModelID.String()),
	)

	sourceModel, err := u.modelResolver.ResolveTrainableModel(ctx, userID, orgID, command.SourceModelID)
	if err != nil {
		return nil, err
	}
	trainingProfile, evaluationProfile, err := u.resolveProfiles(ctx, command.TrainingProfile, command.EvaluationProfile)
	if err != nil {
		return nil, err
	}
	request := u.agentAdapterTrainingRunRequest(command, userID, orgID, sourceModel, trainingProfile, evaluationProfile)
	if err := u.starter.StartTrainingWorkflow(ctx, request); err != nil {
		return nil, fmt.Errorf("%w: start agent adapter training workflow: %w", domain.ErrTrainModel, err)
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

func (u *trainingCommandUsecase) resolveProfiles(ctx context.Context, trainingProfileName string, evaluationProfileName string) (model.TrainingProfile, string, error) {
	log.Trace("trainingCommandUsecase resolveProfiles")

	trainingProfile, err := u.profileCatalog.ResolveTrainingProfile(ctx, trainingProfileName)
	if err != nil {
		return model.TrainingProfile{}, "", err
	}
	evaluationProfile, err := u.profileCatalog.ResolveEvaluationProfile(ctx, evaluationProfileName)
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
		LineageName:            trainingLineageName(datasetRef, sourceModel),
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

func trainingLineageName(datasetRef model.MaterializedDatasetRef, sourceModel model.SourceModelRef) string {
	log.Trace("trainingLineageName")

	if sharedDomain.ToModelKind(sourceModel.ModelKind) == sharedDomain.ModelKindFineTuned && strings.TrimSpace(sourceModel.LineageName) != "" {
		return strings.TrimSpace(sourceModel.LineageName)
	}
	return strings.TrimSpace(datasetRef.TableName)
}

func trainingModelVersion(sourceModel model.SourceModelRef) string {
	log.Trace("trainingModelVersion")

	if sharedDomain.ToModelKind(sourceModel.ModelKind) == sharedDomain.ModelKindFineTuned {
		return fmt.Sprintf("%d", sourceModel.ModelVersion+1)
	}
	return "1"
}

func (u *trainingCommandUsecase) dpoTrainingRunRequest(idempotencyKey string, userID uuid.UUID, orgID uuid.UUID, preferenceDataset model.PreferenceDatasetRef, trainingProfile model.TrainingProfile, evaluationProfile string) model.TrainingRunRequest {
	log.Trace("trainingCommandUsecase dpoTrainingRunRequest")

	trainingProfile.Trainer = "dpo"
	trainingProfile.PreferenceDatasetURI = preferenceDataset.OutputURI
	if strings.TrimSpace(trainingProfile.Name) == "" {
		trainingProfile.Name = "dpo"
	}
	trainingRunID := uuid.NewSHA1(uuid.NameSpaceURL, []byte(strings.Join([]string{
		"dpo",
		orgID.String(),
		userID.String(),
		preferenceDataset.PreferenceDatasetID,
		preferenceDataset.ModelID,
		fmt.Sprintf("%d", preferenceDataset.ParentModelVersion),
		idempotencyKey,
	}, ":")))
	modelVersion := fmt.Sprintf("%d", preferenceDataset.ParentModelVersion+1)
	return model.TrainingRunRequest{
		TrainingRunID:          trainingRunID.String(),
		UserID:                 userID.String(),
		OrgID:                  orgID.String(),
		DatasetID:              preferenceDataset.DatasetID,
		PreferenceDatasetID:    preferenceDataset.PreferenceDatasetID,
		PreferenceDatasetURI:   preferenceDataset.OutputURI,
		SourceModelID:          preferenceDataset.ModelID,
		SourceArtifactURI:      preferenceDataset.ParentArtifactURI,
		SourceModelKind:        preferenceDataset.ParentModelKind,
		SourceArtifactChecksum: preferenceDataset.ParentArtifactChecksum,
		ParentModelID:          preferenceDataset.ModelID,
		ParentModelVersion:     fmt.Sprintf("%d", preferenceDataset.ParentModelVersion),
		ParentAdapterURI:       preferenceDataset.ParentAdapterURI,
		ModelName:              "dpo-" + preferenceDataset.ModelID,
		LineageName:            preferenceDpoLineageName(preferenceDataset),
		ModelVersion:           modelVersion,
		BaseModel:              preferenceDataset.ParentBaseModel,
		EvaluationProfile:      preferenceDpoEvaluationProfile(evaluationProfile, preferenceDataset.EvaluationOutputURI),
		TrainingProfile:        trainingProfile,
	}
}

func (u *trainingCommandUsecase) agentAdapterTrainingRunRequest(command model.StartAgentAdapterTrainingRunCommand, userID uuid.UUID, orgID uuid.UUID, sourceModel model.SourceModelRef, trainingProfile model.TrainingProfile, evaluationProfile string) model.TrainingRunRequest {
	log.Trace("trainingCommandUsecase agentAdapterTrainingRunRequest")

	if strings.TrimSpace(trainingProfile.Trainer) == "" {
		trainingProfile.Trainer = "dpo"
	}
	trainingProfile.PreferenceDatasetURI = strings.TrimSpace(command.DatasetURI)
	trainingRunID := uuid.NewSHA1(uuid.NameSpaceURL, []byte(strings.Join([]string{
		"agent-adapter",
		orgID.String(),
		userID.String(),
		command.DatasetID.String(),
		sourceModel.ModelID,
		command.DatasetContentHash,
		command.IdempotencyKey.String(),
	}, ":")))
	baseModel := sourceModel.BaseModel
	if baseModel == "" {
		baseModel = sourceModel.Name
	}
	return model.TrainingRunRequest{
		TrainingRunID:          trainingRunID.String(),
		UserID:                 userID.String(),
		OrgID:                  orgID.String(),
		DatasetID:              command.DatasetID.String(),
		DatasetVersion:         strings.TrimSpace(command.DatasetContentHash),
		DatasetURI:             strings.TrimSpace(command.DatasetURI),
		PreferenceDatasetID:    command.DatasetID.String(),
		PreferenceDatasetURI:   strings.TrimSpace(command.DatasetURI),
		SourceModelID:          sourceModel.ModelID,
		SourceArtifactURI:      sourceModel.ArtifactLocation,
		SourceModelKind:        sourceModel.ModelKind,
		SourceArtifactChecksum: sourceModel.ArtifactChecksum,
		ParentModelID:          sourceModel.ModelID,
		ParentModelVersion:     fmt.Sprintf("%d", sourceModel.ModelVersion),
		ParentAdapterURI:       sourceModel.AdapterURI,
		ModelName:              "agent-" + command.AgentLineage,
		LineageName:            "agent-" + command.AgentLineage,
		ModelVersion:           trainingModelVersion(sourceModel),
		BaseModel:              baseModel,
		EvaluationProfile:      evaluationProfile,
		TrainingProfile:        trainingProfile,
	}
}

func preferenceDpoLineageName(preferenceDataset model.PreferenceDatasetRef) string {
	log.Trace("preferenceDpoLineageName")

	if lineageName := strings.TrimSpace(preferenceDataset.ParentLineageName); lineageName != "" {
		return lineageName
	}
	if parentBaseModel := strings.TrimSpace(preferenceDataset.ParentBaseModel); parentBaseModel != "" {
		return parentBaseModel
	}
	return strings.TrimSpace(preferenceDataset.ModelID)
}

func preferenceDpoEvaluationProfile(profile string, evaluationOutputURI string) string {
	log.Trace("preferenceDpoEvaluationProfile")

	profile = strings.TrimSpace(profile)
	evaluationOutputURI = strings.TrimSpace(evaluationOutputURI)
	if evaluationOutputURI == "" {
		return profile
	}
	values := map[string]any{}
	if strings.HasPrefix(profile, "{") {
		if err := json.Unmarshal([]byte(profile), &values); err != nil {
			return profile
		}
	} else if profile != "" {
		values["evaluator_name"] = profile
	}
	if _, ok := values["metric_suite"]; !ok {
		values["metric_suite"] = "preference"
	}
	if _, ok := values["evaluator_name"]; !ok {
		values["evaluator_name"] = "pairwise-judge"
	}
	if _, ok := values["evaluator_version"]; !ok {
		values["evaluator_version"] = "v1"
	}
	values["dataset_uri"] = evaluationOutputURI
	values["dataset_mode"] = "heldout_preference"
	raw, err := json.Marshal(values)
	if err != nil {
		return profile
	}
	return string(raw)
}
