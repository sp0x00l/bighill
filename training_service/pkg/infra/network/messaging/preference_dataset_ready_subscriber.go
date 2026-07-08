package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"training_service/pkg/domain/model"

	inferencepb "lib/data_contracts_lib/inference"
	sharedDomain "lib/shared_lib/domain"
	msgConn "lib/shared_lib/messaging"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

var errPreferenceDatasetParentNotReady = errors.New("preference dataset parent model is not ready")

type TrainingWorkflowStarter interface {
	StartTrainingWorkflow(ctx context.Context, request model.TrainingRunRequest) error
}

type TrainingProfileCatalog interface {
	ResolveTrainingProfile(ctx context.Context, name string) (model.TrainingProfile, error)
	ResolveEvaluationProfile(ctx context.Context, name string) (string, error)
}

type TrainingTopics struct {
	Inference     string
	ModelRegistry string
	Training      string
}

type preferenceDatasetReadyEventListener struct {
	starter               TrainingWorkflowStarter
	profileCatalog        TrainingProfileCatalog
	trainingProfileName   string
	evaluationProfileName string
}

func NewPreferenceDatasetReadyEventListener(starter TrainingWorkflowStarter, profileCatalog TrainingProfileCatalog, trainingProfileName string, evaluationProfileName string) *preferenceDatasetReadyEventListener {
	log.Trace("NewPreferenceDatasetReadyEventListener")

	return &preferenceDatasetReadyEventListener{
		starter:               starter,
		profileCatalog:        profileCatalog,
		trainingProfileName:   strings.TrimSpace(trainingProfileName),
		evaluationProfileName: strings.TrimSpace(evaluationProfileName),
	}
}

func (l *preferenceDatasetReadyEventListener) MsgType() msgConn.MsgType {
	log.Trace("preferenceDatasetReadyEventListener MsgType")

	return msgConn.MsgTypePreferenceDatasetReady
}

func (l *preferenceDatasetReadyEventListener) NewMessage() *inferencepb.PreferenceDatasetReadyEvent {
	log.Trace("preferenceDatasetReadyEventListener NewMessage")

	return &inferencepb.PreferenceDatasetReadyEvent{}
}

func (l *preferenceDatasetReadyEventListener) Handle(ctx context.Context, resourceKey uuid.UUID, payload *inferencepb.PreferenceDatasetReadyEvent) error {
	log.Trace("preferenceDatasetReadyEventListener Handle")

	if l.starter == nil {
		return msgConn.NonRetryable(fmt.Errorf("training workflow starter is nil"))
	}
	profile, evaluationProfile, err := l.resolveProfiles(ctx)
	if err != nil {
		return msgConn.NonRetryable(err)
	}
	request, err := buildDPOTrainingRunRequest(resourceKey, payload, profile, evaluationProfile)
	if err != nil {
		if errors.Is(err, errPreferenceDatasetParentNotReady) {
			return err
		}
		return msgConn.NonRetryable(err)
	}
	if strings.TrimSpace(request.TrainingRunID) == "" {
		return nil
	}
	return l.starter.StartTrainingWorkflow(ctx, request)
}

func (l *preferenceDatasetReadyEventListener) resolveProfiles(ctx context.Context) (model.TrainingProfile, string, error) {
	log.Trace("preferenceDatasetReadyEventListener resolveProfiles")

	profile, err := l.profileCatalog.ResolveTrainingProfile(ctx, l.trainingProfileName)
	if err != nil {
		return model.TrainingProfile{}, "", err
	}
	evaluationProfile, err := l.profileCatalog.ResolveEvaluationProfile(ctx, l.evaluationProfileName)
	if err != nil {
		return model.TrainingProfile{}, "", err
	}
	return profile, evaluationProfile, nil
}

func buildDPOTrainingRunRequest(resourceKey uuid.UUID, payload *inferencepb.PreferenceDatasetReadyEvent, profile model.TrainingProfile, evaluationProfile string) (model.TrainingRunRequest, error) {
	log.Trace("buildDPOTrainingRunRequest")

	if resourceKey == uuid.Nil {
		return model.TrainingRunRequest{}, fmt.Errorf("resource key is required")
	}
	if payload == nil {
		return model.TrainingRunRequest{}, fmt.Errorf("preference dataset ready payload is required")
	}
	datasetID, err := msgConn.ParseUUID("dataset_id", payload.GetDatasetId())
	if err != nil {
		return model.TrainingRunRequest{}, err
	}
	if datasetID != resourceKey {
		return model.TrainingRunRequest{}, fmt.Errorf("dataset id %s does not match resource key %s", datasetID, resourceKey)
	}
	preferenceDatasetID, err := msgConn.ParseUUID("preference_dataset_id", payload.GetPreferenceDatasetId())
	if err != nil {
		return model.TrainingRunRequest{}, err
	}
	modelID, err := msgConn.ParseUUID("model_id", payload.GetModelId())
	if err != nil {
		return model.TrainingRunRequest{}, err
	}
	userID, err := msgConn.ParseUUID("user_id", payload.GetUserId())
	if err != nil {
		return model.TrainingRunRequest{}, err
	}
	orgID, err := msgConn.ParseUUID("org_id", payload.GetOrgId())
	if err != nil {
		return model.TrainingRunRequest{}, err
	}
	sourceRequestID, err := msgConn.ParseUUID("source_request_id", payload.GetSourceRequestId())
	if err != nil {
		return model.TrainingRunRequest{}, err
	}
	outputURI := strings.TrimSpace(payload.GetOutputUri())
	if outputURI == "" {
		return model.TrainingRunRequest{}, fmt.Errorf("preference dataset output uri is required")
	}
	evaluationOutputURI := strings.TrimSpace(payload.GetEvaluationOutputUri())
	if payload.GetExampleCount() <= 0 {
		return model.TrainingRunRequest{}, fmt.Errorf("preference dataset examples are required")
	}
	if payload.GetMinExamples() > 0 && payload.GetExampleCount() < payload.GetMinExamples() {
		return model.TrainingRunRequest{}, nil
	}
	parentModelKind := sharedDomain.ToModelKind(payload.GetParentModelKind())
	if !sharedDomain.IsKnownModelKind(parentModelKind) {
		return model.TrainingRunRequest{}, fmt.Errorf("parent model kind is required")
	}
	parentAdapterURI := strings.TrimSpace(payload.GetParentAdapterUri())
	if parentModelKind == sharedDomain.ModelKindFineTuned && parentAdapterURI == "" {
		return model.TrainingRunRequest{}, fmt.Errorf("fine-tuned parent adapter uri is required")
	}
	if parentModelKind == sharedDomain.ModelKindBase && parentAdapterURI != "" {
		return model.TrainingRunRequest{}, fmt.Errorf("base parent adapter uri must be empty")
	}
	parentArtifactURI := strings.TrimSpace(payload.GetParentArtifactUri())
	if parentArtifactURI == "" {
		return model.TrainingRunRequest{}, fmt.Errorf("parent artifact uri is required")
	}
	parentModelVersion := payload.GetParentModelVersion()
	if parentModelVersion <= 0 {
		return model.TrainingRunRequest{}, fmt.Errorf("%w: parent model version is required", errPreferenceDatasetParentNotReady)
	}
	parentBaseModel := strings.TrimSpace(payload.GetParentBaseModel())
	if parentBaseModel == "" {
		return model.TrainingRunRequest{}, fmt.Errorf("%w: parent base model is required", errPreferenceDatasetParentNotReady)
	}
	profile.Trainer = "dpo"
	profile.PreferenceDatasetURI = outputURI
	if strings.TrimSpace(profile.Name) == "" {
		profile.Name = "dpo"
	}
	trainingRunID := uuid.NewSHA1(uuid.NameSpaceURL, []byte(strings.Join([]string{
		"dpo",
		orgID.String(),
		datasetID.String(),
		modelID.String(),
		fmt.Sprintf("%d", parentModelVersion),
		preferenceDatasetID.String(),
		sourceRequestID.String(),
		outputURI,
	}, ":")))
	evaluationProfile = preferenceEvaluationProfile(evaluationProfile, evaluationOutputURI)
	if evaluationProfile == "" {
		evaluationProfile = "smoke"
	}
	modelVersion := fmt.Sprintf("%d", parentModelVersion+1)
	return model.TrainingRunRequest{
		TrainingRunID:        trainingRunID.String(),
		UserID:               userID.String(),
		OrgID:                orgID.String(),
		DatasetID:            datasetID.String(),
		DatasetVersion:       "",
		PreferenceDatasetID:  preferenceDatasetID.String(),
		PreferenceDatasetURI: outputURI,
		ParentModelID:        modelID.String(),
		ParentModelVersion:   fmt.Sprintf("%d", parentModelVersion),
		ParentAdapterURI:     parentAdapterURI,
		SourceModelID:        modelID.String(),
		SourceArtifactURI:    parentArtifactURI,
		SourceModelKind:      parentModelKind.String(),
		ModelName:            "dpo-" + modelID.String(),
		ModelVersion:         modelVersion,
		BaseModel:            parentBaseModel,
		EvaluationProfile:    evaluationProfile,
		TrainingProfile:      profile,
	}, nil
}

func preferenceEvaluationProfile(profile string, evaluationOutputURI string) string {
	log.Trace("preferenceEvaluationProfile")

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
