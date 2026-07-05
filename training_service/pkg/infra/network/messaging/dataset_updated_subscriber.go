package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"training_service/pkg/domain/model"

	datasetpb "lib/data_contracts_lib/data_registry"
	inferencepb "lib/data_contracts_lib/inference"
	msgConn "lib/shared_lib/messaging"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

var errPreferenceDatasetParentNotReady = errors.New("preference dataset parent model is not ready")

type TrainingWorkflowStarter interface {
	StartTrainingWorkflow(ctx context.Context, request model.TrainingRunRequest) error
}

type DatasetUpdatedSubscriber interface {
	Start(ctx context.Context) error
}

type TrainingTopics struct {
	DataRegistry  string
	Inference     string
	ModelRegistry string
	Training      string
}

type datasetUpdatedSubscriber struct {
	subscriber           msgConn.Subscriber
	starter              TrainingWorkflowStarter
	topics               TrainingTopics
	baseModel            string
	profile              model.TrainingProfile
	evaluationProfile    string
	dpoEvaluationProfile string
}

func NewDatasetUpdatedSubscriber(subscriber msgConn.Subscriber, starter TrainingWorkflowStarter, topics TrainingTopics, baseModel string, profile model.TrainingProfile, evaluationProfile string, dpoEvaluationProfile string) DatasetUpdatedSubscriber {
	log.Trace("NewDatasetUpdatedSubscriber")

	return &datasetUpdatedSubscriber{
		subscriber:           subscriber,
		starter:              starter,
		topics:               topics,
		baseModel:            baseModel,
		profile:              profile,
		evaluationProfile:    evaluationProfile,
		dpoEvaluationProfile: dpoEvaluationProfile,
	}
}

func (s *datasetUpdatedSubscriber) Start(ctx context.Context) error {
	log.Trace("datasetUpdatedSubscriber Start")

	msgConn.AddListener(s.subscriber, NewDatasetUpdatedEventListener(s.starter, s.baseModel, s.profile, s.evaluationProfile))
	msgConn.AddListener(s.subscriber, NewPreferenceDatasetReadyEventListener(s.starter, s.baseModel, s.profile, s.dpoEvaluationProfile))
	return s.subscriber.Subscribe(ctx, s.topics.List())
}

func (t TrainingTopics) List() []string {
	log.Trace("TrainingTopics List")

	topics := make([]string, 0, 2)
	if strings.TrimSpace(t.DataRegistry) != "" {
		topics = append(topics, t.DataRegistry)
	}
	if strings.TrimSpace(t.Inference) != "" {
		topics = append(topics, t.Inference)
	}
	return topics
}

type datasetUpdatedEventListener struct {
	starter           TrainingWorkflowStarter
	baseModel         string
	profile           model.TrainingProfile
	evaluationProfile string
}

func NewDatasetUpdatedEventListener(starter TrainingWorkflowStarter, baseModel string, profile model.TrainingProfile, evaluationProfile string) *datasetUpdatedEventListener {
	log.Trace("NewDatasetUpdatedEventListener")

	return &datasetUpdatedEventListener{
		starter:           starter,
		baseModel:         baseModel,
		profile:           profile,
		evaluationProfile: evaluationProfile,
	}
}

func (l *datasetUpdatedEventListener) MsgType() msgConn.MsgType {
	log.Trace("datasetUpdatedEventListener MsgType")

	return msgConn.MsgTypeDatasetUpdated
}

func (l *datasetUpdatedEventListener) NewMessage() *datasetpb.DatasetUpdatedEvent {
	log.Trace("datasetUpdatedEventListener NewMessage")

	return &datasetpb.DatasetUpdatedEvent{}
}

func (l *datasetUpdatedEventListener) Handle(ctx context.Context, resourceKey uuid.UUID, payload *datasetpb.DatasetUpdatedEvent) error {
	log.Trace("datasetUpdatedEventListener Handle")

	if l.starter == nil {
		return msgConn.NonRetryable(fmt.Errorf("training workflow starter is nil"))
	}
	request, shouldStart, err := datasetUpdatedToTrainingRunRequest(resourceKey, payload, l.baseModel, l.profile, l.evaluationProfile)
	if err != nil {
		return msgConn.NonRetryable(err)
	}
	if !shouldStart {
		return nil
	}
	return l.starter.StartTrainingWorkflow(ctx, request)
}

func datasetUpdatedToTrainingRunRequest(resourceKey uuid.UUID, payload *datasetpb.DatasetUpdatedEvent, baseModel string, profile model.TrainingProfile, evaluationProfile string) (model.TrainingRunRequest, bool, error) {
	log.Trace("datasetUpdatedToTrainingRunRequest")

	if resourceKey == uuid.Nil {
		return model.TrainingRunRequest{}, false, fmt.Errorf("resource key is required")
	}
	if payload == nil {
		return model.TrainingRunRequest{}, false, fmt.Errorf("dataset updated payload is required")
	}
	datasetID, err := msgConn.ParseUUID("dataset_id", payload.GetDatasetId())
	if err != nil {
		return model.TrainingRunRequest{}, false, err
	}
	if datasetID != resourceKey {
		return model.TrainingRunRequest{}, false, fmt.Errorf("dataset id %s does not match resource key %s", datasetID, resourceKey)
	}
	userID, err := msgConn.ParseUUID("user_id", payload.GetUserId())
	if err != nil {
		return model.TrainingRunRequest{}, false, err
	}

	state := strings.TrimSpace(payload.GetProcessingState())
	if state != "FEATURE_MATERIALIZED" && state != "EMBEDDINGS_MATERIALIZED" {
		return model.TrainingRunRequest{}, false, nil
	}
	if strings.TrimSpace(payload.GetTableFormat()) != "PARQUET" {
		return model.TrainingRunRequest{}, false, fmt.Errorf("training requires PARQUET dataset updates")
	}
	featureSnapshotID, err := msgConn.ParseUUID("feature_snapshot_id", payload.GetFeatureSnapshotId())
	if err != nil {
		return model.TrainingRunRequest{}, false, err
	}
	trainingRunID := uuid.NewSHA1(uuid.NameSpaceURL, []byte(fmt.Sprintf("training:%s:%s:%d", datasetID, featureSnapshotID, payload.GetDatasetVersion())))
	modelName := strings.TrimSpace(payload.GetTableName())
	if modelName == "" {
		return model.TrainingRunRequest{}, false, fmt.Errorf("table name is required")
	}
	if strings.TrimSpace(baseModel) == "" {
		return model.TrainingRunRequest{}, false, fmt.Errorf("base model is required")
	}
	profile = resolveTrainingProfile(profile, datasetID, payload.GetDatasetVersion(), featureSnapshotID)
	evaluationProfile = strings.TrimSpace(resolveTemplate(evaluationProfile, datasetID, payload.GetDatasetVersion(), featureSnapshotID))
	if evaluationProfile == "" {
		evaluationProfile = "smoke"
	}
	return model.TrainingRunRequest{
		TrainingRunID:     trainingRunID.String(),
		UserID:            userID.String(),
		DatasetID:         datasetID.String(),
		DatasetVersion:    fmt.Sprintf("%d", payload.GetDatasetVersion()),
		FeatureSnapshotID: featureSnapshotID.String(),
		ModelName:         modelName,
		ModelVersion:      fmt.Sprintf("%d", payload.GetDatasetVersion()),
		BaseModel:         baseModel,
		EvaluationProfile: evaluationProfile,
		TrainingProfile:   profile,
	}, true, nil
}

func resolveTrainingProfile(profile model.TrainingProfile, datasetID uuid.UUID, datasetVersion int32, featureSnapshotID uuid.UUID) model.TrainingProfile {
	log.Trace("resolveTrainingProfile")

	profile.PreferenceDatasetURI = resolveTemplate(profile.PreferenceDatasetURI, datasetID, datasetVersion, featureSnapshotID)
	return profile
}

func resolveTemplate(value string, datasetID uuid.UUID, datasetVersion int32, featureSnapshotID uuid.UUID) string {
	log.Trace("resolveTemplate")

	rendered := strings.TrimSpace(value)
	rendered = strings.ReplaceAll(rendered, "{dataset_id}", datasetID.String())
	rendered = strings.ReplaceAll(rendered, "{dataset_version}", fmt.Sprintf("%d", datasetVersion))
	rendered = strings.ReplaceAll(rendered, "{feature_snapshot_id}", featureSnapshotID.String())
	return rendered
}

type preferenceDatasetReadyEventListener struct {
	starter           TrainingWorkflowStarter
	baseModel         string
	profile           model.TrainingProfile
	evaluationProfile string
}

func NewPreferenceDatasetReadyEventListener(starter TrainingWorkflowStarter, baseModel string, profile model.TrainingProfile, evaluationProfile string) *preferenceDatasetReadyEventListener {
	log.Trace("NewPreferenceDatasetReadyEventListener")

	return &preferenceDatasetReadyEventListener{
		starter:           starter,
		baseModel:         baseModel,
		profile:           profile,
		evaluationProfile: evaluationProfile,
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
	request, err := preferenceDatasetReadyToTrainingRunRequest(resourceKey, payload, l.baseModel, l.profile, l.evaluationProfile)
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

func preferenceDatasetReadyToTrainingRunRequest(resourceKey uuid.UUID, payload *inferencepb.PreferenceDatasetReadyEvent, baseModel string, profile model.TrainingProfile, evaluationProfile string) (model.TrainingRunRequest, error) {
	log.Trace("preferenceDatasetReadyToTrainingRunRequest")

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
	parentAdapterURI := strings.TrimSpace(payload.GetParentAdapterUri())
	if parentAdapterURI == "" {
		return model.TrainingRunRequest{}, fmt.Errorf("%w: parent adapter uri is required", errPreferenceDatasetParentNotReady)
	}
	parentModelVersion := payload.GetParentModelVersion()
	if parentModelVersion <= 0 {
		return model.TrainingRunRequest{}, fmt.Errorf("%w: parent model version is required", errPreferenceDatasetParentNotReady)
	}
	parentBaseModel := strings.TrimSpace(payload.GetParentBaseModel())
	if parentBaseModel != "" {
		baseModel = parentBaseModel
	}
	if strings.TrimSpace(baseModel) == "" {
		return model.TrainingRunRequest{}, fmt.Errorf("base model is required")
	}
	profile.Trainer = "dpo"
	profile.PreferenceDatasetURI = outputURI
	if strings.TrimSpace(profile.Name) == "" {
		profile.Name = "dpo"
	}
	trainingRunID := uuid.NewSHA1(uuid.NameSpaceURL, []byte(strings.Join([]string{
		"dpo",
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
		DatasetID:            datasetID.String(),
		DatasetVersion:       "",
		PreferenceDatasetID:  preferenceDatasetID.String(),
		PreferenceDatasetURI: outputURI,
		ParentModelID:        modelID.String(),
		ParentModelVersion:   fmt.Sprintf("%d", parentModelVersion),
		ParentAdapterURI:     parentAdapterURI,
		ModelName:            "dpo-" + modelID.String(),
		ModelVersion:         modelVersion,
		BaseModel:            baseModel,
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
