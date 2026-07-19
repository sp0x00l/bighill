package messaging

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	usecase "inference_service/pkg/app"
	"inference_service/pkg/domain/model"

	agentregistrypb "lib/data_contracts_lib/agent_registry"
	datasetpb "lib/data_contracts_lib/data_registry"
	modelregistrypb "lib/data_contracts_lib/model_registry"
	msgConn "lib/shared_lib/messaging"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type InferenceTopics struct {
	ModelRegistry     string
	AgentRegistry     string
	DataRegistry      string
	PreferenceDataset string
}

func (t InferenceTopics) List() []string {
	log.Trace("InferenceTopics List")

	topics := make([]string, 0, 2)
	if strings.TrimSpace(t.ModelRegistry) != "" {
		topics = append(topics, t.ModelRegistry)
	}
	if strings.TrimSpace(t.AgentRegistry) != "" {
		topics = append(topics, t.AgentRegistry)
	}
	if strings.TrimSpace(t.DataRegistry) != "" {
		topics = append(topics, t.DataRegistry)
	}
	return topics
}

type ModelUpdatedSubscriber interface {
	Start(ctx context.Context) error
}

type modelUpdatedSubscriber struct {
	subscriber msgConn.Subscriber
	usecase    usecase.InferenceUsecase
	topics     InferenceTopics
}

func NewModelUpdatedSubscriber(subscriber msgConn.Subscriber, usecase usecase.InferenceUsecase, topics InferenceTopics) ModelUpdatedSubscriber {
	log.Trace("NewModelUpdatedSubscriber")

	return &modelUpdatedSubscriber{
		subscriber: subscriber,
		usecase:    usecase,
		topics:     topics,
	}
}

func (s *modelUpdatedSubscriber) Start(ctx context.Context) error {
	log.Trace("modelUpdatedSubscriber Start")

	msgConn.AddListener(s.subscriber, NewModelUpdatedEventListener(s.usecase))
	msgConn.AddListener(s.subscriber, NewDatasetUpdatedEventListener(s.usecase))
	msgConn.AddListener(s.subscriber, NewAgentChampionUpdatedEventListener(s.usecase))
	return s.subscriber.Subscribe(ctx, s.topics.List())
}

type modelUpdatedEventListener struct {
	usecase usecase.InferenceUsecase
}

func NewModelUpdatedEventListener(usecase usecase.InferenceUsecase) *modelUpdatedEventListener {
	log.Trace("NewModelUpdatedEventListener")

	return &modelUpdatedEventListener{
		usecase: usecase,
	}
}

func (l *modelUpdatedEventListener) MsgType() msgConn.MsgType {
	log.Trace("modelUpdatedEventListener MsgType")

	return msgConn.MsgTypeModelUpdated
}

func (l *modelUpdatedEventListener) NewMessage() *modelregistrypb.ModelUpdatedEvent {
	log.Trace("modelUpdatedEventListener NewMessage")

	return &modelregistrypb.ModelUpdatedEvent{}
}

func (l *modelUpdatedEventListener) Handle(ctx context.Context, resourceKey uuid.UUID, payload *modelregistrypb.ModelUpdatedEvent) error {
	log.Trace("modelUpdatedEventListener Handle")

	inferenceModel, idempotencyKey, err := modelUpdatedEventToModel(resourceKey, payload)
	if err != nil {
		return msgConn.NonRetryable(err)
	}
	_, err = l.usecase.RecordModelUpdated(ctx, inferenceModel, idempotencyKey)
	return err
}

type datasetUpdatedEventListener struct {
	usecase usecase.InferenceUsecase
}

func NewDatasetUpdatedEventListener(usecase usecase.InferenceUsecase) *datasetUpdatedEventListener {
	log.Trace("NewDatasetUpdatedEventListener")

	return &datasetUpdatedEventListener{
		usecase: usecase,
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

	dataset, idempotencyKey, err := datasetUpdatedEventToModel(resourceKey, payload)
	if err != nil {
		return msgConn.NonRetryable(err)
	}
	_, err = l.usecase.RecordDatasetUpdated(ctx, dataset, idempotencyKey)
	return err
}

type agentChampionUpdatedEventListener struct {
	usecase usecase.InferenceUsecase
}

func NewAgentChampionUpdatedEventListener(usecase usecase.InferenceUsecase) *agentChampionUpdatedEventListener {
	log.Trace("NewAgentChampionUpdatedEventListener")

	return &agentChampionUpdatedEventListener{
		usecase: usecase,
	}
}

func (l *agentChampionUpdatedEventListener) MsgType() msgConn.MsgType {
	log.Trace("agentChampionUpdatedEventListener MsgType")

	return msgConn.MsgTypeAgentChampionUpdated
}

func (l *agentChampionUpdatedEventListener) NewMessage() *agentregistrypb.AgentChampionUpdatedEvent {
	log.Trace("agentChampionUpdatedEventListener NewMessage")

	return &agentregistrypb.AgentChampionUpdatedEvent{}
}

func (l *agentChampionUpdatedEventListener) Handle(ctx context.Context, resourceKey uuid.UUID, payload *agentregistrypb.AgentChampionUpdatedEvent) error {
	log.Trace("agentChampionUpdatedEventListener Handle")

	update, err := agentChampionUpdatedEventToModel(resourceKey, payload)
	if err != nil {
		return msgConn.NonRetryable(err)
	}
	_, err = l.usecase.ApplyAgentChampionUpdate(ctx, update)
	return err
}

func modelUpdatedEventToModel(resourceKey uuid.UUID, payload *modelregistrypb.ModelUpdatedEvent) (*model.InferenceModel, uuid.UUID, error) {
	log.Trace("modelUpdatedEventToModel")

	if resourceKey == uuid.Nil {
		return nil, uuid.Nil, fmt.Errorf("resource key is required")
	}
	if payload == nil {
		return nil, uuid.Nil, fmt.Errorf("model updated payload is required")
	}
	modelID, err := msgConn.ParseUUID("model_id", payload.GetModelId())
	if err != nil {
		return nil, uuid.Nil, err
	}
	if modelID != resourceKey {
		return nil, uuid.Nil, fmt.Errorf("model id %s does not match resource key %s", modelID, resourceKey)
	}
	trainingRunID, err := msgConn.ParseOptionalUUID("training_run_id", payload.GetTrainingRunId())
	if err != nil {
		return nil, uuid.Nil, err
	}
	datasetID, err := msgConn.ParseOptionalUUID("dataset_id", payload.GetDatasetId())
	if err != nil {
		return nil, uuid.Nil, err
	}
	modelKind := modelKindFromEvent(payload.GetModelKind())
	userID, err := msgConn.ParseOptionalUUID("user_id", payload.GetUserId())
	if err != nil {
		return nil, uuid.Nil, err
	}
	orgID, err := msgConn.ParseOptionalUUID("org_id", payload.GetOrgId())
	if err != nil {
		return nil, uuid.Nil, err
	}
	if userID == uuid.Nil {
		return nil, uuid.Nil, fmt.Errorf("user id is required")
	}
	if orgID == uuid.Nil {
		return nil, uuid.Nil, fmt.Errorf("org id is required")
	}
	status, err := model.ToModelStatus(strings.TrimSpace(payload.GetStatus()))
	if err != nil {
		return nil, uuid.Nil, err
	}
	servingLoadStatus, err := model.ToModelLoadStatus(strings.TrimSpace(payload.GetServingLoadStatus()))
	if err != nil {
		return nil, uuid.Nil, err
	}
	servingProtocol, err := model.ToServingProtocol(strings.TrimSpace(payload.GetServingProtocol()))
	if err != nil {
		return nil, uuid.Nil, err
	}
	inferenceModel := &model.InferenceModel{
		ModelID:           modelID,
		UserID:            userID,
		OrgID:             orgID,
		TrainingRunID:     trainingRunID,
		DatasetID:         datasetID,
		ModelKind:         modelKind,
		Source:            modelSourceFromEvent(payload.GetSource()),
		SourceURI:         strings.TrimSpace(payload.GetSourceUri()),
		SourceMetadata:    strings.TrimSpace(payload.GetSourceMetadata()),
		Name:              strings.TrimSpace(payload.GetName()),
		LineageName:       lineageNameFromModelEvent(payload.GetLineageName(), payload.GetName()),
		ModelVersion:      int(payload.GetModelVersion()),
		BaseModel:         strings.TrimSpace(payload.GetBaseModel()),
		ArtifactLocation:  strings.TrimSpace(payload.GetArtifactLocation()),
		ArtifactFormat:    strings.TrimSpace(payload.GetArtifactFormat()),
		ArtifactChecksum:  strings.TrimSpace(payload.GetArtifactChecksum()),
		ArtifactSizeBytes: payload.GetArtifactSizeBytes(),
		AdapterURI:        strings.TrimSpace(payload.GetAdapterUri()),
		ServingTarget:     strings.TrimSpace(payload.GetServingTarget()),
		ServingModel:      strings.TrimSpace(payload.GetServingModel()),
		ServingProtocol:   servingProtocol,
		ServingLoadStatus: servingLoadStatus,
		EffectiveBaseID:   strings.TrimSpace(payload.GetEffectiveBaseId()),
		MetricsMetadata:   withDefaultJSON(payload.GetMetricsMetadata()),
		Status:            status,
		FailureReason:     strings.TrimSpace(payload.GetFailureReason()),
	}
	if err := validateModelUpdatedEvent(inferenceModel); err != nil {
		return nil, uuid.Nil, err
	}
	idempotencyKey := uuid.NewSHA1(uuid.NameSpaceURL, []byte(strings.Join([]string{
		modelID.String(),
		trainingRunID.String(),
		datasetID.String(),
		inferenceModel.ModelKind.String(),
		inferenceModel.Source.String(),
		status.String(),
		inferenceModel.ArtifactChecksum,
		inferenceModel.ServingProtocol.String(),
		inferenceModel.EffectiveBaseID,
	}, ":")))
	return inferenceModel, idempotencyKey, nil
}

func agentChampionUpdatedEventToModel(resourceKey uuid.UUID, payload *agentregistrypb.AgentChampionUpdatedEvent) (model.AgentChampionUpdate, error) {
	log.Trace("agentChampionUpdatedEventToModel")

	if resourceKey == uuid.Nil {
		return model.AgentChampionUpdate{}, fmt.Errorf("resource key is required")
	}
	if payload == nil {
		return model.AgentChampionUpdate{}, fmt.Errorf("agent champion updated payload is required")
	}
	endpointID, err := msgConn.ParseUUID("endpoint_id", payload.GetEndpointId())
	if err != nil {
		return model.AgentChampionUpdate{}, err
	}
	if endpointID != resourceKey {
		return model.AgentChampionUpdate{}, fmt.Errorf("endpoint id %s does not match resource key %s", endpointID, resourceKey)
	}
	orgID, err := msgConn.ParseUUID("org_id", payload.GetOrgId())
	if err != nil {
		return model.AgentChampionUpdate{}, err
	}
	decisionID, err := msgConn.ParseUUID("decision_id", payload.GetDecisionId())
	if err != nil {
		return model.AgentChampionUpdate{}, err
	}
	decidedAt, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(payload.GetDecidedAt()))
	if err != nil || decidedAt.IsZero() {
		return model.AgentChampionUpdate{}, fmt.Errorf("decided_at is invalid")
	}
	agentLineage := strings.TrimSpace(payload.GetAgentLineage())
	if agentLineage == "" {
		return model.AgentChampionUpdate{}, fmt.Errorf("agent_lineage is required")
	}
	agentSpecHash := strings.TrimSpace(payload.GetAgentSpecHash())
	if agentSpecHash == "" {
		return model.AgentChampionUpdate{}, fmt.Errorf("agent_spec_hash is required")
	}
	servingModelID, err := msgConn.ParseOptionalUUID("serving_model_id", payload.GetServingModelId())
	if err != nil {
		return model.AgentChampionUpdate{}, err
	}
	return model.AgentChampionUpdate{
		OrgID:                 orgID,
		EndpointID:            endpointID,
		AgentLineage:          agentLineage,
		AgentSpecHash:         agentSpecHash,
		ServingModelID:        servingModelID,
		PreviousAgentSpecHash: strings.TrimSpace(payload.GetPreviousAgentSpecHash()),
		DecisionID:            decisionID,
		DecidedAt:             decidedAt,
	}, nil
}

func modelKindFromEvent(value string) model.ModelKind {
	log.Trace("modelKindFromEvent")

	return model.ToModelKind(value)
}

func modelSourceFromEvent(value string) model.ModelSource {
	log.Trace("modelSourceFromEvent")

	return model.ToModelSource(value)
}

func lineageNameFromModelEvent(lineageName string, modelName string) string {
	log.Trace("lineageNameFromModelEvent")

	lineageName = strings.TrimSpace(lineageName)
	if lineageName != "" {
		return lineageName
	}
	return strings.TrimSpace(modelName)
}

func validateModelUpdatedEvent(inferenceModel *model.InferenceModel) error {
	log.Trace("validateModelUpdatedEvent")

	if strings.TrimSpace(inferenceModel.BaseModel) == "" {
		return fmt.Errorf("base model is required")
	}
	if strings.TrimSpace(inferenceModel.Name) == "" {
		return fmt.Errorf("model name is required")
	}
	if inferenceModel.ModelVersion <= 0 {
		return fmt.Errorf("model version is required")
	}
	if !model.IsKnownModelKind(inferenceModel.ModelKind) {
		return fmt.Errorf("model kind is invalid")
	}
	if !model.IsKnownModelSource(inferenceModel.Source) {
		return fmt.Errorf("model source is invalid")
	}
	if inferenceModel.Source.String() == model.ModelSourceTraining.String() && inferenceModel.ModelKind.String() != model.ModelKindBase.String() && inferenceModel.DatasetID == uuid.Nil {
		return fmt.Errorf("dataset id is required for training-sourced fine tuned models")
	}
	if inferenceModel.Source.String() == model.ModelSourceTraining.String() && inferenceModel.TrainingRunID == uuid.Nil {
		return fmt.Errorf("training run id is required for training-sourced models")
	}
	if strings.TrimSpace(inferenceModel.MetricsMetadata) == "" {
		return fmt.Errorf("metrics metadata is required")
	}
	if inferenceModel.Status == model.ModelStatusReady && strings.TrimSpace(inferenceModel.ArtifactLocation) == "" {
		return fmt.Errorf("artifact location is required for ready models")
	}
	if inferenceModel.Status == model.ModelStatusReady && inferenceModel.ServingLoadStatus != model.ModelLoadStatusLoaded {
		return fmt.Errorf("ready models must be loaded by the serving layer")
	}
	if inferenceModel.ServingLoadStatus == model.ModelLoadStatusLoaded && strings.TrimSpace(inferenceModel.ServingProtocol.String()) == "" {
		return fmt.Errorf("loaded models require a serving protocol")
	}
	if inferenceModel.Status == model.ModelStatusFailed && strings.TrimSpace(inferenceModel.FailureReason) == "" {
		return fmt.Errorf("failure reason is required for failed models")
	}
	return nil
}

func withDefaultJSON(value string) string {
	log.Trace("withDefaultJSON")

	if strings.TrimSpace(value) == "" {
		return "{}"
	}
	return strings.TrimSpace(value)
}

func datasetUpdatedEventToModel(resourceKey uuid.UUID, payload *datasetpb.DatasetUpdatedEvent) (*model.InferenceDataset, uuid.UUID, error) {
	log.Trace("datasetUpdatedEventToModel")

	if resourceKey == uuid.Nil {
		return nil, uuid.Nil, fmt.Errorf("resource key is required")
	}
	if payload == nil {
		return nil, uuid.Nil, fmt.Errorf("dataset updated payload is required")
	}
	datasetID, err := msgConn.ParseUUID("dataset_id", payload.GetDatasetId())
	if err != nil {
		return nil, uuid.Nil, err
	}
	if datasetID != resourceKey {
		return nil, uuid.Nil, fmt.Errorf("dataset id %s does not match resource key %s", datasetID, resourceKey)
	}
	userID, err := msgConn.ParseUUID("user_id", payload.GetUserId())
	if err != nil {
		return nil, uuid.Nil, err
	}
	orgID, err := msgConn.ParseUUID("org_id", payload.GetOrgId())
	if err != nil {
		return nil, uuid.Nil, err
	}
	processingState, err := model.ToDatasetProcessingState(strings.TrimSpace(payload.GetProcessingState()))
	if err != nil {
		return nil, uuid.Nil, err
	}
	processingProfile, err := model.ToProcessingProfile(strings.TrimSpace(payload.GetProcessingProfile()))
	if err != nil {
		return nil, uuid.Nil, err
	}
	rawSnapshotID, err := msgConn.ParseOptionalUUID("raw_snapshot_id", payload.GetRawSnapshotId())
	if err != nil {
		return nil, uuid.Nil, err
	}
	featureSnapshotID, err := msgConn.ParseOptionalUUID("feature_snapshot_id", payload.GetFeatureSnapshotId())
	if err != nil {
		return nil, uuid.Nil, err
	}
	embeddingSnapshotID, err := msgConn.ParseOptionalUUID("embedding_snapshot_id", payload.GetEmbeddingSnapshotId())
	if err != nil {
		return nil, uuid.Nil, err
	}
	graphSnapshotID, err := msgConn.ParseOptionalUUID("graph_snapshot_id", payload.GetGraphSnapshotId())
	if err != nil {
		return nil, uuid.Nil, err
	}

	dataset := &model.InferenceDataset{
		DatasetID:                datasetID,
		UserID:                   userID,
		OrgID:                    orgID,
		DatasetVersion:           int(payload.GetDatasetVersion()),
		ProcessingState:          processingState,
		StorageLocation:          strings.TrimSpace(payload.GetStorageLocation()),
		TableNamespace:           strings.TrimSpace(payload.GetTableNamespace()),
		TableName:                strings.TrimSpace(payload.GetTableName()),
		TableFormat:              strings.TrimSpace(payload.GetTableFormat()),
		CatalogProvider:          strings.TrimSpace(payload.GetCatalogProvider()),
		ProcessingProfile:        processingProfile.String(),
		SchemaVersion:            int(payload.GetSchemaVersion()),
		SchemaMetadata:           strings.TrimSpace(payload.GetSchemaMetadata()),
		RawSnapshotID:            rawSnapshotID,
		FeatureSnapshotID:        featureSnapshotID,
		EmbeddingSnapshotID:      embeddingSnapshotID,
		VectorStore:              strings.TrimSpace(payload.GetVectorStore()),
		CollectionName:           strings.TrimSpace(payload.GetCollectionName()),
		EmbeddingDimensions:      int(payload.GetEmbeddingDimensions()),
		EmbeddingCount:           payload.GetEmbeddingCount(),
		EmbeddingStrategyVersion: strings.TrimSpace(payload.GetEmbeddingStrategyVersion()),
		EmbeddingChunkerName:     strings.TrimSpace(payload.GetEmbeddingChunkerName()),
		EmbeddingChunkerVersion:  strings.TrimSpace(payload.GetEmbeddingChunkerVersion()),
		EmbeddingChunkSize:       int(payload.GetEmbeddingChunkSize()),
		EmbeddingChunkOverlap:    int(payload.GetEmbeddingChunkOverlap()),
		EmbeddingProvider:        strings.TrimSpace(payload.GetEmbeddingProvider()),
		EmbeddingModel:           strings.TrimSpace(payload.GetEmbeddingModel()),
		GraphSnapshotID:          graphSnapshotID,
		GraphProvenanceHash:      strings.TrimSpace(payload.GetGraphProvenanceHash()),
		GraphNodeCount:           payload.GetGraphNodeCount(),
		GraphEdgeCount:           payload.GetGraphEdgeCount(),
	}
	if err := validateDatasetUpdatedEvent(dataset); err != nil {
		return nil, uuid.Nil, err
	}

	idempotencyKey := uuid.NewSHA1(uuid.NameSpaceURL, []byte(strings.Join([]string{
		datasetID.String(),
		strconv.Itoa(dataset.DatasetVersion),
		processingState.String(),
		dataset.RawSnapshotID.String(),
		dataset.FeatureSnapshotID.String(),
		dataset.EmbeddingSnapshotID.String(),
		dataset.GraphSnapshotID.String(),
		dataset.GraphProvenanceHash,
	}, ":")))
	return dataset, idempotencyKey, nil
}

func validateDatasetUpdatedEvent(dataset *model.InferenceDataset) error {
	log.Trace("validateDatasetUpdatedEvent")

	if dataset.DatasetVersion <= 0 {
		return fmt.Errorf("dataset version is required")
	}
	if dataset.SchemaVersion <= 0 {
		return fmt.Errorf("schema version is required")
	}
	if strings.TrimSpace(dataset.SchemaMetadata) == "" {
		return fmt.Errorf("schema metadata is required")
	}
	if dataset.ProcessingState == model.DatasetProcessingGraphMaterialized {
		if dataset.GraphSnapshotID == uuid.Nil {
			return fmt.Errorf("graph snapshot id is required")
		}
		if strings.TrimSpace(dataset.GraphProvenanceHash) == "" {
			return fmt.Errorf("graph provenance hash is required")
		}
	}
	return nil
}
