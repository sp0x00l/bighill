package messaging

import (
	"context"
	"errors"
	"fmt"
	"strings"

	usecase "agent_registry_service/pkg/app"
	"agent_registry_service/pkg/domain"
	"agent_registry_service/pkg/domain/model"
	trainingpb "lib/data_contracts_lib/training"
	msgConn "lib/shared_lib/messaging"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type agentTrainingCompletedEventListener struct {
	usecase usecase.AgentRegistryUsecase
}

func NewAgentTrainingCompletedEventListener(usecase usecase.AgentRegistryUsecase) *agentTrainingCompletedEventListener {
	log.Trace("NewAgentTrainingCompletedEventListener")

	return &agentTrainingCompletedEventListener{usecase: usecase}
}

func (l *agentTrainingCompletedEventListener) MsgType() msgConn.MsgType {
	log.Trace("agentTrainingCompletedEventListener MsgType")

	return msgConn.MsgTypeModelTrainingCompleted
}

func (l *agentTrainingCompletedEventListener) NewMessage() *trainingpb.ModelTrainingCompletedEvent {
	log.Trace("agentTrainingCompletedEventListener NewMessage")

	return &trainingpb.ModelTrainingCompletedEvent{}
}

func (l *agentTrainingCompletedEventListener) Handle(ctx context.Context, _ uuid.UUID, payload *trainingpb.ModelTrainingCompletedEvent) error {
	log.Trace("agentTrainingCompletedEventListener Handle")

	completion, err := agentTrainingCompletedToModel(payload)
	if err != nil {
		return msgConn.NonRetryable(err)
	}
	_, err = l.usecase.RecordAgentAdapterTrainingCompleted(ctx, completion)
	if errors.Is(err, domain.ErrAgentAdapterNotFound) {
		return nil
	}
	return err
}

type agentTrainingFailedEventListener struct {
	usecase usecase.AgentRegistryUsecase
}

func NewAgentTrainingFailedEventListener(usecase usecase.AgentRegistryUsecase) *agentTrainingFailedEventListener {
	log.Trace("NewAgentTrainingFailedEventListener")

	return &agentTrainingFailedEventListener{usecase: usecase}
}

func (l *agentTrainingFailedEventListener) MsgType() msgConn.MsgType {
	log.Trace("agentTrainingFailedEventListener MsgType")

	return msgConn.MsgTypeModelTrainingFailed
}

func (l *agentTrainingFailedEventListener) NewMessage() *trainingpb.ModelTrainingFailedEvent {
	log.Trace("agentTrainingFailedEventListener NewMessage")

	return &trainingpb.ModelTrainingFailedEvent{}
}

func (l *agentTrainingFailedEventListener) Handle(ctx context.Context, _ uuid.UUID, payload *trainingpb.ModelTrainingFailedEvent) error {
	log.Trace("agentTrainingFailedEventListener Handle")

	failure, err := agentTrainingFailedToModel(payload)
	if err != nil {
		return msgConn.NonRetryable(err)
	}
	_, err = l.usecase.RecordAgentAdapterTrainingFailed(ctx, failure)
	if errors.Is(err, domain.ErrAgentAdapterNotFound) {
		return nil
	}
	return err
}

func agentTrainingCompletedToModel(payload *trainingpb.ModelTrainingCompletedEvent) (model.AgentAdapterTrainingCompletion, error) {
	log.Trace("agentTrainingCompletedToModel")

	if payload == nil {
		return model.AgentAdapterTrainingCompletion{}, fmt.Errorf("model training completed payload is required")
	}
	trainingRunID, err := msgConn.ParseUUID("training_run_id", payload.GetTrainingRunId())
	if err != nil {
		return model.AgentAdapterTrainingCompletion{}, err
	}
	orgID, err := msgConn.ParseUUID("org_id", payload.GetOrgId())
	if err != nil {
		return model.AgentAdapterTrainingCompletion{}, err
	}
	servingModelID, err := msgConn.ParseUUID("model_id", payload.GetModelId())
	if err != nil {
		return model.AgentAdapterTrainingCompletion{}, err
	}
	adapterURI := strings.TrimSpace(payload.GetAdapterUri())
	if adapterURI == "" {
		return model.AgentAdapterTrainingCompletion{}, fmt.Errorf("adapter_uri is required")
	}
	checksum := strings.TrimSpace(payload.GetArtifactChecksum())
	if checksum == "" {
		return model.AgentAdapterTrainingCompletion{}, fmt.Errorf("artifact_checksum is required")
	}
	return model.AgentAdapterTrainingCompletion{
		TrainingRunID:    trainingRunID,
		OrgID:            orgID,
		ServingModelID:   servingModelID,
		AdapterURI:       adapterURI,
		AdapterChecksum:  checksum,
		TrainingProvider: "training-service-agent-adapter",
	}, nil
}

func agentTrainingFailedToModel(payload *trainingpb.ModelTrainingFailedEvent) (model.AgentAdapterTrainingFailure, error) {
	log.Trace("agentTrainingFailedToModel")

	if payload == nil {
		return model.AgentAdapterTrainingFailure{}, fmt.Errorf("model training failed payload is required")
	}
	trainingRunID, err := msgConn.ParseUUID("training_run_id", payload.GetTrainingRunId())
	if err != nil {
		return model.AgentAdapterTrainingFailure{}, err
	}
	orgID, err := msgConn.ParseUUID("org_id", payload.GetOrgId())
	if err != nil {
		return model.AgentAdapterTrainingFailure{}, err
	}
	failureReason := strings.TrimSpace(payload.GetFailureReason())
	if failureReason == "" {
		return model.AgentAdapterTrainingFailure{}, fmt.Errorf("failure_reason is required")
	}
	return model.AgentAdapterTrainingFailure{
		TrainingRunID: trainingRunID,
		OrgID:         orgID,
		FailureReason: failureReason,
	}, nil
}
