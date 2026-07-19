package training

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"agent_registry_service/pkg/domain"
	"agent_registry_service/pkg/domain/model"
	trainingadapter "agent_registry_service/pkg/infra/network/adapter"
	"lib/shared_lib/authz"
	serializers "lib/shared_lib/serializer"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

const trainingServiceAgentAdapterTrainingDispatcherName = "training-service-agent-adapter"

type TrainingServiceAgentAdapterDispatcherConfig struct {
	BaseURL        string
	RequestTimeout time.Duration
}

type TrainingServiceAgentAdapterDispatcher struct {
	baseURL string
	client  *http.Client
	adapter trainingadapter.AgentAdapterTrainingDTOAdapter
}

func NewTrainingServiceAgentAdapterDispatcher(config TrainingServiceAgentAdapterDispatcherConfig) *TrainingServiceAgentAdapterDispatcher {
	log.Trace("NewTrainingServiceAgentAdapterDispatcher")

	return NewTrainingServiceAgentAdapterDispatcherWithClient(config, &http.Client{Timeout: config.RequestTimeout}, trainingadapter.NewAgentAdapterTrainingDTOAdapter(serializers.NewJSONSerializer()))
}

func NewTrainingServiceAgentAdapterDispatcherWithClient(config TrainingServiceAgentAdapterDispatcherConfig, client *http.Client, adapter trainingadapter.AgentAdapterTrainingDTOAdapter) *TrainingServiceAgentAdapterDispatcher {
	log.Trace("NewTrainingServiceAgentAdapterDispatcherWithClient")

	return &TrainingServiceAgentAdapterDispatcher{
		baseURL: strings.TrimRight(strings.TrimSpace(config.BaseURL), "/"),
		client:  client,
		adapter: adapter,
	}
}

func (t *TrainingServiceAgentAdapterDispatcher) DispatchAgentAdapterTraining(ctx context.Context, request model.AgentAdapterTrainingRequest) (*model.AgentAdapterTrainingResult, error) {
	log.Trace("TrainingServiceAgentAdapterDispatcher DispatchAgentAdapterTraining")

	if t == nil || t.client == nil || strings.TrimSpace(t.baseURL) == "" {
		return nil, domain.ErrAgentTrainingFailed.Extend("training service agent adapter dispatcher is not configured")
	}
	requestID := agentTrainingRequestID(request)
	raw, err := t.adapter.ToStartAgentAdapterTrainingRunDTO(ctx, request)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.baseURL+"/v1/training-runs/agent-adapter", bytes.NewReader(raw))
	if err != nil {
		return nil, domain.ErrAgentTrainingFailed.Extend(err.Error())
	}
	req.Header.Set(authz.HeaderOrgID, request.OrgID.String())
	req.Header.Set(authz.HeaderUserID, request.UserID.String())
	req.Header.Set("X-Request-ID", requestID.String())
	req.Header.Set("Content-Type", "application/json")
	resp, err := t.client.Do(req)
	if err != nil {
		return nil, domain.ErrAgentTrainingFailed.Extend(err.Error())
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, domain.ErrAgentTrainingFailed.Extend(fmt.Sprintf("training service returned status %d", resp.StatusCode))
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, domain.ErrAgentTrainingFailed.Extend(err.Error())
	}
	trainingRunID, err := t.adapter.FromStartAgentAdapterTrainingRunResponseDTO(ctx, body)
	if err != nil {
		return nil, err
	}
	return &model.AgentAdapterTrainingResult{
		TrainingRunID:    trainingRunID,
		TrainingProvider: trainingServiceAgentAdapterTrainingDispatcherName,
	}, nil
}

func agentTrainingRequestID(request model.AgentAdapterTrainingRequest) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(strings.Join([]string{
		"agent-adapter-training",
		request.OrgID.String(),
		request.UserID.String(),
		request.AgentLineage,
		request.DatasetID.String(),
		request.ContentHash,
		request.SourceModelID.String(),
		request.TrainingProfile,
	}, ":")))
}
