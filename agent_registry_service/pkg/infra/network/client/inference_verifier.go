package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"agent_registry_service/pkg/domain"
	"agent_registry_service/pkg/domain/model"
	"lib/shared_lib/authz"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type InferenceVerifierConfig struct {
	BaseURL        string
	RequestTimeout time.Duration
	PollInterval   time.Duration
	PollAttempts   int
}

type InferenceVerifier struct {
	baseURL      string
	client       *http.Client
	pollInterval time.Duration
	pollAttempts int
	adapter      inferenceVerifierDTOAdapter
}

func NewInferenceVerifier(config InferenceVerifierConfig) *InferenceVerifier {
	log.Trace("NewInferenceVerifier")

	return NewInferenceVerifierWithClient(config, &http.Client{Timeout: config.RequestTimeout})
}

func NewInferenceVerifierWithClient(config InferenceVerifierConfig, client *http.Client) *InferenceVerifier {
	log.Trace("NewInferenceVerifierWithClient")

	return &InferenceVerifier{
		baseURL:      strings.TrimRight(strings.TrimSpace(config.BaseURL), "/"),
		client:       client,
		pollInterval: config.PollInterval,
		pollAttempts: config.PollAttempts,
		adapter:      newInferenceVerifierDTOAdapter(),
	}
}

func (v *InferenceVerifier) ReadAgentSpec(ctx context.Context, orgID uuid.UUID, userID uuid.UUID, agentSpecHash string) (*model.AgentSpecRef, error) {
	log.Trace("InferenceVerifier ReadAgentSpec")

	var dtos []agentSpecDTO
	if err := v.get(ctx, orgID, userID, "/v1/inference/agent-specs/"+url.PathEscape(agentSpecHash), domain.ErrAgentSpecUnavailable, &dtos); err != nil {
		return nil, err
	}
	if len(dtos) == 0 {
		return nil, domain.ErrAgentSpecUnavailable
	}
	return v.adapter.FromAgentSpecDTO(orgID, dtos[0])
}

func (v *InferenceVerifier) ReadEndpoint(ctx context.Context, orgID uuid.UUID, userID uuid.UUID, endpointID uuid.UUID) (*model.EndpointRef, error) {
	log.Trace("InferenceVerifier ReadEndpoint")

	var dtos []endpointDTO
	if err := v.get(ctx, orgID, userID, "/v1/inference/endpoints/"+endpointID.String(), domain.ErrEndpointUnavailable, &dtos); err != nil {
		return nil, err
	}
	if len(dtos) == 0 {
		return nil, domain.ErrEndpointUnavailable
	}
	return v.adapter.FromEndpointDTO(orgID, dtos[0])
}

func (v *InferenceVerifier) ReadAgentTrajectory(ctx context.Context, orgID uuid.UUID, userID uuid.UUID, runID uuid.UUID) (*model.AgentTrajectoryRef, error) {
	log.Trace("InferenceVerifier ReadAgentTrajectory")

	var dto agentTrajectoryDTO
	if err := v.get(ctx, orgID, userID, "/v1/inference/agent-runs/"+runID.String(), domain.ErrAgentEvalFailed, &dto); err != nil {
		return nil, err
	}
	record, err := v.adapter.FromAgentTrajectoryDTO(dto)
	if err != nil {
		return nil, err
	}
	return record, nil
}

func (v *InferenceVerifier) RunAgentTask(ctx context.Context, command model.AgentTaskRunCommand) (model.AgentTaskRunResult, error) {
	log.Trace("InferenceVerifier RunAgentTask")

	requestID := agentTaskRequestID(command)
	var response generateResponseDTO
	body := v.adapter.ToAgentTaskRunDTO(command)
	if err := v.post(ctx, command.OrgID, command.UserID, requestID,
		"/v1/inference/endpoints/"+command.EndpointID.String()+"/agent-eval-runs/"+url.PathEscape(command.AgentSpecHash),
		body,
		&response,
	); err != nil {
		return model.AgentTaskRunResult{}, err
	}
	runID, err := v.adapter.FromGenerateResponseDTO(response)
	if err != nil {
		return model.AgentTaskRunResult{}, err
	}
	return v.waitForAgentTrajectory(ctx, command.OrgID, command.UserID, runID)
}

func (v *InferenceVerifier) waitForAgentTrajectory(ctx context.Context, orgID uuid.UUID, userID uuid.UUID, runID uuid.UUID) (model.AgentTaskRunResult, error) {
	log.Trace("InferenceVerifier waitForAgentTrajectory")

	for attempt := 0; attempt < v.pollAttempts; attempt++ {
		var dto agentTrajectoryDTO
		if err := v.get(ctx, orgID, userID, "/v1/inference/agent-runs/"+runID.String(), domain.ErrAgentEvalFailed, &dto); err != nil {
			return model.AgentTaskRunResult{}, err
		}
		if !strings.EqualFold(dto.Run.Status, "RUNNING") && strings.TrimSpace(dto.Run.Status) != "" {
			return v.adapter.FromAgentTaskRunResultDTO(dto)
		}
		if attempt+1 < v.pollAttempts {
			timer := time.NewTimer(v.pollInterval)
			select {
			case <-ctx.Done():
				timer.Stop()
				return model.AgentTaskRunResult{}, ctx.Err()
			case <-timer.C:
			}
		}
	}
	return model.AgentTaskRunResult{}, domain.ErrAgentEvalFailed.Extend("inference eval run did not finish before polling deadline")
}

func (v *InferenceVerifier) get(ctx context.Context, orgID uuid.UUID, userID uuid.UUID, path string, failure *domain.ServiceError, target any) error {
	log.Trace("InferenceVerifier get")

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, v.baseURL+path, nil)
	if err != nil {
		return domain.ErrAgentRegistryValidation.Extend(err.Error())
	}
	req.Header.Set(authz.HeaderOrgID, orgID.String())
	req.Header.Set(authz.HeaderUserID, userID.String())
	resp, err := v.client.Do(req)
	if err != nil {
		return failure.Extend(err.Error())
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return failure
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return failure.Extend(fmt.Sprintf("inference verifier returned status %d", resp.StatusCode))
	}
	if err := json.NewDecoder(resp.Body).Decode(target); err != nil {
		return failure.Extend(err.Error())
	}
	return nil
}

func (v *InferenceVerifier) post(ctx context.Context, orgID uuid.UUID, userID uuid.UUID, requestID uuid.UUID, path string, body any, target any) error {
	log.Trace("InferenceVerifier post")

	raw, err := json.Marshal(body)
	if err != nil {
		return domain.ErrAgentRegistryValidation.Extend(err.Error())
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, v.baseURL+path, bytes.NewReader(raw))
	if err != nil {
		return domain.ErrAgentRegistryValidation.Extend(err.Error())
	}
	req.Header.Set(authz.HeaderOrgID, orgID.String())
	req.Header.Set(authz.HeaderUserID, userID.String())
	req.Header.Set("X-Request-ID", requestID.String())
	req.Header.Set("Content-Type", "application/json")
	resp, err := v.client.Do(req)
	if err != nil {
		return domain.ErrAgentEvalFailed.Extend(err.Error())
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return domain.ErrAgentEvalFailed.Extend(fmt.Sprintf("inference eval runner returned status %d", resp.StatusCode))
	}
	if err := json.NewDecoder(resp.Body).Decode(target); err != nil {
		return domain.ErrAgentEvalFailed.Extend(err.Error())
	}
	return nil
}

func agentTaskRequestID(command model.AgentTaskRunCommand) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(strings.Join([]string{
		"agent-eval-task",
		command.OrgID.String(),
		command.EndpointID.String(),
		command.AgentSpecHash,
		command.ServingModelID.String(),
		command.TaskID.String(),
	}, ":")))
}
