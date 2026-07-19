package domain

import (
	"errors"
	"fmt"
)

type ServiceError struct {
	Code    string
	Message string
}

func (e *ServiceError) Error() string {
	return e.Message
}

func (e *ServiceError) Extend(message string) *ServiceError {
	return &ServiceError{
		Code:    e.Code,
		Message: fmt.Sprintf("%s: %s", e.Message, message),
	}
}

func (e *ServiceError) Is(target error) bool {
	var serviceError *ServiceError
	if !errors.As(target, &serviceError) {
		return false
	}
	return e.Code == serviceError.Code
}

var (
	ErrAgentRegistryValidation   = &ServiceError{Code: "agent_registry_validation", Message: "agent registry validation failed"}
	ErrAgentSpecUnavailable      = &ServiceError{Code: "agent_spec_unavailable", Message: "agent spec unavailable"}
	ErrEndpointUnavailable       = &ServiceError{Code: "agent_endpoint_unavailable", Message: "agent endpoint unavailable"}
	ErrAgentVersionNotFound      = &ServiceError{Code: "agent_version_not_found", Message: "agent version not found"}
	ErrGoldenTaskLeak            = &ServiceError{Code: "golden_task_leak", Message: "golden task leakage detected"}
	ErrAgentEvalFailed           = &ServiceError{Code: "agent_eval_failed", Message: "agent eval failed"}
	ErrAgentLabelNotFound        = &ServiceError{Code: "agent_label_not_found", Message: "agent run label not found"}
	ErrTrajectoryDatasetNotFound = &ServiceError{Code: "agent_trajectory_dataset_not_found", Message: "agent trajectory dataset not found"}
	ErrAgentAdapterNotFound      = &ServiceError{Code: "agent_adapter_not_found", Message: "agent adapter not found"}
	ErrAgentChampionNotFound     = &ServiceError{Code: "agent_champion_not_found", Message: "agent champion not found"}
	ErrAgentTrainingFailed       = &ServiceError{Code: "agent_training_failed", Message: "agent adapter training failed"}
	ErrAgentPromotionFailed      = &ServiceError{Code: "agent_promotion_failed", Message: "agent adapter promotion failed"}
)
