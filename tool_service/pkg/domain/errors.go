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
	ErrValidationFailed = &ServiceError{Code: "validation_failed", Message: "validation failed"}
	ErrToolNotFound     = &ServiceError{Code: "tool_not_found", Message: "tool not found"}
	ErrToolDenied       = &ServiceError{Code: "tool_denied", Message: "tool denied"}
	ErrToolPolicy       = &ServiceError{Code: "tool_policy_violation", Message: "tool policy violation"}
	ErrToolExecution    = &ServiceError{Code: "tool_execution_failed", Message: "tool execution failed"}
)
