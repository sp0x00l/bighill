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
	ErrModelNotFound    = &ServiceError{Code: "model_not_found", Message: "model not found"}
	ErrModelNotReady    = &ServiceError{Code: "model_not_ready", Message: "model not ready"}
	ErrModelMismatch    = &ServiceError{Code: "model_mismatch", Message: "model mismatch"}
	ErrDatasetNotFound  = &ServiceError{Code: "dataset_not_found", Message: "dataset not found"}
	ErrDatasetNotReady  = &ServiceError{Code: "dataset_not_ready", Message: "dataset not ready"}
	ErrRetrievalFailed  = &ServiceError{Code: "retrieval_failed", Message: "retrieval failed"}
	ErrGenerationFailed = &ServiceError{Code: "generation_failed", Message: "generation failed"}
)
