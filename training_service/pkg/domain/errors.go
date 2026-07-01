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
	ErrPrepareDataset   = &ServiceError{Code: "prepare_dataset_failed", Message: "prepare dataset failed"}
	ErrTrainModel       = &ServiceError{Code: "train_model_failed", Message: "train model failed"}
	ErrEvaluateModel    = &ServiceError{Code: "evaluate_model_failed", Message: "evaluate model failed"}
)
