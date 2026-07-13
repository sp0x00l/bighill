package domain

import (
	"errors"
	"fmt"

	log "github.com/sirupsen/logrus"
)

type ServiceError struct {
	Code    string
	Message string
}

func (e *ServiceError) Error() string {
	log.Trace("ServiceError Error")

	return e.Message
}

func (e *ServiceError) Extend(message string) *ServiceError {
	log.Trace("ServiceError Extend")

	return &ServiceError{
		Code:    e.Code,
		Message: fmt.Sprintf("%s: %s", e.Message, message),
	}
}

func (e *ServiceError) Is(target error) bool {
	log.Trace("ServiceError Is")

	var serviceError *ServiceError
	if !errors.As(target, &serviceError) {
		return false
	}
	return e.Code == serviceError.Code
}

var (
	ErrValidationFailed = &ServiceError{Code: "validation_failed", Message: "validation failed"}
	ErrUnauthorized     = &ServiceError{Code: "unauthorized", Message: "unauthorized"}
	ErrDependencyFailed = &ServiceError{Code: "dependency_failed", Message: "dependency failed"}
	ErrBackpressure     = &ServiceError{Code: "backpressure", Message: "backpressure"}
)

func IsServiceError(err error, target *ServiceError) bool {
	log.Trace("IsServiceError")

	return errors.Is(err, target)
}
