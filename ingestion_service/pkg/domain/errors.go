package domain

import (
	"errors"
	"fmt"
	"strings"
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
		Message: message,
	}
}

func (e *ServiceError) Is(target error) bool {
	var serviceError *ServiceError
	if !errors.As(target, &serviceError) {
		return false
	}
	return e.Code == serviceError.Code
}

func IsServiceError(err error, target *ServiceError) bool {
	return errors.Is(err, target)
}

var (
	ErrResourceAlreadyExists = &ServiceError{Code: "resource_already_exists", Message: "resource already exists"}
	ErrResourceNotFound      = &ServiceError{Code: "resource_not_found", Message: "resource not found"}
	ErrValidationFailed      = &ServiceError{Code: "validation_failed", Message: "validation failed"}
	ErrForbidden             = &ServiceError{Code: "forbidden", Message: "forbidden"}
	ErrDependencyNotReady    = &ServiceError{Code: "dependency_not_ready", Message: "dependency not ready"}
)

type ExternalProviderError struct {
	Provider   string
	StatusCode int
	Code       string
	Message    string
}

func (e *ExternalProviderError) Error() string {
	provider := strings.TrimSpace(e.Provider)
	if provider == "" {
		provider = "external provider"
	}
	message := strings.TrimSpace(e.Message)
	if message == "" {
		message = "request failed"
	}
	code := strings.TrimSpace(e.Code)
	if e.StatusCode > 0 && code != "" {
		return fmt.Sprintf("%s returned %d (%s): %s", provider, e.StatusCode, code, message)
	}
	if e.StatusCode > 0 {
		return fmt.Sprintf("%s returned %d: %s", provider, e.StatusCode, message)
	}
	if code != "" {
		return fmt.Sprintf("%s returned %s: %s", provider, code, message)
	}
	return fmt.Sprintf("%s request failed: %s", provider, message)
}
