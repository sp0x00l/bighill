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
	ErrToolCatalogValidation = &ServiceError{Code: "tool_catalog_validation", Message: "tool catalog validation failed"}
	ErrCapabilityNotFound    = &ServiceError{Code: "tool_capability_not_found", Message: "tool capability not found"}
)
