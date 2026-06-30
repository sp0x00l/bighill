package domain

import "errors"

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
)
