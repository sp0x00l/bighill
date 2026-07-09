package domain

type ServiceError struct {
	Code    string
	Message string
}

func (e *ServiceError) Error() string {
	return e.Message
}

func (e *ServiceError) Is(target error) bool {
	other, ok := target.(*ServiceError)
	return ok && e.Code == other.Code
}

func (e *ServiceError) Extend(message string) *ServiceError {
	return &ServiceError{Code: e.Code, Message: e.Message + ": " + message}
}

var (
	ErrValidationFailed    = &ServiceError{Code: "validation_failed", Message: "validation failed"}
	ErrModelServe          = &ServiceError{Code: "model_serve_failed", Message: "model serve failed"}
	ErrServedModelNotFound = &ServiceError{
		Code:    "served_model_not_found",
		Message: "served model not found",
	}
)
