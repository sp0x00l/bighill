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
	ErrValidationFailed          = &ServiceError{Code: "validation_failed", Message: "validation failed"}
	ErrRawSnapshotMaterialize    = &ServiceError{Code: "raw_snapshot_materialize_failed", Message: "raw snapshot materialize failed"}
	ErrFeatureSnapshotBuild      = &ServiceError{Code: "feature_snapshot_build_failed", Message: "feature snapshot build failed"}
	ErrEmbeddingMaterialize      = &ServiceError{Code: "embedding_materialize_failed", Message: "embedding materialize failed"}
	ErrRawSnapshotNotFound       = &ServiceError{Code: "raw_snapshot_not_found", Message: "raw snapshot not found"}
	ErrFeatureSnapshotNotFound   = &ServiceError{Code: "feature_snapshot_not_found", Message: "feature snapshot not found"}
	ErrEmbeddingSnapshotNotFound = &ServiceError{Code: "embedding_snapshot_not_found", Message: "embedding snapshot not found"}
)
