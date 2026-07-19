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

func ServiceErrorCode(err error) string {
	var serviceError *ServiceError
	if !errors.As(err, &serviceError) {
		return ""
	}
	return serviceError.Code
}

var (
	ErrValidationFailed            = &ServiceError{Code: "validation_failed", Message: "validation failed"}
	ErrRawSnapshotMaterialize      = &ServiceError{Code: "raw_snapshot_materialize_failed", Message: "raw snapshot materialize failed"}
	ErrFeatureSnapshotBuild        = &ServiceError{Code: "feature_snapshot_build_failed", Message: "feature snapshot build failed"}
	ErrEmbeddingMaterialize        = &ServiceError{Code: "embedding_materialize_failed", Message: "embedding materialize failed"}
	ErrEmbeddingSearch             = &ServiceError{Code: "embedding_search_failed", Message: "embedding search failed"}
	ErrGraphMaterialize            = &ServiceError{Code: "graph_materialize_failed", Message: "graph materialize failed"}
	ErrGraphExtractionInvalid      = &ServiceError{Code: "graph_extraction_invalid_document", Message: "graph extraction document invalid"}
	ErrGraphSearch                 = &ServiceError{Code: "graph_search_failed", Message: "graph search failed"}
	ErrArtifactRead                = &ServiceError{Code: "artifact_read_failed", Message: "artifact read failed"}
	ErrArtifactWrite               = &ServiceError{Code: "artifact_write_failed", Message: "artifact write failed"}
	ErrCatalogRegister             = &ServiceError{Code: "catalog_register_failed", Message: "catalog register failed"}
	ErrRegistryUpdate              = &ServiceError{Code: "registry_update_failed", Message: "registry update failed"}
	ErrRawSnapshotNotFound         = &ServiceError{Code: "raw_snapshot_not_found", Message: "raw snapshot not found"}
	ErrFeatureSnapshotNotFound     = &ServiceError{Code: "feature_snapshot_not_found", Message: "feature snapshot not found"}
	ErrEmbeddingSnapshotNotFound   = &ServiceError{Code: "embedding_snapshot_not_found", Message: "embedding snapshot not found"}
	ErrGraphSnapshotNotFound       = &ServiceError{Code: "graph_snapshot_not_found", Message: "graph snapshot not found"}
	ErrRawSnapshotInProgress       = &ServiceError{Code: "raw_snapshot_in_progress", Message: "raw snapshot in progress"}
	ErrFeatureSnapshotInProgress   = &ServiceError{Code: "feature_snapshot_in_progress", Message: "feature snapshot in progress"}
	ErrEmbeddingSnapshotInProgress = &ServiceError{Code: "embedding_snapshot_in_progress", Message: "embedding snapshot in progress"}
	ErrGraphSnapshotInProgress     = &ServiceError{Code: "graph_snapshot_in_progress", Message: "graph snapshot in progress"}
)
