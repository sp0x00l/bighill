package app

import (
	"context"

	"model_serving_service/pkg/domain/model"
)

type ServingRuntime interface {
	EnsureServedModel(ctx context.Context, servedModel *model.ServedModel) (*model.ServingRuntimeState, error)
}

type ServedModelStatusWriter interface {
	UpdateStatus(ctx context.Context, resourceName string, status *model.ServedModelStatus) error
}
