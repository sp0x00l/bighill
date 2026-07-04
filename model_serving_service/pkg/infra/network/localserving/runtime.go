package localserving

import (
	"context"
	"fmt"
	"strings"

	"model_serving_service/pkg/domain"
	"model_serving_service/pkg/domain/model"

	log "github.com/sirupsen/logrus"
)

type Runtime struct {
	namespace string
	port      int32
}

func NewRuntime(namespace string, port int32) *Runtime {
	log.Trace("localserving NewRuntime")

	return &Runtime{namespace: namespace, port: port}
}

func (r *Runtime) EnsureServedModel(ctx context.Context, servedModel *model.ServedModel) (*model.ServingRuntimeState, error) {
	log.Trace("localserving Runtime EnsureServedModel")

	if strings.TrimSpace(servedModel.BaseModel) == "" {
		return nil, domain.ErrValidationFailed.Extend("base model is required")
	}
	servingModel := strings.TrimSpace(servedModel.ServingModel)
	if servingModel == "" {
		servingModel = fmt.Sprintf("%s-v%d-%s", servedModel.Name, servedModel.ModelVersion, servedModel.ModelID.String()[:8])
	}
	servingTarget := strings.TrimSpace(servedModel.ServingTarget)
	if servingTarget == "" {
		servingTarget = fmt.Sprintf("http://local-model-serving.%s.local:%d", r.namespace, r.port)
	}
	return &model.ServingRuntimeState{
		Ready:         true,
		ServingTarget: servingTarget,
		ServingModel:  servingModel,
		ReadyReplicas: 1,
	}, nil
}
