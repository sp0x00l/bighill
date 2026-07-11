package app

import (
	"context"
	"errors"

	"model_serving_service/pkg/domain"
	"model_serving_service/pkg/domain/model"

	log "github.com/sirupsen/logrus"
)

type ServedModelReconciler interface {
	Reconcile(ctx context.Context, servedModel *model.ServedModel) (*model.ServedModelStatus, error)
	Delete(ctx context.Context, servedModel *model.ServedModel) error
}

type servedModelReconciler struct {
	runtime      ServingRuntime
	statusWriter ServedModelStatusWriter
}

func NewServedModelReconciler(runtime ServingRuntime, statusWriter ServedModelStatusWriter) ServedModelReconciler {
	log.Trace("NewServedModelReconciler")

	return &servedModelReconciler{
		runtime:      runtime,
		statusWriter: statusWriter,
	}
}

func (r *servedModelReconciler) Reconcile(ctx context.Context, servedModel *model.ServedModel) (*model.ServedModelStatus, error) {
	log.Trace("ServedModelReconciler Reconcile")

	state, err := r.runtime.EnsureServedModel(ctx, servedModel)
	if err != nil {
		status := &model.ServedModelStatus{
			ServingLoadStatus:  model.ModelLoadStatusFailed,
			ServingTarget:      servedModel.ServingTarget,
			ServingModel:       servedModel.ServingModel,
			ServingProtocol:    servedModel.ServingProtocol,
			FailureReason:      err.Error(),
			ObservedGeneration: servedModel.Generation,
		}
		if statusErr := r.statusWriter.UpdateStatus(ctx, servedModel.ResourceName, status); statusErr != nil {
			return status, errors.Join(err, statusErr)
		}
		return status, domain.ErrModelServe.Extend(err.Error())
	}

	loadStatus := model.ModelLoadStatusNotLoaded
	if state.Ready {
		loadStatus = model.ModelLoadStatusLoaded
	} else if state.Failed {
		loadStatus = model.ModelLoadStatusFailed
	}
	status := &model.ServedModelStatus{
		ServingLoadStatus:  loadStatus,
		ServingTarget:      state.ServingTarget,
		ServingModel:       state.ServingModel,
		ServingProtocol:    state.ServingProtocol,
		FailureReason:      state.FailureReason,
		ObservedGeneration: servedModel.Generation,
		ReadyReplicas:      state.ReadyReplicas,
	}
	return status, r.statusWriter.UpdateStatus(ctx, servedModel.ResourceName, status)
}

func (r *servedModelReconciler) Delete(ctx context.Context, servedModel *model.ServedModel) error {
	log.Trace("ServedModelReconciler Delete")

	deletionRuntime, ok := r.runtime.(ServingRuntimeDeletion)
	if !ok {
		return nil
	}
	return deletionRuntime.DeleteServedModel(ctx, servedModel)
}
