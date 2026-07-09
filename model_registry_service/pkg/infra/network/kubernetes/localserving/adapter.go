package localserving

import (
	"context"
	"errors"
	"fmt"
	"time"

	"model_registry_service/pkg/app"
	"model_registry_service/pkg/domain"
	"model_registry_service/pkg/domain/model"

	localstore "lib/shared_lib/servedmodel"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type Adapter struct {
	namespace string
	store     *localstore.Store
}

type StatusObserver struct {
	adapter        *Adapter
	recorder       app.ServingStatusRecorder
	resyncInterval time.Duration
	seen           map[string]uuid.UUID
}

func NewAdapter(namespace string, path string) (*Adapter, error) {
	log.Trace("localserving NewAdapter")

	store, err := localstore.NewStore(path)
	if err != nil {
		return nil, err
	}
	return &Adapter{namespace: namespace, store: store}, nil
}

func (a *Adapter) EnsureServedModel(ctx context.Context, registeredModel *model.Model) error {
	log.Trace("localserving Adapter EnsureServedModel")

	return a.store.UpsertSpec(ServedModelName(registeredModel.ModelID, registeredModel.ModelVersion), a.namespace, localstore.Spec{
		ModelID:          registeredModel.ModelID.String(),
		TrainingRunID:    registeredModel.TrainingRunID.String(),
		DatasetID:        registeredModel.DatasetID.String(),
		ModelKind:        registeredModel.ModelKind.String(),
		Name:             registeredModel.Name,
		ModelVersion:     registeredModel.ModelVersion,
		BaseModel:        registeredModel.BaseModel,
		ArtifactLocation: registeredModel.ArtifactLocation,
		ArtifactFormat:   registeredModel.ArtifactFormat,
		ArtifactChecksum: registeredModel.ArtifactChecksum,
		AdapterURI:       registeredModel.AdapterURI,
		ServingTarget:    registeredModel.ServingTarget,
		ServingModel:     registeredModel.ServingModel,
		ServingProtocol:  registeredModel.ServingProtocol.String(),
	})
}

func ServedModelName(modelID uuid.UUID, modelVersion int) string {
	log.Trace("localserving ServedModelName")

	return localstore.ResourceName(modelID.String(), modelVersion)
}

func NewStatusObserver(adapter *Adapter, recorder app.ServingStatusRecorder, resyncInterval time.Duration) (*StatusObserver, error) {
	log.Trace("localserving NewStatusObserver")

	if adapter == nil {
		return nil, fmt.Errorf("local served model adapter is required")
	}
	if recorder == nil {
		return nil, fmt.Errorf("serving status recorder is required")
	}
	return &StatusObserver{
		adapter:        adapter,
		recorder:       recorder,
		resyncInterval: resyncInterval,
		seen:           map[string]uuid.UUID{},
	}, nil
}

func (o *StatusObserver) Start(ctx context.Context) error {
	log.Trace("localserving StatusObserver Start")

	for {
		resourceVersion, err := o.ProcessSnapshot(ctx)
		if err != nil {
			log.WithContext(ctx).WithError(err).Error("local served model status snapshot failed")
		}
		changes, err := o.adapter.store.Watch(ctx, o.adapter.namespace, resourceVersion)
		if err != nil {
			log.WithContext(ctx).WithError(err).Error("local served model status watch failed")
			return err
		}
		resync := time.NewTicker(o.resyncInterval)
	watchLoop:
		for {
			select {
			case <-ctx.Done():
				resync.Stop()
				return ctx.Err()
			case <-resync.C:
				if _, err := o.ProcessSnapshot(ctx); err != nil {
					log.WithContext(ctx).WithError(err).Error("local served model status snapshot failed")
				}
			case _, ok := <-changes:
				if !ok {
					resync.Stop()
					if err := ctx.Err(); err != nil {
						return err
					}
					break watchLoop
				}
				if _, err := o.ProcessSnapshot(ctx); err != nil {
					log.WithContext(ctx).WithError(err).Error("local served model status snapshot failed")
				}
			}
		}
	}
}

func (o *StatusObserver) ProcessOnce(ctx context.Context) error {
	log.Trace("localserving StatusObserver ProcessOnce")

	_, err := o.ProcessSnapshot(ctx)
	return err
}

func (o *StatusObserver) ProcessSnapshot(ctx context.Context) (string, error) {
	log.Trace("localserving StatusObserver ProcessSnapshot")

	records, resourceVersion, err := o.adapter.store.List(o.adapter.namespace)
	if err != nil {
		return "", err
	}
	for _, record := range records {
		o.processRecord(ctx, record)
	}
	return resourceVersion, nil
}

func (o *StatusObserver) processRecord(ctx context.Context, record localstore.Record) {
	log.Trace("localserving StatusObserver processRecord")

	if record.Status.ServingLoadStatus == "" {
		return
	}
	modelID, err := uuid.Parse(record.Spec.ModelID)
	if err != nil {
		log.WithContext(ctx).WithError(err).WithField("served_model", record.Name).Error("local served model status ignored")
		return
	}
	loadStatus, err := model.ToModelLoadStatus(record.Status.ServingLoadStatus)
	if err != nil {
		log.WithContext(ctx).WithError(err).WithField("served_model", record.Name).Error("local served model load status ignored")
		return
	}
	servingProtocol, err := model.ToServingProtocol(record.Status.ServingProtocol)
	if err != nil {
		log.WithContext(ctx).WithError(err).WithField("served_model", record.Name).Error("local served model protocol ignored")
		return
	}
	status := &model.ServedModelStatus{
		ModelID:           modelID,
		ServingTarget:     record.Status.ServingTarget,
		ServingModel:      record.Status.ServingModel,
		ServingProtocol:   servingProtocol,
		ServingLoadStatus: loadStatus,
		FailureReason:     record.Status.FailureReason,
	}
	key := servedModelStatusID(status, record.Generation)
	if o.seen[record.Name] == key {
		return
	}
	if _, err := o.recorder.RecordModelServingStatus(ctx, status, key); err != nil {
		if errors.Is(err, domain.ErrModelNotFound) {
			if deleteErr := o.adapter.store.Delete(record.Name); deleteErr != nil && !errors.Is(deleteErr, localstore.ErrNotFound) {
				log.WithContext(ctx).WithError(deleteErr).WithField("served_model", record.Name).Error("stale local served model cleanup failed")
				return
			}
			delete(o.seen, record.Name)
			log.WithContext(ctx).WithField("served_model", record.Name).Warn("removed stale local served model status for missing model")
			return
		}
		log.WithContext(ctx).WithError(err).WithField("served_model", record.Name).Error("local served model status recording failed")
		return
	}
	o.seen[record.Name] = key
}

func servedModelStatusID(status *model.ServedModelStatus, generation int64) uuid.UUID {
	log.Trace("localserving servedModelStatusID")

	input := fmt.Sprintf("served-model:%s:%s:%s:%s:%s:%s:%d", status.ModelID, status.ServingLoadStatus.String(), status.ServingTarget, status.ServingModel, status.ServingProtocol, status.FailureReason, generation)
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(input))
}
