package localserving

import (
	"context"
	"fmt"
	"time"

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
	adapter      *Adapter
	recorder     ServingStatusRecorder
	pollInterval time.Duration
	seen         map[string]uuid.UUID
}

type ServingStatusRecorder interface {
	RecordModelServingStatus(ctx context.Context, servedModelStatus *model.ServedModelStatus, idempotencyKey uuid.UUID) (*model.Model, error)
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
		Name:             registeredModel.Name,
		ModelVersion:     registeredModel.ModelVersion,
		BaseModel:        registeredModel.BaseModel,
		ArtifactLocation: registeredModel.ArtifactLocation,
		ArtifactFormat:   registeredModel.ArtifactFormat,
		ArtifactChecksum: registeredModel.ArtifactChecksum,
		AdapterURI:       registeredModel.AdapterURI,
		ServingTarget:    registeredModel.ServingTarget,
		ServingModel:     registeredModel.ServingModel,
	})
}

func ServedModelName(modelID uuid.UUID, modelVersion int) string {
	log.Trace("localserving ServedModelName")

	return localstore.ResourceName(modelID.String(), modelVersion)
}

func NewStatusObserver(adapter *Adapter, recorder ServingStatusRecorder, pollInterval time.Duration) (*StatusObserver, error) {
	log.Trace("localserving NewStatusObserver")

	if adapter == nil {
		return nil, fmt.Errorf("local served model adapter is required")
	}
	if recorder == nil {
		return nil, fmt.Errorf("serving status recorder is required")
	}
	if pollInterval <= 0 {
		return nil, fmt.Errorf("served model status poll interval is required")
	}
	return &StatusObserver{
		adapter:      adapter,
		recorder:     recorder,
		pollInterval: pollInterval,
		seen:         map[string]uuid.UUID{},
	}, nil
}

func (o *StatusObserver) Start(ctx context.Context) error {
	log.Trace("localserving StatusObserver Start")

	ticker := time.NewTicker(o.pollInterval)
	defer ticker.Stop()
	for {
		if err := o.ProcessOnce(ctx); err != nil {
			log.WithContext(ctx).WithError(err).Error("local served model status poll failed")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (o *StatusObserver) ProcessOnce(ctx context.Context) error {
	log.Trace("localserving StatusObserver ProcessOnce")

	records, _, err := o.adapter.store.List(o.adapter.namespace)
	if err != nil {
		return err
	}
	for _, record := range records {
		o.processRecord(ctx, record)
	}
	return nil
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
	status := &model.ServedModelStatus{
		ModelID:           modelID,
		ServingTarget:     record.Status.ServingTarget,
		ServingModel:      record.Status.ServingModel,
		ServingLoadStatus: loadStatus,
		FailureReason:     record.Status.FailureReason,
	}
	key := servedModelStatusID(status, record.Generation)
	if o.seen[record.Name] == key {
		return
	}
	if _, err := o.recorder.RecordModelServingStatus(ctx, status, key); err != nil {
		log.WithContext(ctx).WithError(err).WithField("served_model", record.Name).Error("local served model status recording failed")
		return
	}
	o.seen[record.Name] = key
}

func servedModelStatusID(status *model.ServedModelStatus, generation int64) uuid.UUID {
	log.Trace("localserving servedModelStatusID")

	input := fmt.Sprintf("served-model:%s:%s:%s:%s:%s:%d", status.ModelID, status.ServingLoadStatus.String(), status.ServingTarget, status.ServingModel, status.FailureReason, generation)
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(input))
}
