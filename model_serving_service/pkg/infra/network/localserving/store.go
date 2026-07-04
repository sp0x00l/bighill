package localserving

import (
	"context"
	"fmt"

	"model_serving_service/pkg/domain"
	"model_serving_service/pkg/domain/model"

	localstore "lib/shared_lib/servedmodel"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"
)

type Store struct {
	namespace string
	store     *localstore.Store
}

func NewStore(namespace string, path string) (*Store, error) {
	log.Trace("localserving NewStore")

	store, err := localstore.NewStore(path)
	if err != nil {
		return nil, err
	}
	return &Store{namespace: namespace, store: store}, nil
}

func (s *Store) Namespace() string {
	log.Trace("localserving Store Namespace")

	return s.namespace
}

func (s *Store) Read(ctx context.Context, resourceName string) (*model.ServedModel, error) {
	log.Trace("localserving Store Read")

	record, ok, err := s.store.Read(resourceName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("%w: served model %s not found", domain.ErrModelServe, resourceName)
	}
	return recordToServedModel(record)
}

func (s *Store) ListWithResourceVersion(ctx context.Context) ([]*model.ServedModel, string, error) {
	log.Trace("localserving Store ListWithResourceVersion")

	records, resourceVersion, err := s.store.List(s.namespace)
	if err != nil {
		return nil, "", err
	}
	out := make([]*model.ServedModel, 0, len(records))
	for _, record := range records {
		servedModel, err := recordToServedModel(record)
		if err != nil {
			log.WithContext(ctx).WithError(err).WithField("served_model", record.Name).Error("local served model ignored")
			continue
		}
		out = append(out, servedModel)
	}
	return out, resourceVersion, nil
}

func (s *Store) Watch(ctx context.Context, resourceVersion string) (watch.Interface, error) {
	log.Trace("localserving Store Watch")

	records, _, err := s.store.List(s.namespace)
	if err != nil {
		return nil, err
	}
	known := recordsByName(records)
	changes, err := s.store.Watch(ctx, s.namespace, resourceVersion)
	if err != nil {
		return nil, err
	}
	events := make(chan watch.Event)
	go func() {
		defer close(events)
		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-changes:
				if !ok {
					return
				}
				nextRecords, _, err := s.store.List(s.namespace)
				if err != nil {
					log.WithContext(ctx).WithError(err).Error("local served model watch list failed")
					continue
				}
				next := recordsByName(nextRecords)
				for name, record := range next {
					eventType := watch.Modified
					if _, exists := known[name]; !exists {
						eventType = watch.Added
					}
					if !sendWatchEvent(ctx, events, watch.Event{Type: eventType, Object: recordToObject(record)}) {
						return
					}
				}
				for name := range known {
					if _, exists := next[name]; exists {
						continue
					}
					if !sendWatchEvent(ctx, events, watch.Event{Type: watch.Deleted, Object: deletedObject(name)}) {
						return
					}
				}
				known = next
			}
		}
	}()
	return watch.NewProxyWatcher(events), nil
}

func (s *Store) UpdateStatus(ctx context.Context, resourceName string, status *model.ServedModelStatus) error {
	log.Trace("localserving Store UpdateStatus")

	return s.store.UpdateStatus(resourceName, localstore.Status{
		ServingLoadStatus:  status.ServingLoadStatus.String(),
		ServingTarget:      status.ServingTarget,
		ServingModel:       status.ServingModel,
		FailureReason:      status.FailureReason,
		ObservedGeneration: status.ObservedGeneration,
		ReadyReplicas:      status.ReadyReplicas,
	})
}

func recordToServedModel(record localstore.Record) (*model.ServedModel, error) {
	log.Trace("recordToServedModel")

	modelID, err := uuid.Parse(record.Spec.ModelID)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid local served model id: %w", domain.ErrValidationFailed, err)
	}
	trainingRunID, err := parseOptionalUUID(record.Spec.TrainingRunID)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid local training run id: %w", domain.ErrValidationFailed, err)
	}
	datasetID, err := parseOptionalUUID(record.Spec.DatasetID)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid local dataset id: %w", domain.ErrValidationFailed, err)
	}
	status, err := recordStatusToServedModelStatus(record.Status)
	if err != nil {
		return nil, err
	}
	return &model.ServedModel{
		ResourceName:     record.Name,
		Namespace:        record.Namespace,
		Generation:       record.Generation,
		ModelID:          modelID,
		TrainingRunID:    trainingRunID,
		DatasetID:        datasetID,
		Name:             record.Spec.Name,
		ModelVersion:     record.Spec.ModelVersion,
		BaseModel:        record.Spec.BaseModel,
		ArtifactLocation: record.Spec.ArtifactLocation,
		ArtifactFormat:   record.Spec.ArtifactFormat,
		ArtifactChecksum: record.Spec.ArtifactChecksum,
		AdapterURI:       record.Spec.AdapterURI,
		ServingTarget:    record.Spec.ServingTarget,
		ServingModel:     record.Spec.ServingModel,
		Status:           status,
	}, nil
}

func recordStatusToServedModelStatus(status localstore.Status) (*model.ServedModelStatus, error) {
	log.Trace("recordStatusToServedModelStatus")

	if status.ServingLoadStatus == "" {
		return nil, nil
	}
	loadStatus, err := model.ToModelLoadStatus(status.ServingLoadStatus)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid local served model load status: %w", domain.ErrValidationFailed, err)
	}
	return &model.ServedModelStatus{
		ServingLoadStatus:  loadStatus,
		ServingTarget:      status.ServingTarget,
		ServingModel:       status.ServingModel,
		FailureReason:      status.FailureReason,
		ObservedGeneration: status.ObservedGeneration,
		ReadyReplicas:      status.ReadyReplicas,
	}, nil
}

func parseOptionalUUID(value string) (uuid.UUID, error) {
	log.Trace("parseOptionalUUID")

	if value == "" {
		return uuid.Nil, nil
	}
	return uuid.Parse(value)
}

func recordsByName(records []localstore.Record) map[string]localstore.Record {
	log.Trace("recordsByName")

	out := make(map[string]localstore.Record, len(records))
	for _, record := range records {
		out[record.Name] = record
	}
	return out
}

func sendWatchEvent(ctx context.Context, events chan<- watch.Event, event watch.Event) bool {
	log.Trace("sendWatchEvent")

	select {
	case <-ctx.Done():
		return false
	case events <- event:
		return true
	}
}

func deletedObject(name string) *unstructured.Unstructured {
	log.Trace("deletedObject")

	obj := &unstructured.Unstructured{}
	obj.SetName(name)
	return obj
}

func recordToObject(record localstore.Record) *unstructured.Unstructured {
	log.Trace("recordToObject")

	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "serving.bighill.io/v1alpha1",
		"kind":       "ServedModel",
		"spec": map[string]any{
			"modelID":          record.Spec.ModelID,
			"trainingRunID":    record.Spec.TrainingRunID,
			"datasetID":        record.Spec.DatasetID,
			"name":             record.Spec.Name,
			"modelVersion":     int64(record.Spec.ModelVersion),
			"baseModel":        record.Spec.BaseModel,
			"artifactLocation": record.Spec.ArtifactLocation,
			"artifactFormat":   record.Spec.ArtifactFormat,
			"artifactChecksum": record.Spec.ArtifactChecksum,
			"adapterURI":       record.Spec.AdapterURI,
			"servingTarget":    record.Spec.ServingTarget,
			"servingModel":     record.Spec.ServingModel,
		},
	}}
	obj.SetName(record.Name)
	obj.SetNamespace(record.Namespace)
	obj.SetGeneration(record.Generation)
	if record.Status.ServingLoadStatus != "" {
		obj.Object["status"] = map[string]any{
			"servingLoadStatus":  record.Status.ServingLoadStatus,
			"servingTarget":      record.Status.ServingTarget,
			"servingModel":       record.Status.ServingModel,
			"failureReason":      record.Status.FailureReason,
			"observedGeneration": record.Status.ObservedGeneration,
			"readyReplicas":      int64(record.Status.ReadyReplicas),
		}
	}
	return obj
}
