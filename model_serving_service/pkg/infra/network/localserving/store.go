package localserving

import (
	"context"
	"fmt"

	"model_serving_service/pkg/domain"
	"model_serving_service/pkg/domain/model"

	localstore "lib/shared_lib/servedmodel"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
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

func (s *Store) Watch(ctx context.Context, _ string) (watch.Interface, error) {
	log.Trace("localserving Store Watch")

	return watch.NewEmptyWatch(), nil
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
	}, nil
}

func parseOptionalUUID(value string) (uuid.UUID, error) {
	log.Trace("parseOptionalUUID")

	if value == "" {
		return uuid.Nil, nil
	}
	return uuid.Parse(value)
}
