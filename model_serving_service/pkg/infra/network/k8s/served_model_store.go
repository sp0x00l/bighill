package kubernetes

import (
	"context"
	"fmt"
	"strings"

	"model_serving_service/pkg/domain"
	"model_serving_service/pkg/domain/model"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
)

type ServedModelStoreConfig struct {
	Namespace string
	Group     string
	Version   string
	Resource  string
}

const (
	servedModelObjectSpec       = "spec"
	servedModelObjectStatus     = "status"
	servedModelSpecModelID      = "modelID"
	servedModelSpecOrgID        = "orgID"
	servedModelSpecTrainingID   = "trainingRunID"
	servedModelSpecDatasetID    = "datasetID"
	servedModelSpecKind         = "modelKind"
	servedModelSpecName         = "name"
	servedModelSpecVersion      = "modelVersion"
	servedModelSpecBaseModel    = "baseModel"
	servedModelSpecArtifactLoc  = "artifactLocation"
	servedModelSpecArtifactFmt  = "artifactFormat"
	servedModelSpecChecksum     = "artifactChecksum"
	servedModelSpecAdapterURI   = "adapterURI"
	servedModelSpecAdapterRank  = "adapterRank"
	servedModelSpecIsolation    = "runtimeIsolation"
	servedModelSpecPinned       = "pinned"
	servedModelSpecTarget       = "servingTarget"
	servedModelSpecModel        = "servingModel"
	servedModelSpecProtocol     = "servingProtocol"
	servedModelStatusLoad       = "servingLoadStatus"
	servedModelStatusTarget     = "servingTarget"
	servedModelStatusModel      = "servingModel"
	servedModelStatusProtocol   = "servingProtocol"
	servedModelStatusFailure    = "failureReason"
	servedModelStatusGeneration = "observedGeneration"
	servedModelStatusReplicas   = "readyReplicas"
)

type ServedModelStore struct {
	namespace string
	gvr       schema.GroupVersionResource
	client    dynamic.Interface
}

type servedModelDTOAdapter struct {
	namespace string
}

func NewServedModelStore(config ServedModelStoreConfig, client dynamic.Interface) (*ServedModelStore, error) {
	log.Trace("NewServedModelStore")

	if strings.TrimSpace(config.Namespace) == "" {
		return nil, domain.ErrValidationFailed.Extend("served model namespace is required")
	}
	if strings.TrimSpace(config.Group) == "" || strings.TrimSpace(config.Version) == "" || strings.TrimSpace(config.Resource) == "" {
		return nil, domain.ErrValidationFailed.Extend("served model gvr is required")
	}
	if client == nil {
		return nil, domain.ErrValidationFailed.Extend("kubernetes client is required")
	}
	return &ServedModelStore{
		namespace: strings.TrimSpace(config.Namespace),
		gvr: schema.GroupVersionResource{
			Group:    strings.TrimSpace(config.Group),
			Version:  strings.TrimSpace(config.Version),
			Resource: strings.TrimSpace(config.Resource),
		},
		client: client,
	}, nil
}

func (s *ServedModelStore) Namespace() string {
	log.Trace("ServedModelStore Namespace")

	return s.namespace
}

func (s *ServedModelStore) Read(ctx context.Context, resourceName string) (*model.ServedModel, error) {
	log.Trace("ServedModelStore Read")

	obj, err := s.client.Resource(s.gvr).Namespace(s.namespace).Get(ctx, resourceName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("%w: served model %s: %w", domain.ErrServedModelNotFound, resourceName, err)
		}
		return nil, fmt.Errorf("%w: read served model: %w", domain.ErrModelServe, err)
	}
	return servedModelDTOAdapter{namespace: s.namespace}.FromObject(obj)
}

func (s *ServedModelStore) List(ctx context.Context) ([]*model.ServedModel, error) {
	log.Trace("ServedModelStore List")

	servedModels, _, err := s.ListWithResourceVersion(ctx)
	return servedModels, err
}

func (s *ServedModelStore) ListWithResourceVersion(ctx context.Context) ([]*model.ServedModel, string, error) {
	log.Trace("ServedModelStore ListWithResourceVersion")

	items, err := s.client.Resource(s.gvr).Namespace(s.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, "", fmt.Errorf("%w: list served models: %w", domain.ErrModelServe, err)
	}
	out := make([]*model.ServedModel, 0, len(items.Items))
	adapter := servedModelDTOAdapter{namespace: s.namespace}
	for i := range items.Items {
		servedModel, err := adapter.FromObject(&items.Items[i])
		if err != nil {
			log.WithContext(ctx).WithError(err).WithField("served_model", items.Items[i].GetName()).Error("served model spec ignored")
			continue
		}
		out = append(out, servedModel)
	}
	return out, items.GetResourceVersion(), nil
}

func (s *ServedModelStore) Watch(ctx context.Context, resourceVersion string) (watch.Interface, error) {
	log.Trace("ServedModelStore Watch")

	watcher, err := s.client.Resource(s.gvr).Namespace(s.namespace).Watch(ctx, metav1.ListOptions{
		ResourceVersion: resourceVersion,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: watch served models: %w", domain.ErrModelServe, err)
	}
	return watcher, nil
}

func (s *ServedModelStore) UpdateStatus(ctx context.Context, resourceName string, status *model.ServedModelStatus) error {
	log.Trace("ServedModelStore UpdateStatus")

	resource := s.client.Resource(s.gvr).Namespace(s.namespace)
	obj, err := resource.Get(ctx, resourceName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("%w: read served model for status update: %w", domain.ErrModelServe, err)
	}
	if servedModelStatusMatches(obj, status) {
		return nil
	}
	setStatusFields(obj, status)
	if _, err := resource.UpdateStatus(ctx, obj, metav1.UpdateOptions{}); err != nil {
		if apierrors.IsMethodNotSupported(err) {
			if _, updateErr := resource.Update(ctx, obj, metav1.UpdateOptions{}); updateErr != nil {
				return fmt.Errorf("%w: update served model status fallback: %w", domain.ErrModelServe, updateErr)
			}
			return nil
		}
		return fmt.Errorf("%w: update served model status: %w", domain.ErrModelServe, err)
	}
	return nil
}

func servedModelStatusMatches(obj *unstructured.Unstructured, status *model.ServedModelStatus) bool {
	log.Trace("servedModelStatusMatches")

	loadStatus, _, _ := unstructured.NestedString(obj.Object, servedModelObjectStatus, servedModelStatusLoad)
	servingTarget, _, _ := unstructured.NestedString(obj.Object, servedModelObjectStatus, servedModelStatusTarget)
	servingModel, _, _ := unstructured.NestedString(obj.Object, servedModelObjectStatus, servedModelStatusModel)
	servingProtocol, _, _ := unstructured.NestedString(obj.Object, servedModelObjectStatus, servedModelStatusProtocol)
	failureReason, _, _ := unstructured.NestedString(obj.Object, servedModelObjectStatus, servedModelStatusFailure)
	observedGeneration, _, _ := unstructured.NestedInt64(obj.Object, servedModelObjectStatus, servedModelStatusGeneration)
	readyReplicas, _, _ := unstructured.NestedInt64(obj.Object, servedModelObjectStatus, servedModelStatusReplicas)
	return loadStatus == status.ServingLoadStatus.String() &&
		servingTarget == status.ServingTarget &&
		servingModel == status.ServingModel &&
		servingProtocol == status.ServingProtocol.String() &&
		failureReason == status.FailureReason &&
		observedGeneration == status.ObservedGeneration &&
		readyReplicas == int64(status.ReadyReplicas)
}

func (a servedModelDTOAdapter) FromObject(obj *unstructured.Unstructured) (*model.ServedModel, error) {
	log.Trace("servedModelDTOAdapter FromObject")

	modelID, err := uuid.Parse(requiredSpecString(obj, servedModelSpecModelID))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid model id: %w", domain.ErrValidationFailed, err)
	}
	orgID, err := parseOptionalUUID(specString(obj, servedModelSpecOrgID))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid org id: %w", domain.ErrValidationFailed, err)
	}
	trainingRunID, err := parseOptionalUUID(specString(obj, servedModelSpecTrainingID))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid training run id: %w", domain.ErrValidationFailed, err)
	}
	datasetID, err := parseOptionalUUID(specString(obj, servedModelSpecDatasetID))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid dataset id: %w", domain.ErrValidationFailed, err)
	}
	modelVersion, _, _ := unstructured.NestedInt64(obj.Object, servedModelObjectSpec, servedModelSpecVersion)
	adapterRank, _, _ := unstructured.NestedInt64(obj.Object, servedModelObjectSpec, servedModelSpecAdapterRank)
	pinned, _, _ := unstructured.NestedBool(obj.Object, servedModelObjectSpec, servedModelSpecPinned)
	status, err := a.StatusFromObject(obj)
	if err != nil {
		return nil, err
	}
	servingProtocol, err := model.ToServingProtocol(specString(obj, servedModelSpecProtocol))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid served model protocol: %w", domain.ErrValidationFailed, err)
	}
	return &model.ServedModel{
		ResourceName:     obj.GetName(),
		Namespace:        a.namespace,
		Generation:       obj.GetGeneration(),
		ModelID:          modelID,
		OrgID:            orgID,
		TrainingRunID:    trainingRunID,
		DatasetID:        datasetID,
		ModelKind:        specString(obj, servedModelSpecKind),
		Name:             specString(obj, servedModelSpecName),
		ModelVersion:     int(modelVersion),
		BaseModel:        specString(obj, servedModelSpecBaseModel),
		ArtifactLocation: specString(obj, servedModelSpecArtifactLoc),
		ArtifactFormat:   specString(obj, servedModelSpecArtifactFmt),
		ArtifactChecksum: specString(obj, servedModelSpecChecksum),
		AdapterURI:       specString(obj, servedModelSpecAdapterURI),
		AdapterRank:      int(adapterRank),
		RuntimeIsolation: specString(obj, servedModelSpecIsolation),
		Pinned:           pinned,
		ServingTarget:    specString(obj, servedModelSpecTarget),
		ServingModel:     specString(obj, servedModelSpecModel),
		ServingProtocol:  servingProtocol,
		Status:           status,
	}, nil
}

func (a servedModelDTOAdapter) StatusFromObject(obj *unstructured.Unstructured) (*model.ServedModelStatus, error) {
	log.Trace("servedModelDTOAdapter StatusFromObject")

	loadStatusValue, exists, _ := unstructured.NestedString(obj.Object, servedModelObjectStatus, servedModelStatusLoad)
	if !exists || strings.TrimSpace(loadStatusValue) == "" {
		return nil, nil
	}
	loadStatus, err := model.ToModelLoadStatus(loadStatusValue)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid served model load status: %w", domain.ErrValidationFailed, err)
	}
	servingTarget, _, _ := unstructured.NestedString(obj.Object, servedModelObjectStatus, servedModelStatusTarget)
	servingModel, _, _ := unstructured.NestedString(obj.Object, servedModelObjectStatus, servedModelStatusModel)
	servingProtocol, _, _ := unstructured.NestedString(obj.Object, servedModelObjectStatus, servedModelStatusProtocol)
	parsedServingProtocol, err := model.ToServingProtocol(servingProtocol)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid served model protocol: %w", domain.ErrValidationFailed, err)
	}
	failureReason, _, _ := unstructured.NestedString(obj.Object, servedModelObjectStatus, servedModelStatusFailure)
	observedGeneration, _, _ := unstructured.NestedInt64(obj.Object, servedModelObjectStatus, servedModelStatusGeneration)
	readyReplicas, _, _ := unstructured.NestedInt64(obj.Object, servedModelObjectStatus, servedModelStatusReplicas)
	return &model.ServedModelStatus{
		ServingLoadStatus:  loadStatus,
		ServingTarget:      servingTarget,
		ServingModel:       servingModel,
		ServingProtocol:    parsedServingProtocol,
		FailureReason:      failureReason,
		ObservedGeneration: observedGeneration,
		ReadyReplicas:      int32(readyReplicas),
	}, nil
}

func setStatusFields(obj *unstructured.Unstructured, status *model.ServedModelStatus) {
	log.Trace("setStatusFields")

	_ = unstructured.SetNestedField(obj.Object, status.ServingLoadStatus.String(), servedModelObjectStatus, servedModelStatusLoad)
	_ = unstructured.SetNestedField(obj.Object, status.ServingTarget, servedModelObjectStatus, servedModelStatusTarget)
	_ = unstructured.SetNestedField(obj.Object, status.ServingModel, servedModelObjectStatus, servedModelStatusModel)
	_ = unstructured.SetNestedField(obj.Object, status.ServingProtocol.String(), servedModelObjectStatus, servedModelStatusProtocol)
	_ = unstructured.SetNestedField(obj.Object, status.FailureReason, servedModelObjectStatus, servedModelStatusFailure)
	_ = unstructured.SetNestedField(obj.Object, status.ObservedGeneration, servedModelObjectStatus, servedModelStatusGeneration)
	_ = unstructured.SetNestedField(obj.Object, int64(status.ReadyReplicas), servedModelObjectStatus, servedModelStatusReplicas)
}

func requiredSpecString(obj *unstructured.Unstructured, key string) string {
	log.Trace("requiredSpecString")

	return strings.TrimSpace(specString(obj, key))
}

func specString(obj *unstructured.Unstructured, key string) string {
	log.Trace("specString")

	value, _, _ := unstructured.NestedString(obj.Object, servedModelObjectSpec, key)
	return strings.TrimSpace(value)
}

func parseOptionalUUID(value string) (uuid.UUID, error) {
	log.Trace("parseOptionalUUID")

	value = strings.TrimSpace(value)
	if value == "" {
		return uuid.Nil, nil
	}
	return uuid.Parse(value)
}
