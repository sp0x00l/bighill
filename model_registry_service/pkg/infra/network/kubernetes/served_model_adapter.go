package kubernetes

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	"model_registry_service/pkg/app"
	"model_registry_service/pkg/domain"
	"model_registry_service/pkg/domain/model"

	servedmodelstore "lib/shared_lib/servedmodel"
	"lib/shared_lib/uuidutil"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type ServedModelConfig struct {
	Namespace    string
	Group        string
	Version      string
	Resource     string
	Kind         string
	PollInterval time.Duration
}

const (
	servedModelObjectAPIVersion = "apiVersion"
	servedModelObjectKind       = "kind"
	servedModelObjectMetadata   = "metadata"
	servedModelObjectLabels     = "labels"
	servedModelObjectName       = "name"
	servedModelObjectNamespace  = "namespace"
	servedModelObjectSpec       = "spec"
	servedModelObjectStatus     = "status"

	servedModelLabelName      = "app.kubernetes.io/name"
	servedModelLabelManagedBy = "app.kubernetes.io/managed-by"
	servedModelLabelModelID   = "bighill.io/model-id"

	servedModelSpecModelID     = "modelID"
	servedModelSpecTrainingID  = "trainingRunID"
	servedModelSpecDatasetID   = "datasetID"
	servedModelSpecKind        = "modelKind"
	servedModelSpecName        = "name"
	servedModelSpecVersion     = "modelVersion"
	servedModelSpecBaseModel   = "baseModel"
	servedModelSpecArtifactLoc = "artifactLocation"
	servedModelSpecArtifactFmt = "artifactFormat"
	servedModelSpecChecksum    = "artifactChecksum"
	servedModelSpecAdapterURI  = "adapterURI"
	servedModelSpecTarget      = "servingTarget"
	servedModelSpecModel       = "servingModel"
	servedModelSpecProtocol    = "servingProtocol"
	servedModelStatusLoad      = "servingLoadStatus"
	servedModelStatusTarget    = "servingTarget"
	servedModelStatusModel     = "servingModel"
	servedModelStatusProtocol  = "servingProtocol"
	servedModelStatusFailure   = "failureReason"
)

type ServedModelAdapter struct {
	namespace string
	gvr       schema.GroupVersionResource
	kind      string
	client    dynamic.Interface
}

type ServedModelStatusObserver struct {
	adapter      *ServedModelAdapter
	recorder     app.ServingStatusRecorder
	pollInterval time.Duration
	seen         map[string]uuid.UUID
}

type servedModelStatusDTOAdapter struct{}

func NewServedModelAdapter(config ServedModelConfig) (*ServedModelAdapter, error) {
	log.Trace("NewServedModelAdapter")

	client, err := NewDynamicClient()
	if err != nil {
		return nil, err
	}
	return NewServedModelAdapterWithClient(config, client)
}

func NewServedModelAdapterWithClient(config ServedModelConfig, client dynamic.Interface) (*ServedModelAdapter, error) {
	log.Trace("NewServedModelAdapterWithClient")

	if strings.TrimSpace(config.Namespace) == "" {
		return nil, fmt.Errorf("%w: served model namespace is required", domain.ErrValidationFailed)
	}
	if strings.TrimSpace(config.Group) == "" || strings.TrimSpace(config.Version) == "" || strings.TrimSpace(config.Resource) == "" || strings.TrimSpace(config.Kind) == "" {
		return nil, fmt.Errorf("%w: served model gvr and kind are required", domain.ErrValidationFailed)
	}
	if client == nil {
		return nil, fmt.Errorf("%w: served model k8s client is required", domain.ErrValidationFailed)
	}
	return &ServedModelAdapter{
		namespace: strings.TrimSpace(config.Namespace),
		gvr: schema.GroupVersionResource{
			Group:    strings.TrimSpace(config.Group),
			Version:  strings.TrimSpace(config.Version),
			Resource: strings.TrimSpace(config.Resource),
		},
		kind:   strings.TrimSpace(config.Kind),
		client: client,
	}, nil
}

func NewServedModelStatusObserver(adapter *ServedModelAdapter, recorder app.ServingStatusRecorder, pollInterval time.Duration) (*ServedModelStatusObserver, error) {
	log.Trace("NewServedModelStatusObserver")

	return &ServedModelStatusObserver{
		adapter:      adapter,
		recorder:     recorder,
		pollInterval: pollInterval,
		seen:         map[string]uuid.UUID{},
	}, nil
}

func (a *ServedModelAdapter) EnsureServedModel(ctx context.Context, registeredModel *model.Model) error {
	log.Trace("ServedModelAdapter EnsureServedModel")

	name := ServedModelName(registeredModel.ModelID, registeredModel.ModelVersion)
	obj := a.servedModelObject(name, registeredModel)
	_, err := a.client.Resource(a.gvr).Namespace(a.namespace).Create(ctx, obj, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		existing, getErr := a.client.Resource(a.gvr).Namespace(a.namespace).Get(ctx, name, metav1.GetOptions{})
		if getErr != nil {
			return fmt.Errorf("%w: read served model: %w", domain.ErrModelServe, getErr)
		}
		obj.SetResourceVersion(existing.GetResourceVersion())
		_, updateErr := a.client.Resource(a.gvr).Namespace(a.namespace).Update(ctx, obj, metav1.UpdateOptions{})
		if updateErr != nil {
			return fmt.Errorf("%w: update served model: %w", domain.ErrModelServe, updateErr)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("%w: create served model: %w", domain.ErrModelServe, err)
	}
	return nil
}

func (o *ServedModelStatusObserver) Start(ctx context.Context) error {
	log.Trace("ServedModelStatusObserver Start")

	for {
		resourceVersion, err := o.ProcessSnapshot(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			log.WithContext(ctx).WithError(err).Error("served model status snapshot failed")
			if err := sleepContext(ctx, reconnectDelay(o.pollInterval)); err != nil {
				return err
			}
			continue
		}
		if err := o.Watch(ctx, resourceVersion); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			log.WithContext(ctx).WithError(err).Error("served model status watch failed")
		}
		if err := sleepContext(ctx, reconnectDelay(o.pollInterval)); err != nil {
			return err
		}
	}
}

func (o *ServedModelStatusObserver) Watch(ctx context.Context, resourceVersion string) error {
	log.Trace("ServedModelStatusObserver Watch")

	watcher, err := o.adapter.client.Resource(o.adapter.gvr).Namespace(o.adapter.namespace).Watch(ctx, metav1.ListOptions{
		ResourceVersion: resourceVersion,
	})
	if err != nil {
		return fmt.Errorf("%w: watch served models: %w", domain.ErrModelServe, err)
	}
	defer watcher.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-watcher.ResultChan():
			if !ok {
				return nil
			}
			if err := o.ProcessWatchEvent(ctx, event); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return err
				}
				log.WithContext(ctx).WithError(err).Error("served model status watch event failed")
			}
		}
	}
}

func (o *ServedModelStatusObserver) ProcessOnce(ctx context.Context) error {
	log.Trace("ServedModelStatusObserver ProcessOnce")

	_, err := o.ProcessSnapshot(ctx)
	return err
}

func (o *ServedModelStatusObserver) ProcessSnapshot(ctx context.Context) (string, error) {
	log.Trace("ServedModelStatusObserver ProcessSnapshot")

	items, err := o.adapter.client.Resource(o.adapter.gvr).Namespace(o.adapter.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", fmt.Errorf("%w: list served models: %w", domain.ErrModelServe, err)
	}
	for i := range items.Items {
		o.processStatusObject(ctx, &items.Items[i])
	}
	return items.GetResourceVersion(), nil
}

func (o *ServedModelStatusObserver) ProcessWatchEvent(ctx context.Context, event watch.Event) error {
	log.Trace("ServedModelStatusObserver ProcessWatchEvent")

	switch event.Type {
	case watch.Added, watch.Modified:
		obj, ok := event.Object.(*unstructured.Unstructured)
		if !ok {
			return fmt.Errorf("%w: served model status watch event object is not unstructured", domain.ErrModelServe)
		}
		o.processStatusObject(ctx, obj)
	case watch.Deleted, watch.Bookmark:
		return nil
	case watch.Error:
		return fmt.Errorf("%w: served model status watch returned an error event", domain.ErrModelServe)
	}
	return nil
}

func (o *ServedModelStatusObserver) processStatusObject(ctx context.Context, obj *unstructured.Unstructured) {
	log.Trace("ServedModelStatusObserver processStatusObject")

	status, ok, err := servedModelStatusDTOAdapter{}.FromObject(obj)
	if err != nil {
		log.WithContext(ctx).WithError(err).WithField("served_model", obj.GetName()).Error("served model status ignored")
		return
	}
	if !ok {
		return
	}
	key := servedModelStatusID(status, obj.GetGeneration())
	if o.seen[obj.GetName()] == key {
		return
	}
	if _, err := o.recorder.RecordModelServingStatus(ctx, status, key); err != nil {
		log.WithContext(ctx).WithError(err).WithFields(log.Fields{
			"served_model": obj.GetName(),
			"model_id":     status.ModelID,
		}).Error("served model status recording failed")
		return
	}
	o.seen[obj.GetName()] = key
}

func (a *ServedModelAdapter) servedModelObject(name string, registeredModel *model.Model) *unstructured.Unstructured {
	log.Trace("ServedModelAdapter servedModelObject")

	return &unstructured.Unstructured{Object: map[string]any{
		servedModelObjectAPIVersion: a.gvr.Group + "/" + a.gvr.Version,
		servedModelObjectKind:       a.kind,
		servedModelObjectMetadata: map[string]any{
			servedModelObjectName:      name,
			servedModelObjectNamespace: a.namespace,
			servedModelObjectLabels: map[string]any{
				servedModelLabelName:      "served-model",
				servedModelLabelManagedBy: "model-registry-service",
				servedModelLabelModelID:   registeredModel.ModelID.String(),
			},
		},
		servedModelObjectSpec: map[string]any{
			servedModelSpecModelID:     registeredModel.ModelID.String(),
			servedModelSpecTrainingID:  uuidutil.StringOrEmpty(registeredModel.TrainingRunID),
			servedModelSpecDatasetID:   uuidutil.StringOrEmpty(registeredModel.DatasetID),
			servedModelSpecKind:        registeredModel.ModelKind.String(),
			servedModelSpecName:        registeredModel.Name,
			servedModelSpecVersion:     int64(registeredModel.ModelVersion),
			servedModelSpecBaseModel:   registeredModel.BaseModel,
			servedModelSpecArtifactLoc: registeredModel.ArtifactLocation,
			servedModelSpecArtifactFmt: registeredModel.ArtifactFormat,
			servedModelSpecChecksum:    registeredModel.ArtifactChecksum,
			servedModelSpecAdapterURI:  registeredModel.AdapterURI,
			servedModelSpecTarget:      registeredModel.ServingTarget,
			servedModelSpecModel:       registeredModel.ServingModel,
			servedModelSpecProtocol:    registeredModel.ServingProtocol.String(),
		},
	}}
}

func (servedModelStatusDTOAdapter) FromObject(obj *unstructured.Unstructured) (*model.ServedModelStatus, bool, error) {
	log.Trace("servedModelStatusDTOAdapter FromObject")

	statusRaw, _, _ := unstructured.NestedString(obj.Object, servedModelObjectStatus, servedModelStatusLoad)
	if statusRaw == "" {
		return nil, false, nil
	}
	loadStatus, err := model.ToModelLoadStatus(statusRaw)
	if err != nil {
		return nil, false, fmt.Errorf("%w: served model status %s has invalid load status: %w", domain.ErrValidationFailed, obj.GetName(), err)
	}
	modelIDRaw, _, _ := unstructured.NestedString(obj.Object, servedModelObjectSpec, servedModelSpecModelID)
	modelID, err := uuid.Parse(modelIDRaw)
	if err != nil {
		return nil, false, fmt.Errorf("%w: served model status %s has invalid model id: %w", domain.ErrValidationFailed, obj.GetName(), err)
	}
	servingTarget, _, _ := unstructured.NestedString(obj.Object, servedModelObjectStatus, servedModelStatusTarget)
	if servingTarget == "" {
		servingTarget, _, _ = unstructured.NestedString(obj.Object, servedModelObjectSpec, servedModelSpecTarget)
	}
	servingModel, _, _ := unstructured.NestedString(obj.Object, servedModelObjectStatus, servedModelStatusModel)
	if servingModel == "" {
		servingModel, _, _ = unstructured.NestedString(obj.Object, servedModelObjectSpec, servedModelSpecModel)
	}
	servingProtocol, _, _ := unstructured.NestedString(obj.Object, servedModelObjectStatus, servedModelStatusProtocol)
	if servingProtocol == "" {
		servingProtocol, _, _ = unstructured.NestedString(obj.Object, servedModelObjectSpec, servedModelSpecProtocol)
	}
	parsedServingProtocol, err := model.ToServingProtocol(servingProtocol)
	if err != nil {
		return nil, false, fmt.Errorf("%w: served model status %s has invalid serving protocol: %w", domain.ErrValidationFailed, obj.GetName(), err)
	}
	failureReason, _, _ := unstructured.NestedString(obj.Object, servedModelObjectStatus, servedModelStatusFailure)
	return &model.ServedModelStatus{
		ModelID:           modelID,
		ServingTarget:     servingTarget,
		ServingModel:      servingModel,
		ServingProtocol:   parsedServingProtocol,
		ServingLoadStatus: loadStatus,
		FailureReason:     failureReason,
	}, true, nil
}

func servedModelStatusID(status *model.ServedModelStatus, generation int64) uuid.UUID {
	log.Trace("servedModelStatusID")

	input := fmt.Sprintf("served-model:%s:%s:%s:%s:%s:%s:%d", status.ModelID, status.ServingLoadStatus.String(), status.ServingTarget, status.ServingModel, status.ServingProtocol, status.FailureReason, generation)
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(input))
}

func ServedModelName(modelID uuid.UUID, modelVersion int) string {
	log.Trace("ServedModelName")

	return servedmodelstore.ResourceName(modelID.String(), modelVersion)
}

func NewDynamicClient() (dynamic.Interface, error) {
	log.Trace("NewDynamicClient")

	cfg, err := rest.InClusterConfig()
	if err != nil {
		kubeconfig := os.Getenv("KUBECONFIG")
		if kubeconfig == "" {
			home, homeErr := os.UserHomeDir()
			if homeErr == nil {
				kubeconfig = filepath.Join(home, ".kube", "config")
			}
		}
		cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return nil, fmt.Errorf("%w: create served model client config: %w", domain.ErrModelServe, err)
		}
	}
	client, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("%w: create served model client: %w", domain.ErrModelServe, err)
	}
	return client, nil
}

func sleepContext(ctx context.Context, duration time.Duration) error {
	log.Trace("sleepContext")

	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func reconnectDelay(base time.Duration) time.Duration {
	log.Trace("reconnectDelay")

	if base <= 0 {
		base = time.Second
	}
	jitterMax := int64(base / 5)
	if jitterMax <= 0 {
		return base
	}
	return base + time.Duration(rand.Int63n(jitterMax))
}
