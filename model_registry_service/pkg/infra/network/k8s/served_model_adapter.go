package k8s

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"model_registry_service/pkg/domain"
	"model_registry_service/pkg/domain/model"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
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

type ServedModelAdapter struct {
	namespace string
	gvr       schema.GroupVersionResource
	kind      string
	client    dynamic.Interface
}

type ServingStatusRecorder interface {
	RecordModelServingStatus(ctx context.Context, servedModelStatus *model.ServedModelStatus, idempotencyKey uuid.UUID) (*model.Model, error)
}

type ServedModelStatusObserver struct {
	adapter      *ServedModelAdapter
	recorder     ServingStatusRecorder
	pollInterval time.Duration
	seen         map[string]uuid.UUID
}

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

func NewServedModelStatusObserver(adapter *ServedModelAdapter, recorder ServingStatusRecorder, pollInterval time.Duration) (*ServedModelStatusObserver, error) {
	log.Trace("NewServedModelStatusObserver")

	if adapter == nil {
		return nil, fmt.Errorf("%w: served model adapter is required", domain.ErrValidationFailed)
	}
	if recorder == nil {
		return nil, fmt.Errorf("%w: serving status recorder is required", domain.ErrValidationFailed)
	}
	if pollInterval <= 0 {
		return nil, fmt.Errorf("%w: served model status poll interval is required", domain.ErrValidationFailed)
	}
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

	if err := o.ProcessOnce(ctx); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		log.WithContext(ctx).WithError(err).Error("served model status poll failed")
	}
	ticker := time.NewTicker(o.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := o.ProcessOnce(ctx); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return err
				}
				log.WithContext(ctx).WithError(err).Error("served model status poll failed")
			}
		}
	}
}

func (o *ServedModelStatusObserver) ProcessOnce(ctx context.Context) error {
	log.Trace("ServedModelStatusObserver ProcessOnce")

	items, err := o.adapter.client.Resource(o.adapter.gvr).Namespace(o.adapter.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("%w: list served models: %w", domain.ErrModelServe, err)
	}
	for i := range items.Items {
		status, ok, err := servedModelStatusFromObject(&items.Items[i])
		if err != nil {
			log.WithContext(ctx).WithError(err).WithField("served_model", items.Items[i].GetName()).Error("served model status ignored")
			continue
		}
		if !ok {
			continue
		}
		key := servedModelStatusID(status, items.Items[i].GetGeneration())
		if o.seen[items.Items[i].GetName()] == key {
			continue
		}
		if _, err := o.recorder.RecordModelServingStatus(ctx, status, key); err != nil {
			log.WithContext(ctx).WithError(err).WithFields(log.Fields{
				"served_model": items.Items[i].GetName(),
				"model_id":     status.ModelID,
			}).Error("served model status recording failed")
			continue
		}
		o.seen[items.Items[i].GetName()] = key
	}
	return nil
}

func (a *ServedModelAdapter) servedModelObject(name string, registeredModel *model.Model) *unstructured.Unstructured {
	log.Trace("ServedModelAdapter servedModelObject")

	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": a.gvr.Group + "/" + a.gvr.Version,
		"kind":       a.kind,
		"metadata": map[string]any{
			"name":      name,
			"namespace": a.namespace,
			"labels": map[string]any{
				"app.kubernetes.io/name":       "served-model",
				"app.kubernetes.io/managed-by": "model-registry-service",
				"bighill.io/model-id":          registeredModel.ModelID.String(),
			},
		},
		"spec": map[string]any{
			"modelID":          registeredModel.ModelID.String(),
			"trainingRunID":    registeredModel.TrainingRunID.String(),
			"datasetID":        registeredModel.DatasetID.String(),
			"name":             registeredModel.Name,
			"modelVersion":     int64(registeredModel.ModelVersion),
			"baseModel":        registeredModel.BaseModel,
			"artifactLocation": registeredModel.ArtifactLocation,
			"artifactFormat":   registeredModel.ArtifactFormat,
			"artifactChecksum": registeredModel.ArtifactChecksum,
			"adapterURI":       registeredModel.AdapterURI,
			"servingTarget":    registeredModel.ServingTarget,
			"servingModel":     registeredModel.ServingModel,
		},
	}}
}

func servedModelStatusFromObject(obj *unstructured.Unstructured) (*model.ServedModelStatus, bool, error) {
	log.Trace("servedModelStatusFromObject")

	statusRaw, _, _ := unstructured.NestedString(obj.Object, "status", "servingLoadStatus")
	if statusRaw == "" {
		return nil, false, nil
	}
	loadStatus, err := model.ToModelLoadStatus(statusRaw)
	if err != nil {
		return nil, false, fmt.Errorf("%w: served model status %s has invalid load status: %w", domain.ErrValidationFailed, obj.GetName(), err)
	}
	modelIDRaw, _, _ := unstructured.NestedString(obj.Object, "spec", "modelID")
	modelID, err := uuid.Parse(modelIDRaw)
	if err != nil {
		return nil, false, fmt.Errorf("%w: served model status %s has invalid model id: %w", domain.ErrValidationFailed, obj.GetName(), err)
	}
	servingTarget, _, _ := unstructured.NestedString(obj.Object, "status", "servingTarget")
	if servingTarget == "" {
		servingTarget, _, _ = unstructured.NestedString(obj.Object, "spec", "servingTarget")
	}
	servingModel, _, _ := unstructured.NestedString(obj.Object, "status", "servingModel")
	if servingModel == "" {
		servingModel, _, _ = unstructured.NestedString(obj.Object, "spec", "servingModel")
	}
	failureReason, _, _ := unstructured.NestedString(obj.Object, "status", "failureReason")
	return &model.ServedModelStatus{
		ModelID:           modelID,
		ServingTarget:     servingTarget,
		ServingModel:      servingModel,
		ServingLoadStatus: loadStatus,
		FailureReason:     failureReason,
	}, true, nil
}

func servedModelStatusID(status *model.ServedModelStatus, generation int64) uuid.UUID {
	log.Trace("servedModelStatusID")

	input := fmt.Sprintf("served-model:%s:%s:%s:%s:%s:%d", status.ModelID, status.ServingLoadStatus.String(), status.ServingTarget, status.ServingModel, status.FailureReason, generation)
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(input))
}

func ServedModelName(modelID uuid.UUID, modelVersion int) string {
	log.Trace("ServedModelName")

	return dns1123Name(fmt.Sprintf("served-model-%s-v%d", modelID.String(), modelVersion))
}

func dns1123Name(value string) string {
	log.Trace("dns1123Name")

	name := strings.ToLower(value)
	name = invalidKubernetesNameChars.ReplaceAllString(name, "-")
	name = strings.Trim(name, "-")
	if name == "" {
		name = "served-model"
	}
	if len(name) <= maxKubernetesNameLength {
		return name
	}
	sum := sha1.Sum([]byte(name))
	suffix := hex.EncodeToString(sum[:])[:10]
	prefix := strings.Trim(name[:maxKubernetesNameLength-len(suffix)-1], "-")
	if prefix == "" {
		prefix = "served-model"
	}
	return prefix + "-" + suffix
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

var invalidKubernetesNameChars = regexp.MustCompile(`[^a-z0-9-]+`)

const maxKubernetesNameLength = 63
