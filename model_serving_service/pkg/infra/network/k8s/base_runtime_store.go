package kubernetes

import (
	"context"
	"fmt"
	"strings"
	"time"

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

type BaseRuntimeStoreConfig struct {
	Namespace string
	Group     string
	Version   string
	Resource  string
}

const (
	baseRuntimeObjectSpec         = "spec"
	baseRuntimeObjectStatus       = "status"
	baseRuntimeSpecBaseModel      = "baseModel"
	baseRuntimeSpecPoolKey        = "poolKey"
	baseRuntimeSpecMaxLoras       = "maxLoras"
	baseRuntimeSpecMaxLoraRank    = "maxLoraRank"
	baseRuntimeSpecGPU            = "gpu"
	baseRuntimeSpecImage          = "image"
	baseRuntimeStatusEndpoint     = "endpoint"
	baseRuntimeStatusPhase        = "phase"
	baseRuntimeStatusReadyReplica = "readyReplicas"
	baseRuntimeStatusLoaded       = "loadedAdapters"
	baseRuntimeAdapterModel       = "servingModel"
	baseRuntimeAdapterResource    = "servedModelResourceName"
	baseRuntimeAdapterModelID     = "modelID"
	baseRuntimeAdapterGeneration  = "observedGeneration"
	baseRuntimeAdapterLastUsed    = "lastUsedAt"
	baseRuntimeAdapterPinned      = "pinned"
)

type BaseRuntimeStore struct {
	namespace string
	gvr       schema.GroupVersionResource
	client    dynamic.Interface
}

type baseRuntimeDTOAdapter struct {
	namespace string
}

func NewBaseRuntimeStore(config BaseRuntimeStoreConfig, client dynamic.Interface) (*BaseRuntimeStore, error) {
	log.Trace("NewBaseRuntimeStore")

	if strings.TrimSpace(config.Namespace) == "" {
		return nil, domain.ErrValidationFailed.Extend("base runtime namespace is required")
	}
	if strings.TrimSpace(config.Group) == "" || strings.TrimSpace(config.Version) == "" || strings.TrimSpace(config.Resource) == "" {
		return nil, domain.ErrValidationFailed.Extend("base runtime gvr is required")
	}
	if client == nil {
		return nil, domain.ErrValidationFailed.Extend("kubernetes client is required")
	}
	return &BaseRuntimeStore{
		namespace: strings.TrimSpace(config.Namespace),
		gvr: schema.GroupVersionResource{
			Group:    strings.TrimSpace(config.Group),
			Version:  strings.TrimSpace(config.Version),
			Resource: strings.TrimSpace(config.Resource),
		},
		client: client,
	}, nil
}

func (s *BaseRuntimeStore) Namespace() string {
	log.Trace("BaseRuntimeStore Namespace")

	return s.namespace
}

func (s *BaseRuntimeStore) ListWithResourceVersion(ctx context.Context) ([]*model.BaseRuntime, string, error) {
	log.Trace("BaseRuntimeStore ListWithResourceVersion")

	items, err := s.client.Resource(s.gvr).Namespace(s.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, "", fmt.Errorf("%w: list base runtimes: %w", domain.ErrModelServe, err)
	}
	out := make([]*model.BaseRuntime, 0, len(items.Items))
	adapter := baseRuntimeDTOAdapter{namespace: s.namespace}
	for i := range items.Items {
		baseRuntime, err := adapter.FromObject(&items.Items[i])
		if err != nil {
			log.WithContext(ctx).WithError(err).WithField("base_runtime", items.Items[i].GetName()).Error("base runtime spec ignored")
			continue
		}
		out = append(out, baseRuntime)
	}
	return out, items.GetResourceVersion(), nil
}

func (s *BaseRuntimeStore) Watch(ctx context.Context, resourceVersion string) (watch.Interface, error) {
	log.Trace("BaseRuntimeStore Watch")

	watcher, err := s.client.Resource(s.gvr).Namespace(s.namespace).Watch(ctx, metav1.ListOptions{
		ResourceVersion: resourceVersion,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: watch base runtimes: %w", domain.ErrModelServe, err)
	}
	return watcher, nil
}

func (s *BaseRuntimeStore) FindOrCreate(ctx context.Context, baseRuntime *model.BaseRuntime) (*model.BaseRuntime, error) {
	log.Trace("BaseRuntimeStore FindOrCreate")

	resourceName := strings.TrimSpace(baseRuntime.ResourceName)
	if resourceName == "" {
		resourceName = BaseRuntimeResourceName(baseRuntime.BaseModel, baseRuntime.PoolKey)
	}
	resource := s.client.Resource(s.gvr).Namespace(s.namespace)
	obj, err := resource.Get(ctx, resourceName, metav1.GetOptions{})
	if err == nil {
		if !baseRuntimeMutableSpecMatches(obj, baseRuntime) {
			setBaseRuntimeMutableSpec(obj, baseRuntime)
			obj, err = resource.Update(ctx, obj, metav1.UpdateOptions{})
			if err != nil {
				return nil, fmt.Errorf("%w: update base runtime: %w", domain.ErrModelServe, err)
			}
		}
		return baseRuntimeDTOAdapter{namespace: s.namespace}.FromObject(obj)
	}
	if !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("%w: read base runtime: %w", domain.ErrModelServe, err)
	}
	obj = &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": s.gvr.Group + "/" + s.gvr.Version,
			"kind":       "BaseRuntime",
			"metadata": map[string]any{
				"name":      resourceName,
				"namespace": s.namespace,
			},
			baseRuntimeObjectSpec: map[string]any{},
		},
	}
	setBaseRuntimeSpec(obj, baseRuntime)
	created, err := resource.Create(ctx, obj, metav1.CreateOptions{})
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			created, err = resource.Get(ctx, resourceName, metav1.GetOptions{})
			if err != nil {
				return nil, fmt.Errorf("%w: read existing base runtime: %w", domain.ErrModelServe, err)
			}
			return baseRuntimeDTOAdapter{namespace: s.namespace}.FromObject(created)
		}
		return nil, fmt.Errorf("%w: create base runtime: %w", domain.ErrModelServe, err)
	}
	return baseRuntimeDTOAdapter{namespace: s.namespace}.FromObject(created)
}

func (s *BaseRuntimeStore) Read(ctx context.Context, resourceName string) (*model.BaseRuntime, error) {
	log.Trace("BaseRuntimeStore Read")

	obj, err := s.client.Resource(s.gvr).Namespace(s.namespace).Get(ctx, resourceName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("%w: base runtime %s: %w", domain.ErrServedModelNotFound, resourceName, err)
		}
		return nil, fmt.Errorf("%w: read base runtime: %w", domain.ErrModelServe, err)
	}
	return baseRuntimeDTOAdapter{namespace: s.namespace}.FromObject(obj)
}

func (s *BaseRuntimeStore) UpdateStatus(ctx context.Context, resourceName string, endpoint string, phase string, readyReplicas int32) error {
	log.Trace("BaseRuntimeStore UpdateStatus")

	resource := s.client.Resource(s.gvr).Namespace(s.namespace)
	obj, err := resource.Get(ctx, resourceName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("%w: read base runtime for status update: %w", domain.ErrModelServe, err)
	}
	_ = unstructured.SetNestedField(obj.Object, endpoint, baseRuntimeObjectStatus, baseRuntimeStatusEndpoint)
	_ = unstructured.SetNestedField(obj.Object, phase, baseRuntimeObjectStatus, baseRuntimeStatusPhase)
	_ = unstructured.SetNestedField(obj.Object, int64(readyReplicas), baseRuntimeObjectStatus, baseRuntimeStatusReadyReplica)
	if _, err := resource.UpdateStatus(ctx, obj, metav1.UpdateOptions{}); err != nil {
		if apierrors.IsMethodNotSupported(err) {
			if _, updateErr := resource.Update(ctx, obj, metav1.UpdateOptions{}); updateErr != nil {
				return fmt.Errorf("%w: update base runtime status fallback: %w", domain.ErrModelServe, updateErr)
			}
			return nil
		}
		return fmt.Errorf("%w: update base runtime status: %w", domain.ErrModelServe, err)
	}
	return nil
}

func (s *BaseRuntimeStore) RecordAdapterLoaded(ctx context.Context, resourceName string, adapter model.BaseRuntimeLoadedAdapter) error {
	log.Trace("BaseRuntimeStore RecordAdapterLoaded")

	return s.updateLoadedAdapters(ctx, resourceName, func(adapters []model.BaseRuntimeLoadedAdapter) []model.BaseRuntimeLoadedAdapter {
		for i := range adapters {
			if strings.TrimSpace(adapters[i].ServingModel) == strings.TrimSpace(adapter.ServingModel) {
				adapters[i] = adapter
				return adapters
			}
		}
		return append(adapters, adapter)
	})
}

func (s *BaseRuntimeStore) RemoveLoadedAdapter(ctx context.Context, resourceName string, servingModel string) error {
	log.Trace("BaseRuntimeStore RemoveLoadedAdapter")

	return s.updateLoadedAdapters(ctx, resourceName, func(adapters []model.BaseRuntimeLoadedAdapter) []model.BaseRuntimeLoadedAdapter {
		out := make([]model.BaseRuntimeLoadedAdapter, 0, len(adapters))
		for _, adapter := range adapters {
			if strings.TrimSpace(adapter.ServingModel) == strings.TrimSpace(servingModel) {
				continue
			}
			out = append(out, adapter)
		}
		return out
	})
}

func (s *BaseRuntimeStore) updateLoadedAdapters(ctx context.Context, resourceName string, update func([]model.BaseRuntimeLoadedAdapter) []model.BaseRuntimeLoadedAdapter) error {
	log.Trace("BaseRuntimeStore updateLoadedAdapters")

	resource := s.client.Resource(s.gvr).Namespace(s.namespace)
	obj, err := resource.Get(ctx, resourceName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("%w: read base runtime loaded adapters: %w", domain.ErrModelServe, err)
	}
	current, err := baseRuntimeLoadedAdaptersFromObject(obj)
	if err != nil {
		return err
	}
	setBaseRuntimeLoadedAdapters(obj, update(current))
	if _, err := resource.UpdateStatus(ctx, obj, metav1.UpdateOptions{}); err != nil {
		if apierrors.IsMethodNotSupported(err) {
			if _, updateErr := resource.Update(ctx, obj, metav1.UpdateOptions{}); updateErr != nil {
				return fmt.Errorf("%w: update base runtime loaded adapters fallback: %w", domain.ErrModelServe, updateErr)
			}
			return nil
		}
		return fmt.Errorf("%w: update base runtime loaded adapters: %w", domain.ErrModelServe, err)
	}
	return nil
}

func (a baseRuntimeDTOAdapter) FromObject(obj *unstructured.Unstructured) (*model.BaseRuntime, error) {
	log.Trace("baseRuntimeDTOAdapter FromObject")

	maxLoras, _, _ := unstructured.NestedInt64(obj.Object, baseRuntimeObjectSpec, baseRuntimeSpecMaxLoras)
	maxLoraRank, _, _ := unstructured.NestedInt64(obj.Object, baseRuntimeObjectSpec, baseRuntimeSpecMaxLoraRank)
	readyReplicas, _, _ := unstructured.NestedInt64(obj.Object, baseRuntimeObjectStatus, baseRuntimeStatusReadyReplica)
	endpoint, _, _ := unstructured.NestedString(obj.Object, baseRuntimeObjectStatus, baseRuntimeStatusEndpoint)
	phase, _, _ := unstructured.NestedString(obj.Object, baseRuntimeObjectStatus, baseRuntimeStatusPhase)
	loadedAdapters, err := baseRuntimeLoadedAdaptersFromObject(obj)
	if err != nil {
		return nil, err
	}
	return &model.BaseRuntime{
		ResourceName:   obj.GetName(),
		Namespace:      a.namespace,
		Generation:     obj.GetGeneration(),
		BaseModel:      baseRuntimeSpecString(obj, baseRuntimeSpecBaseModel),
		PoolKey:        baseRuntimeSpecString(obj, baseRuntimeSpecPoolKey),
		MaxLoras:       int(maxLoras),
		MaxLoraRank:    int(maxLoraRank),
		GPU:            baseRuntimeSpecString(obj, baseRuntimeSpecGPU),
		Image:          baseRuntimeSpecString(obj, baseRuntimeSpecImage),
		Endpoint:       endpoint,
		Phase:          phase,
		ReadyReplicas:  int32(readyReplicas),
		LoadedAdapters: loadedAdapters,
	}, nil
}

func setBaseRuntimeSpec(obj *unstructured.Unstructured, baseRuntime *model.BaseRuntime) {
	log.Trace("setBaseRuntimeSpec")

	_ = unstructured.SetNestedField(obj.Object, strings.TrimSpace(baseRuntime.BaseModel), baseRuntimeObjectSpec, baseRuntimeSpecBaseModel)
	_ = unstructured.SetNestedField(obj.Object, strings.TrimSpace(baseRuntime.PoolKey), baseRuntimeObjectSpec, baseRuntimeSpecPoolKey)
	_ = unstructured.SetNestedField(obj.Object, int64(baseRuntime.MaxLoras), baseRuntimeObjectSpec, baseRuntimeSpecMaxLoras)
	_ = unstructured.SetNestedField(obj.Object, int64(baseRuntime.MaxLoraRank), baseRuntimeObjectSpec, baseRuntimeSpecMaxLoraRank)
	_ = unstructured.SetNestedField(obj.Object, strings.TrimSpace(baseRuntime.GPU), baseRuntimeObjectSpec, baseRuntimeSpecGPU)
	_ = unstructured.SetNestedField(obj.Object, strings.TrimSpace(baseRuntime.Image), baseRuntimeObjectSpec, baseRuntimeSpecImage)
}

func setBaseRuntimeMutableSpec(obj *unstructured.Unstructured, baseRuntime *model.BaseRuntime) {
	log.Trace("setBaseRuntimeMutableSpec")

	_ = unstructured.SetNestedField(obj.Object, strings.TrimSpace(baseRuntime.BaseModel), baseRuntimeObjectSpec, baseRuntimeSpecBaseModel)
	_ = unstructured.SetNestedField(obj.Object, strings.TrimSpace(baseRuntime.PoolKey), baseRuntimeObjectSpec, baseRuntimeSpecPoolKey)
	_ = unstructured.SetNestedField(obj.Object, strings.TrimSpace(baseRuntime.GPU), baseRuntimeObjectSpec, baseRuntimeSpecGPU)
	_ = unstructured.SetNestedField(obj.Object, strings.TrimSpace(baseRuntime.Image), baseRuntimeObjectSpec, baseRuntimeSpecImage)
}

func baseRuntimeMutableSpecMatches(obj *unstructured.Unstructured, baseRuntime *model.BaseRuntime) bool {
	log.Trace("baseRuntimeMutableSpecMatches")

	return baseRuntimeSpecString(obj, baseRuntimeSpecBaseModel) == strings.TrimSpace(baseRuntime.BaseModel) &&
		baseRuntimeSpecString(obj, baseRuntimeSpecPoolKey) == strings.TrimSpace(baseRuntime.PoolKey) &&
		baseRuntimeSpecString(obj, baseRuntimeSpecGPU) == strings.TrimSpace(baseRuntime.GPU) &&
		baseRuntimeSpecString(obj, baseRuntimeSpecImage) == strings.TrimSpace(baseRuntime.Image)
}

func baseRuntimeSpecString(obj *unstructured.Unstructured, field string) string {
	log.Trace("baseRuntimeSpecString")

	value, _, _ := unstructured.NestedString(obj.Object, baseRuntimeObjectSpec, field)
	return value
}

func baseRuntimeLoadedAdaptersFromObject(obj *unstructured.Unstructured) ([]model.BaseRuntimeLoadedAdapter, error) {
	log.Trace("baseRuntimeLoadedAdaptersFromObject")

	rawAdapters, _, _ := unstructured.NestedSlice(obj.Object, baseRuntimeObjectStatus, baseRuntimeStatusLoaded)
	adapters := make([]model.BaseRuntimeLoadedAdapter, 0, len(rawAdapters))
	for _, raw := range rawAdapters {
		item, ok := raw.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%w: invalid loaded adapter entry", domain.ErrValidationFailed)
		}
		servingModel, _ := item[baseRuntimeAdapterModel].(string)
		resourceName, _ := item[baseRuntimeAdapterResource].(string)
		modelIDRaw, _ := item[baseRuntimeAdapterModelID].(string)
		var modelID uuid.UUID
		if strings.TrimSpace(modelIDRaw) != "" {
			parsed, err := uuid.Parse(modelIDRaw)
			if err != nil {
				return nil, fmt.Errorf("%w: invalid loaded adapter model id: %w", domain.ErrValidationFailed, err)
			}
			modelID = parsed
		}
		observedGeneration, _ := item[baseRuntimeAdapterGeneration].(int64)
		lastUsedRaw, _ := item[baseRuntimeAdapterLastUsed].(string)
		var lastUsedAt time.Time
		if strings.TrimSpace(lastUsedRaw) != "" {
			parsed, err := time.Parse(time.RFC3339Nano, lastUsedRaw)
			if err != nil {
				return nil, fmt.Errorf("%w: invalid loaded adapter last used time: %w", domain.ErrValidationFailed, err)
			}
			lastUsedAt = parsed
		}
		pinned, _ := item[baseRuntimeAdapterPinned].(bool)
		adapters = append(adapters, model.BaseRuntimeLoadedAdapter{
			ServingModel:            strings.TrimSpace(servingModel),
			ServedModelResourceName: strings.TrimSpace(resourceName),
			ModelID:                 modelID,
			ObservedGeneration:      observedGeneration,
			LastUsedAt:              lastUsedAt,
			Pinned:                  pinned,
		})
	}
	return adapters, nil
}

func setBaseRuntimeLoadedAdapters(obj *unstructured.Unstructured, adapters []model.BaseRuntimeLoadedAdapter) {
	log.Trace("setBaseRuntimeLoadedAdapters")

	rawAdapters := make([]any, 0, len(adapters))
	for _, adapter := range adapters {
		item := map[string]any{
			baseRuntimeAdapterModel:      strings.TrimSpace(adapter.ServingModel),
			baseRuntimeAdapterResource:   strings.TrimSpace(adapter.ServedModelResourceName),
			baseRuntimeAdapterGeneration: adapter.ObservedGeneration,
			baseRuntimeAdapterLastUsed:   adapter.LastUsedAt.UTC().Format(time.RFC3339Nano),
			baseRuntimeAdapterPinned:     adapter.Pinned,
		}
		if adapter.ModelID != uuid.Nil {
			item[baseRuntimeAdapterModelID] = adapter.ModelID.String()
		}
		rawAdapters = append(rawAdapters, item)
	}
	_ = unstructured.SetNestedSlice(obj.Object, rawAdapters, baseRuntimeObjectStatus, baseRuntimeStatusLoaded)
}
