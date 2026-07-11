package kubernetes

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"model_serving_service/pkg/domain"
	"model_serving_service/pkg/domain/model"

	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

type VLLMRuntimeConfig struct {
	Namespace               string
	Image                   string
	ImagePullPolicy         string
	ServiceAccount          string
	ForceDedicated          bool
	MaxLoras                int
	MaxLoraRank             int
	Replicas                int32
	Port                    int32
	CPU                     string
	Memory                  string
	GPUResource             string
	GPU                     string
	RequestTimeout          time.Duration
	HTTPClient              *http.Client
	BaseRuntimeStore        *BaseRuntimeStore
	ServedModelStatusWriter servedModelStatusWriter
	Now                     func() time.Time
}

type VLLMRuntime struct {
	namespace       string
	image           string
	imagePullPolicy string
	serviceAccount  string
	forceDedicated  bool
	maxLoras        int
	maxLoraRank     int
	replicas        int32
	port            int32
	cpu             string
	memory          string
	gpuResource     string
	gpu             string
	httpClient      *http.Client
	client          dynamic.Interface
	baseRuntimes    *BaseRuntimeStore
	servedModels    servedModelStatusWriter
	now             func() time.Time
}

type servedModelStatusWriter interface {
	UpdateStatus(ctx context.Context, resourceName string, status *model.ServedModelStatus) error
}

var (
	deploymentGVR = schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
	serviceGVR    = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "services"}
)

const (
	vllmRuntimeLoraUpdatingEnv     = "VLLM_ALLOW_RUNTIME_LORA_UPDATING"
	vllmRuntimeLoraUpdatingEnabled = "1"
)

func NewVLLMRuntime(config VLLMRuntimeConfig, client dynamic.Interface) (*VLLMRuntime, error) {
	log.Trace("NewVLLMRuntime")

	if strings.TrimSpace(config.Namespace) == "" {
		return nil, domain.ErrValidationFailed.Extend("serving namespace is required")
	}
	if strings.TrimSpace(config.Image) == "" {
		return nil, domain.ErrValidationFailed.Extend("vllm image is required")
	}
	if strings.TrimSpace(config.ImagePullPolicy) == "" {
		return nil, domain.ErrValidationFailed.Extend("vllm image pull policy is required")
	}
	if config.Replicas <= 0 {
		return nil, domain.ErrValidationFailed.Extend("serving replicas must be greater than zero")
	}
	if config.Port <= 0 {
		return nil, domain.ErrValidationFailed.Extend("serving port must be greater than zero")
	}
	if config.MaxLoraRank <= 0 {
		return nil, domain.ErrValidationFailed.Extend("vllm max lora rank must be greater than zero")
	}
	if config.MaxLoras <= 0 {
		return nil, domain.ErrValidationFailed.Extend("vllm max loras must be greater than zero")
	}
	if !config.ForceDedicated && config.BaseRuntimeStore == nil {
		return nil, domain.ErrValidationFailed.Extend("base runtime store is required")
	}
	if strings.TrimSpace(config.CPU) == "" {
		return nil, domain.ErrValidationFailed.Extend("vllm cpu is required")
	}
	if strings.TrimSpace(config.Memory) == "" {
		return nil, domain.ErrValidationFailed.Extend("vllm memory is required")
	}
	if client == nil {
		return nil, domain.ErrValidationFailed.Extend("kubernetes client is required")
	}
	httpClient := config.HTTPClient
	if httpClient == nil {
		if config.RequestTimeout <= 0 {
			return nil, domain.ErrValidationFailed.Extend("vllm request timeout must be greater than zero")
		}
		httpClient = &http.Client{
			Timeout:   config.RequestTimeout,
			Transport: otelhttp.NewTransport(http.DefaultTransport),
		}
	}
	return &VLLMRuntime{
		namespace:       strings.TrimSpace(config.Namespace),
		image:           strings.TrimSpace(config.Image),
		imagePullPolicy: strings.TrimSpace(config.ImagePullPolicy),
		serviceAccount:  strings.TrimSpace(config.ServiceAccount),
		forceDedicated:  config.ForceDedicated,
		maxLoras:        config.MaxLoras,
		maxLoraRank:     config.MaxLoraRank,
		replicas:        config.Replicas,
		port:            config.Port,
		cpu:             strings.TrimSpace(config.CPU),
		memory:          strings.TrimSpace(config.Memory),
		gpuResource:     strings.TrimSpace(config.GPUResource),
		gpu:             strings.TrimSpace(config.GPU),
		httpClient:      httpClient,
		client:          client,
		baseRuntimes:    config.BaseRuntimeStore,
		servedModels:    config.ServedModelStatusWriter,
		now:             vllmRuntimeNow(config.Now),
	}, nil
}

func vllmRuntimeNow(now func() time.Time) func() time.Time {
	log.Trace("vllmRuntimeNow")

	if now != nil {
		return now
	}
	return time.Now
}

func (r *VLLMRuntime) EnsureServedModel(ctx context.Context, servedModel *model.ServedModel) (*model.ServingRuntimeState, error) {
	log.Trace("VLLMRuntime EnsureServedModel")

	if strings.TrimSpace(servedModel.BaseModel) == "" {
		return nil, domain.ErrValidationFailed.Extend("base model is required")
	}
	if strings.EqualFold(strings.TrimSpace(servedModel.ModelKind), "FINE_TUNED") && strings.TrimSpace(servedModel.AdapterURI) == "" {
		return &model.ServingRuntimeState{
			Failed:          true,
			ServingProtocol: model.ServingProtocolOpenAIChatCompletions,
			FailureReason:   "fine-tuned model has no adapter URI",
		}, nil
	}
	if failure := validateAdapterRankKnown(servedModel); failure != "" {
		return &model.ServingRuntimeState{
			Failed:          true,
			ServingProtocol: model.ServingProtocolOpenAIChatCompletions,
			FailureReason:   failure,
		}, nil
	}
	sharedAdapter := r.sharedAdapter(servedModel)
	baseRuntime, err := r.resolveBaseRuntime(ctx, servedModel, sharedAdapter)
	if err != nil {
		return nil, err
	}
	if failure := r.validateAdapterCompat(servedModel, baseRuntime); failure != "" {
		return &model.ServingRuntimeState{
			Failed:          true,
			ServingProtocol: model.ServingProtocolOpenAIChatCompletions,
			FailureReason:   failure,
		}, nil
	}
	maxLoras, maxLoraRank := r.loraLimits(baseRuntime)
	workloadName := r.workloadName(servedModel, sharedAdapter)
	if baseRuntime != nil {
		workloadName = baseRuntime.ResourceName
	}
	servingModel := ServingModelName(servedModel)
	runtimeServingModel := servingModel
	if sharedAdapter {
		runtimeServingModel = SharedRuntimeServingModelName(servedModel)
	}
	loadedServingModel := servingModel
	if strings.TrimSpace(servedModel.AdapterURI) == "" {
		loadedServingModel = runtimeServingModel
	}
	servingTarget := strings.TrimSpace(servedModel.ServingTarget)
	if servingTarget == "" {
		servingTarget = ServiceEndpoint(r.namespace, workloadName, r.port)
		if baseRuntime != nil && strings.TrimSpace(baseRuntime.Endpoint) != "" {
			servingTarget = strings.TrimSpace(baseRuntime.Endpoint)
		}
	}
	if err := r.upsertDeployment(ctx, servedModel, workloadName, runtimeServingModel, maxLoras, maxLoraRank); err != nil {
		return nil, err
	}
	if err := r.upsertService(ctx, servedModel, workloadName); err != nil {
		return nil, err
	}
	readyReplicas, deploymentReady, failed, failureReason, err := r.deploymentState(ctx, workloadName)
	if err != nil {
		return nil, err
	}
	if baseRuntime != nil {
		phase := "Pending"
		if failed {
			phase = "Failed"
		} else if deploymentReady {
			phase = "Ready"
		}
		if err := r.baseRuntimes.UpdateStatus(ctx, baseRuntime.ResourceName, ServiceEndpoint(r.namespace, workloadName, r.port), phase, readyReplicas); err != nil {
			return nil, err
		}
	}
	state := &model.ServingRuntimeState{
		Failed:          failed,
		ServingTarget:   servingTarget,
		ServingModel:    loadedServingModel,
		ServingProtocol: model.ServingProtocolOpenAIChatCompletions,
		FailureReason:   failureReason,
		ReadyReplicas:   readyReplicas,
	}
	if failed || !deploymentReady {
		return state, nil
	}
	confirmed, adapterFailed, failureReason := r.ensureServingModel(ctx, servingTarget, loadedServingModel, servedModel, sharedAdapter, baseRuntime)
	state.Ready = confirmed
	state.Failed = adapterFailed
	state.FailureReason = failureReason
	if confirmed {
		state.FailureReason = ""
	}
	return state, nil
}

func (r *VLLMRuntime) loraLimits(baseRuntime *model.BaseRuntime) (int, int) {
	log.Trace("VLLMRuntime loraLimits")

	maxLoras := r.maxLoras
	maxLoraRank := r.maxLoraRank
	if baseRuntime != nil {
		if baseRuntime.MaxLoras > 0 {
			maxLoras = baseRuntime.MaxLoras
		}
		if baseRuntime.MaxLoraRank > 0 {
			maxLoraRank = baseRuntime.MaxLoraRank
		}
	}
	return maxLoras, maxLoraRank
}

func (r *VLLMRuntime) resolveBaseRuntime(ctx context.Context, servedModel *model.ServedModel, sharedAdapter bool) (*model.BaseRuntime, error) {
	log.Trace("VLLMRuntime resolveBaseRuntime")

	if !sharedAdapter {
		return nil, nil
	}
	if r.baseRuntimes == nil {
		return nil, domain.ErrValidationFailed.Extend("base runtime store is required")
	}
	return r.baseRuntimes.FindOrCreate(ctx, &model.BaseRuntime{
		ResourceName: BaseRuntimeResourceName(servedModel.BaseModel, RuntimePoolKey(servedModel)),
		BaseModel:    strings.TrimSpace(servedModel.BaseModel),
		PoolKey:      RuntimePoolKey(servedModel),
		MaxLoras:     r.maxLoras,
		MaxLoraRank:  r.maxLoraRank,
		GPU:          strings.TrimSpace(r.gpu),
		Image:        strings.TrimSpace(r.image),
	})
}

func (r *VLLMRuntime) sharedAdapter(servedModel *model.ServedModel) bool {
	log.Trace("VLLMRuntime sharedAdapter")

	return servedModel.IsAdapter() && !r.forceDedicated
}

func (r *VLLMRuntime) DeleteServedModel(ctx context.Context, servedModel *model.ServedModel) error {
	log.Trace("VLLMRuntime DeleteServedModel")

	if !r.sharedAdapter(servedModel) || r.baseRuntimes == nil {
		return nil
	}
	resourceName := BaseRuntimeResourceName(servedModel.BaseModel, RuntimePoolKey(servedModel))
	baseRuntime, err := r.baseRuntimes.Read(ctx, resourceName)
	if err != nil {
		if errors.Is(err, domain.ErrServedModelNotFound) {
			return nil
		}
		return err
	}
	servingTarget := strings.TrimSpace(baseRuntime.Endpoint)
	if servingTarget == "" {
		servingTarget = ServiceEndpoint(r.namespace, resourceName, r.port)
	}
	servingModel := ServingModelName(servedModel)
	if failure := r.unloadLoraAdapter(ctx, servingTarget, servingModel); failure != "" {
		return domain.ErrModelServe.Extend(failure)
	}
	return r.baseRuntimes.RemoveLoadedAdapter(ctx, resourceName, servingModel)
}

func (r *VLLMRuntime) workloadName(servedModel *model.ServedModel, sharedAdapter bool) string {
	log.Trace("VLLMRuntime workloadName")

	if sharedAdapter {
		return SharedRuntimeWorkloadName(servedModel)
	}
	return WorkloadName(servedModel)
}

type vllmModelsResponse struct {
	Data []struct {
		ID string `json:"id"`
	} `json:"data"`
}

type vllmLoadLoraAdapterRequest struct {
	LoraName string `json:"lora_name"`
	LoraPath string `json:"lora_path"`
}

type vllmUnloadLoraAdapterRequest struct {
	LoraName string `json:"lora_name"`
}

func (r *VLLMRuntime) ensureServingModel(ctx context.Context, servingTarget string, servingModel string, servedModel *model.ServedModel, sharedAdapter bool, baseRuntime *model.BaseRuntime) (bool, bool, string) {
	log.Trace("VLLMRuntime ensureServingModel")

	confirmed, failureReason := r.confirmServingModel(ctx, servingTarget, servingModel)
	if confirmed {
		if sharedAdapter {
			if recordErr := r.recordAdapterLoaded(ctx, baseRuntime, servedModel, servingModel); recordErr != nil {
				return false, true, recordErr.Error()
			}
		}
		return true, false, ""
	}
	if strings.TrimSpace(servedModel.AdapterURI) == "" {
		return false, false, failureReason
	}
	if !sharedAdapter {
		return false, false, failureReason
	}
	if capacityFailure := r.prepareAdapterCapacity(ctx, servingTarget, baseRuntime, servedModel, servingModel); capacityFailure != "" {
		return false, true, capacityFailure
	}
	if loadFailure := r.loadLoraAdapter(ctx, servingTarget, servingModel, servedModel.AdapterURI); loadFailure != "" {
		return false, true, loadFailure
	}
	confirmed, failureReason = r.confirmServingModel(ctx, servingTarget, servingModel)
	if !confirmed {
		return false, false, failureReason
	}
	if recordErr := r.recordAdapterLoaded(ctx, baseRuntime, servedModel, servingModel); recordErr != nil {
		return false, true, recordErr.Error()
	}
	return true, false, ""
}

func (r *VLLMRuntime) recordAdapterLoaded(ctx context.Context, baseRuntime *model.BaseRuntime, servedModel *model.ServedModel, servingModel string) error {
	log.Trace("VLLMRuntime recordAdapterLoaded")

	if baseRuntime == nil || r.baseRuntimes == nil || !servedModel.IsAdapter() {
		return nil
	}
	return r.baseRuntimes.RecordAdapterLoaded(ctx, baseRuntime.ResourceName, model.BaseRuntimeLoadedAdapter{
		ServingModel:            strings.TrimSpace(servingModel),
		ServedModelResourceName: strings.TrimSpace(servedModel.ResourceName),
		ModelID:                 servedModel.ModelID,
		ObservedGeneration:      servedModel.Generation,
		LastUsedAt:              r.now().UTC(),
		Pinned:                  servedModel.Pinned,
	})
}

func (r *VLLMRuntime) prepareAdapterCapacity(ctx context.Context, servingTarget string, baseRuntime *model.BaseRuntime, servedModel *model.ServedModel, servingModel string) string {
	log.Trace("VLLMRuntime prepareAdapterCapacity")

	if baseRuntime == nil || !servedModel.IsAdapter() {
		return ""
	}
	if loadedAdapterIndex(baseRuntime.LoadedAdapters, servingModel) >= 0 {
		return ""
	}
	maxLoras, _ := r.loraLimits(baseRuntime)
	if len(baseRuntime.LoadedAdapters) < maxLoras {
		return ""
	}
	victimIndex := lruEvictionCandidate(baseRuntime.LoadedAdapters)
	if victimIndex < 0 {
		return "base runtime at capacity with all adapters pinned"
	}
	victim := baseRuntime.LoadedAdapters[victimIndex]
	if unloadFailure := r.unloadLoraAdapter(ctx, servingTarget, victim.ServingModel); unloadFailure != "" {
		return unloadFailure
	}
	if r.baseRuntimes != nil {
		if err := r.baseRuntimes.RemoveLoadedAdapter(ctx, baseRuntime.ResourceName, victim.ServingModel); err != nil {
			return err.Error()
		}
		baseRuntime.LoadedAdapters = removeLoadedAdapter(baseRuntime.LoadedAdapters, victim.ServingModel)
	}
	if r.servedModels != nil && strings.TrimSpace(victim.ServedModelResourceName) != "" {
		status := &model.ServedModelStatus{
			ServingLoadStatus:  model.ModelLoadStatusNotLoaded,
			ServingTarget:      strings.TrimSpace(servingTarget),
			ServingModel:       strings.TrimSpace(victim.ServingModel),
			ServingProtocol:    model.ServingProtocolOpenAIChatCompletions,
			FailureReason:      model.NotLoadedReasonCapacityEvicted,
			ObservedGeneration: victim.ObservedGeneration,
			ReadyReplicas:      baseRuntime.ReadyReplicas,
		}
		if err := r.servedModels.UpdateStatus(ctx, victim.ServedModelResourceName, status); err != nil {
			return err.Error()
		}
	}
	return ""
}

func validateAdapterRankKnown(servedModel *model.ServedModel) string {
	log.Trace("validateAdapterRankKnown")

	if !servedModel.IsAdapter() {
		return ""
	}
	if servedModel.AdapterRank <= 0 {
		return "unknown adapter rank"
	}
	return ""
}

func (r *VLLMRuntime) validateAdapterCompat(servedModel *model.ServedModel, baseRuntime *model.BaseRuntime) string {
	log.Trace("VLLMRuntime validateAdapterCompat")

	if !servedModel.IsAdapter() {
		return ""
	}
	if baseRuntime != nil && strings.TrimSpace(baseRuntime.BaseModel) != strings.TrimSpace(servedModel.BaseModel) {
		return fmt.Sprintf("adapter base model %s does not match base runtime %s", strings.TrimSpace(servedModel.BaseModel), strings.TrimSpace(baseRuntime.BaseModel))
	}
	_, maxLoraRank := r.loraLimits(baseRuntime)
	if servedModel.AdapterRank > maxLoraRank {
		return fmt.Sprintf("adapter rank %d exceeds max lora rank %d", servedModel.AdapterRank, maxLoraRank)
	}
	return ""
}

func (r *VLLMRuntime) loadLoraAdapter(ctx context.Context, servingTarget string, servingModel string, adapterURI string) string {
	log.Trace("VLLMRuntime loadLoraAdapter")

	body, err := json.Marshal(vllmLoadLoraAdapterRequest{
		LoraName: servingModel,
		LoraPath: strings.TrimSpace(adapterURI),
	})
	if err != nil {
		return err.Error()
	}
	endpoint := strings.TrimRight(servingTarget, "/") + "/v1/load_lora_adapter"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err.Error()
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.httpClient.Do(req)
	if err != nil {
		return err.Error()
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		message := strings.TrimSpace(string(raw))
		if isIdempotentLoraLoadResponse(resp.StatusCode, message) {
			return ""
		}
		if message != "" {
			return fmt.Sprintf("vllm lora load returned status %d: %s", resp.StatusCode, message)
		}
		return fmt.Sprintf("vllm lora load returned status %d", resp.StatusCode)
	}
	return ""
}

func (r *VLLMRuntime) unloadLoraAdapter(ctx context.Context, servingTarget string, servingModel string) string {
	log.Trace("VLLMRuntime unloadLoraAdapter")

	body, err := json.Marshal(vllmUnloadLoraAdapterRequest{
		LoraName: strings.TrimSpace(servingModel),
	})
	if err != nil {
		return err.Error()
	}
	endpoint := strings.TrimRight(servingTarget, "/") + "/v1/unload_lora_adapter"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err.Error()
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.httpClient.Do(req)
	if err != nil {
		return err.Error()
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return ""
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		message := strings.TrimSpace(string(raw))
		if message != "" {
			return fmt.Sprintf("vllm lora unload returned status %d: %s", resp.StatusCode, message)
		}
		return fmt.Sprintf("vllm lora unload returned status %d", resp.StatusCode)
	}
	return ""
}

func loadedAdapterIndex(adapters []model.BaseRuntimeLoadedAdapter, servingModel string) int {
	log.Trace("loadedAdapterIndex")

	servingModel = strings.TrimSpace(servingModel)
	for i := range adapters {
		if strings.TrimSpace(adapters[i].ServingModel) == servingModel {
			return i
		}
	}
	return -1
}

func lruEvictionCandidate(adapters []model.BaseRuntimeLoadedAdapter) int {
	log.Trace("lruEvictionCandidate")

	victim := -1
	for i := range adapters {
		if adapters[i].Pinned {
			continue
		}
		if victim < 0 || adapters[i].LastUsedAt.Before(adapters[victim].LastUsedAt) {
			victim = i
		}
	}
	return victim
}

func removeLoadedAdapter(adapters []model.BaseRuntimeLoadedAdapter, servingModel string) []model.BaseRuntimeLoadedAdapter {
	log.Trace("removeLoadedAdapter")

	out := make([]model.BaseRuntimeLoadedAdapter, 0, len(adapters))
	servingModel = strings.TrimSpace(servingModel)
	for _, adapter := range adapters {
		if strings.TrimSpace(adapter.ServingModel) == servingModel {
			continue
		}
		out = append(out, adapter)
	}
	return out
}

func isIdempotentLoraLoadResponse(statusCode int, body string) bool {
	log.Trace("isIdempotentLoraLoadResponse")

	normalized := strings.ToLower(body)
	if !strings.Contains(normalized, "already") || (!strings.Contains(normalized, "loaded") && !strings.Contains(normalized, "exist")) {
		return false
	}
	return statusCode == http.StatusConflict || statusCode == http.StatusBadRequest
}

func (r *VLLMRuntime) confirmServingModel(ctx context.Context, servingTarget string, servingModel string) (bool, string) {
	log.Trace("VLLMRuntime confirmServingModel")

	endpoint := strings.TrimRight(servingTarget, "/") + "/v1/models"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return false, err.Error()
	}
	resp, err := r.httpClient.Do(req)
	if err != nil {
		return false, err.Error()
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return false, fmt.Sprintf("vllm model list returned status %d", resp.StatusCode)
	}
	var payload vllmModelsResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return false, fmt.Sprintf("decode vllm model list: %s", err.Error())
	}
	for _, item := range payload.Data {
		if strings.TrimSpace(item.ID) == servingModel {
			return true, ""
		}
	}
	return false, fmt.Sprintf("serving model %s is not listed by vllm", servingModel)
}

func copyImmutableServiceFields(existing *unstructured.Unstructured, desired *unstructured.Unstructured) {
	log.Trace("copyImmutableServiceFields")

	for _, field := range []string{"clusterIP", "clusterIPs", "ipFamilies", "ipFamilyPolicy"} {
		value, found, _ := unstructured.NestedFieldNoCopy(existing.Object, "spec", field)
		if found {
			_ = unstructured.SetNestedField(desired.Object, value, "spec", field)
		}
	}
}

func deploymentConditionString(condition map[string]any, field string) string {
	log.Trace("deploymentConditionString")

	value, _ := condition[field].(string)
	return strings.TrimSpace(value)
}

func deploymentFailureReason(deployment *unstructured.Unstructured) (bool, string) {
	log.Trace("deploymentFailureReason")

	conditions, _, _ := unstructured.NestedSlice(deployment.Object, "status", "conditions")
	for _, raw := range conditions {
		condition, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		conditionType := deploymentConditionString(condition, "type")
		conditionStatus := deploymentConditionString(condition, "status")
		reason := deploymentConditionString(condition, "reason")
		message := deploymentConditionString(condition, "message")
		if conditionType == "Progressing" && conditionStatus == "False" && reason == "ProgressDeadlineExceeded" {
			if message != "" {
				return true, message
			}
			return true, "vllm deployment exceeded progress deadline"
		}
		if conditionType == "ReplicaFailure" && conditionStatus == "True" {
			if message != "" {
				return true, message
			}
			return true, "vllm deployment replica failure"
		}
	}
	return false, ""
}

func (r *VLLMRuntime) upsertDeployment(ctx context.Context, servedModel *model.ServedModel, workloadName string, servingModel string, maxLoras int, maxLoraRank int) error {
	log.Trace("VLLMRuntime upsertDeployment")

	resource := r.client.Resource(deploymentGVR).Namespace(r.namespace)
	desired := r.deploymentObject(servedModel, workloadName, servingModel, maxLoras, maxLoraRank)
	existing, err := resource.Get(ctx, workloadName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = resource.Create(ctx, desired, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("%w: create vllm deployment: %w", domain.ErrModelServe, err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("%w: read vllm deployment: %w", domain.ErrModelServe, err)
	}
	desired.SetResourceVersion(existing.GetResourceVersion())
	if status, ok := existing.Object["status"]; ok {
		desired.Object["status"] = status
	}
	if _, err := resource.Update(ctx, desired, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("%w: update vllm deployment: %w", domain.ErrModelServe, err)
	}
	return nil
}

func (r *VLLMRuntime) upsertService(ctx context.Context, servedModel *model.ServedModel, workloadName string) error {
	log.Trace("VLLMRuntime upsertService")

	resource := r.client.Resource(serviceGVR).Namespace(r.namespace)
	desired := r.serviceObject(servedModel, workloadName)
	existing, err := resource.Get(ctx, workloadName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = resource.Create(ctx, desired, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("%w: create vllm service: %w", domain.ErrModelServe, err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("%w: read vllm service: %w", domain.ErrModelServe, err)
	}
	desired.SetResourceVersion(existing.GetResourceVersion())
	copyImmutableServiceFields(existing, desired)
	if _, err := resource.Update(ctx, desired, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("%w: update vllm service: %w", domain.ErrModelServe, err)
	}
	return nil
}

func (r *VLLMRuntime) deploymentState(ctx context.Context, workloadName string) (int32, bool, bool, string, error) {
	log.Trace("VLLMRuntime deploymentState")

	deployment, err := r.client.Resource(deploymentGVR).Namespace(r.namespace).Get(ctx, workloadName, metav1.GetOptions{})
	if err != nil {
		return 0, false, false, "", fmt.Errorf("%w: read vllm deployment readiness: %w", domain.ErrModelServe, err)
	}
	readyReplicasRaw, _, _ := unstructured.NestedInt64(deployment.Object, "status", "readyReplicas")
	observedGeneration, _, _ := unstructured.NestedInt64(deployment.Object, "status", "observedGeneration")
	readyReplicas := int32(readyReplicasRaw)
	failed, failureReason := deploymentFailureReason(deployment)
	return readyReplicas, readyReplicas >= r.replicas && observedGeneration >= deployment.GetGeneration(), failed, failureReason, nil
}

func (r *VLLMRuntime) deploymentObject(servedModel *model.ServedModel, workloadName string, servingModel string, maxLoras int, maxLoraRank int) *unstructured.Unstructured {
	log.Trace("VLLMRuntime deploymentObject")

	labels := servingLabels(servedModel, workloadName)
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata": map[string]any{
			"name":      workloadName,
			"namespace": r.namespace,
			"labels":    labels,
		},
		"spec": map[string]any{
			"replicas": int64(r.replicas),
			"selector": map[string]any{
				"matchLabels": labels,
			},
			"template": map[string]any{
				"metadata": map[string]any{
					"labels": labels,
				},
				"spec": r.podSpec(servedModel, servingModel, maxLoras, maxLoraRank),
			},
		},
	}}
}

func (r *VLLMRuntime) serviceObject(servedModel *model.ServedModel, workloadName string) *unstructured.Unstructured {
	log.Trace("VLLMRuntime serviceObject")

	labels := servingLabels(servedModel, workloadName)
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Service",
		"metadata": map[string]any{
			"name":      workloadName,
			"namespace": r.namespace,
			"labels":    labels,
		},
		"spec": map[string]any{
			"type":     "ClusterIP",
			"selector": labels,
			"ports": []any{
				map[string]any{
					"name":       "http",
					"port":       int64(r.port),
					"targetPort": int64(r.port),
					"protocol":   "TCP",
				},
			},
		},
	}}
}

func (r *VLLMRuntime) podSpec(servedModel *model.ServedModel, servingModel string, maxLoras int, maxLoraRank int) map[string]any {
	log.Trace("VLLMRuntime podSpec")

	args := []any{
		"--model", strings.TrimSpace(servedModel.BaseModel),
		"--served-model-name", servingModel,
	}
	if strings.TrimSpace(servedModel.AdapterURI) != "" {
		args = append(args, "--enable-lora")
		args = append(args, "--max-loras", fmt.Sprintf("%d", maxLoras))
		args = append(args, "--max-lora-rank", fmt.Sprintf("%d", maxLoraRank))
	}
	if strings.TrimSpace(servedModel.AdapterURI) != "" && !r.sharedAdapter(servedModel) {
		args = append(args, "--lora-modules", fmt.Sprintf("%s=%s", servingModel, strings.TrimSpace(servedModel.AdapterURI)))
	}
	container := map[string]any{
		"name":            "vllm",
		"image":           r.image,
		"imagePullPolicy": r.imagePullPolicy,
		"args":            args,
		"ports": []any{
			map[string]any{"name": "http", "containerPort": int64(r.port), "protocol": "TCP"},
		},
		"readinessProbe": map[string]any{
			"httpGet":          map[string]any{"path": "/health", "port": int64(r.port)},
			"periodSeconds":    int64(10),
			"failureThreshold": int64(12),
		},
		"resources": r.resources(),
	}
	if strings.TrimSpace(servedModel.AdapterURI) != "" && r.sharedAdapter(servedModel) {
		container["env"] = []any{
			map[string]any{"name": vllmRuntimeLoraUpdatingEnv, "value": vllmRuntimeLoraUpdatingEnabled},
		}
	}
	spec := map[string]any{
		"containers": []any{
			container,
		},
	}
	if r.serviceAccount != "" {
		spec["serviceAccountName"] = r.serviceAccount
	}
	if r.gpuResource != "" && r.gpu != "" {
		spec["nodeSelector"] = map[string]any{"workload": "gpu"}
		spec["tolerations"] = []any{
			map[string]any{
				"key":      "nvidia.com/gpu",
				"operator": "Equal",
				"value":    "true",
				"effect":   "NoSchedule",
			},
		}
	}
	return spec
}

func (r *VLLMRuntime) resources() map[string]any {
	log.Trace("VLLMRuntime resources")

	limits := map[string]any{"cpu": r.cpu, "memory": r.memory}
	requests := map[string]any{"cpu": r.cpu, "memory": r.memory}
	if r.gpuResource != "" && r.gpu != "" {
		limits[r.gpuResource] = r.gpu
		requests[r.gpuResource] = r.gpu
	}
	return map[string]any{"limits": limits, "requests": requests}
}

func servingLabels(servedModel *model.ServedModel, workloadName string) map[string]any {
	log.Trace("servingLabels")

	return map[string]any{
		"app.kubernetes.io/name":       "served-model",
		"app.kubernetes.io/managed-by": "model-serving-service",
		"bighill.io/served-model":      workloadName,
	}
}
