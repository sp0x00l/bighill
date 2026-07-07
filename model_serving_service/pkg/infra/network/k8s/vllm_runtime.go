package k8s

import (
	"bytes"
	"context"
	"encoding/json"
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
	Namespace       string
	Image           string
	ImagePullPolicy string
	ServiceAccount  string
	MultiTenant     bool
	Replicas        int32
	Port            int32
	CPU             string
	Memory          string
	GPUResource     string
	GPU             string
	RequestTimeout  time.Duration
	HTTPClient      *http.Client
}

type VLLMRuntime struct {
	namespace       string
	image           string
	imagePullPolicy string
	serviceAccount  string
	multiTenant     bool
	replicas        int32
	port            int32
	cpu             string
	memory          string
	gpuResource     string
	gpu             string
	httpClient      *http.Client
	client          dynamic.Interface
}

var (
	deploymentGVR = schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
	serviceGVR    = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "services"}
)

func NewVLLMRuntime(config VLLMRuntimeConfig, client dynamic.Interface) (*VLLMRuntime, error) {
	log.Trace("NewVLLMRuntime")

	if strings.TrimSpace(config.Namespace) == "" {
		return nil, domain.ErrValidationFailed.Extend("serving namespace is required")
	}
	if strings.TrimSpace(config.Image) == "" {
		return nil, domain.ErrValidationFailed.Extend("vllm image is required")
	}
	if config.Replicas <= 0 {
		return nil, domain.ErrValidationFailed.Extend("serving replicas must be greater than zero")
	}
	if config.Port <= 0 {
		return nil, domain.ErrValidationFailed.Extend("serving port must be greater than zero")
	}
	if client == nil {
		return nil, domain.ErrValidationFailed.Extend("kubernetes client is required")
	}
	httpClient := config.HTTPClient
	if httpClient == nil {
		timeout := config.RequestTimeout
		if timeout <= 0 {
			timeout = 5 * time.Second
		}
		httpClient = &http.Client{
			Timeout:   timeout,
			Transport: otelhttp.NewTransport(http.DefaultTransport),
		}
	}
	return &VLLMRuntime{
		namespace:       strings.TrimSpace(config.Namespace),
		image:           strings.TrimSpace(config.Image),
		imagePullPolicy: withDefaultString(config.ImagePullPolicy, "IfNotPresent"),
		serviceAccount:  strings.TrimSpace(config.ServiceAccount),
		multiTenant:     config.MultiTenant,
		replicas:        config.Replicas,
		port:            config.Port,
		cpu:             withDefaultString(config.CPU, "1"),
		memory:          withDefaultString(config.Memory, "4Gi"),
		gpuResource:     strings.TrimSpace(config.GPUResource),
		gpu:             strings.TrimSpace(config.GPU),
		httpClient:      httpClient,
		client:          client,
	}, nil
}

func (r *VLLMRuntime) EnsureServedModel(ctx context.Context, servedModel *model.ServedModel) (*model.ServingRuntimeState, error) {
	log.Trace("VLLMRuntime EnsureServedModel")

	if strings.TrimSpace(servedModel.BaseModel) == "" {
		return nil, domain.ErrValidationFailed.Extend("base model is required")
	}
	workloadName := r.workloadName(servedModel)
	servingModel := ServingModelName(servedModel)
	runtimeServingModel := servingModel
	if r.multiTenant {
		runtimeServingModel = SharedRuntimeServingModelName(servedModel)
	}
	loadedServingModel := servingModel
	if strings.TrimSpace(servedModel.AdapterURI) == "" {
		loadedServingModel = runtimeServingModel
	}
	servingTarget := strings.TrimSpace(servedModel.ServingTarget)
	if servingTarget == "" {
		servingTarget = ServiceEndpoint(r.namespace, workloadName, r.port)
	}
	if err := r.upsertDeployment(ctx, servedModel, workloadName, runtimeServingModel); err != nil {
		return nil, err
	}
	if err := r.upsertService(ctx, servedModel, workloadName); err != nil {
		return nil, err
	}
	readyReplicas, deploymentReady, failed, failureReason, err := r.deploymentState(ctx, workloadName)
	if err != nil {
		return nil, err
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
	confirmed, adapterFailed, failureReason := r.ensureServingModel(ctx, servingTarget, loadedServingModel, servedModel.AdapterURI)
	state.Ready = confirmed
	state.Failed = adapterFailed
	state.FailureReason = failureReason
	if confirmed {
		state.FailureReason = ""
	}
	return state, nil
}

func (r *VLLMRuntime) workloadName(servedModel *model.ServedModel) string {
	log.Trace("VLLMRuntime workloadName")

	if r.multiTenant {
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

func (r *VLLMRuntime) ensureServingModel(ctx context.Context, servingTarget string, servingModel string, adapterURI string) (bool, bool, string) {
	log.Trace("VLLMRuntime ensureServingModel")

	confirmed, failureReason := r.confirmServingModel(ctx, servingTarget, servingModel)
	if confirmed {
		return true, false, ""
	}
	if strings.TrimSpace(adapterURI) == "" {
		return false, false, failureReason
	}
	if !r.multiTenant {
		return false, false, failureReason
	}
	if loadFailure := r.loadLoraAdapter(ctx, servingTarget, servingModel, adapterURI); loadFailure != "" {
		return false, true, loadFailure
	}
	confirmed, failureReason = r.confirmServingModel(ctx, servingTarget, servingModel)
	if !confirmed {
		return false, false, failureReason
	}
	return true, false, ""
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

func (r *VLLMRuntime) upsertDeployment(ctx context.Context, servedModel *model.ServedModel, workloadName string, servingModel string) error {
	log.Trace("VLLMRuntime upsertDeployment")

	resource := r.client.Resource(deploymentGVR).Namespace(r.namespace)
	desired := r.deploymentObject(servedModel, workloadName, servingModel)
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

func (r *VLLMRuntime) deploymentObject(servedModel *model.ServedModel, workloadName string, servingModel string) *unstructured.Unstructured {
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
				"spec": r.podSpec(servedModel, servingModel),
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

func (r *VLLMRuntime) podSpec(servedModel *model.ServedModel, servingModel string) map[string]any {
	log.Trace("VLLMRuntime podSpec")

	args := []any{
		"--model", strings.TrimSpace(servedModel.BaseModel),
		"--served-model-name", servingModel,
	}
	if strings.TrimSpace(servedModel.AdapterURI) != "" {
		args = append(args, "--enable-lora")
	}
	if strings.TrimSpace(servedModel.AdapterURI) != "" && !r.multiTenant {
		args = append(args, "--lora-modules", fmt.Sprintf("%s=%s", servingModel, strings.TrimSpace(servedModel.AdapterURI)))
	}
	spec := map[string]any{
		"containers": []any{
			map[string]any{
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
			},
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

func withDefaultString(value, fallback string) string {
	log.Trace("withDefaultString")

	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return strings.TrimSpace(value)
}
