package executor

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"training_service/pkg/domain"
	"training_service/pkg/domain/model"

	log "github.com/sirupsen/logrus"
	"go.temporal.io/sdk/activity"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type KubeRayExecutorConfig struct {
	Namespace               string
	RayVersion              string
	Image                   string
	ImagePullPolicy         string
	ServiceAccountName      string
	TTLSecondsAfterFinished int
	WorkerReplicas          int
	CPU                     string
	Memory                  string
	GPUResource             string
	GPU                     string
	TrainingEntrypoint      string
	EvaluationEntrypoint    string
	PromotionEntrypoint     string
	PollInterval            time.Duration
}

type KubeRayExecutor struct {
	namespace               string
	rayVersion              string
	image                   string
	imagePullPolicy         string
	serviceAccountName      string
	ttlSecondsAfterFinished int
	workerReplicas          int
	cpu                     string
	memory                  string
	gpuResource             string
	gpu                     string
	trainingEntrypoint      string
	evaluationEntrypoint    string
	promotionEntrypoint     string
	pollInterval            time.Duration
	client                  dynamic.Interface
	manifestReader          ManifestReader
}

var rayJobGVR = schema.GroupVersionResource{Group: "ray.io", Version: "v1", Resource: "rayjobs"}

func NewKubeRayExecutor(config KubeRayExecutorConfig, manifestReader ManifestReader) (*KubeRayExecutor, error) {
	log.Trace("NewKubeRayExecutor")

	client, err := newKubeRayDynamicClient()
	if err != nil {
		return nil, err
	}
	return NewKubeRayExecutorWithClient(config, manifestReader, client)
}

func NewKubeRayExecutorWithClient(config KubeRayExecutorConfig, manifestReader ManifestReader, client dynamic.Interface) (*KubeRayExecutor, error) {
	log.Trace("NewKubeRayExecutorWithClient")

	if strings.TrimSpace(config.Namespace) == "" {
		return nil, domain.ErrValidationFailed.Extend("kuberay namespace is required")
	}
	if strings.TrimSpace(config.RayVersion) == "" {
		return nil, domain.ErrValidationFailed.Extend("kuberay ray version is required")
	}
	if strings.TrimSpace(config.Image) == "" {
		return nil, domain.ErrValidationFailed.Extend("kuberay job image is required")
	}
	if strings.TrimSpace(config.TrainingEntrypoint) == "" {
		return nil, domain.ErrValidationFailed.Extend("kuberay training entrypoint is required")
	}
	if strings.TrimSpace(config.EvaluationEntrypoint) == "" {
		return nil, domain.ErrValidationFailed.Extend("kuberay evaluation entrypoint is required")
	}
	if strings.TrimSpace(config.PromotionEntrypoint) == "" {
		return nil, domain.ErrValidationFailed.Extend("kuberay promotion entrypoint is required")
	}
	if config.PollInterval <= 0 {
		return nil, domain.ErrValidationFailed.Extend("kuberay poll interval is required")
	}
	if config.WorkerReplicas <= 0 {
		return nil, domain.ErrValidationFailed.Extend("kuberay worker replicas must be greater than zero")
	}
	if strings.TrimSpace(config.CPU) == "" || strings.TrimSpace(config.Memory) == "" {
		return nil, domain.ErrValidationFailed.Extend("kuberay cpu and memory are required")
	}
	if manifestReader == nil {
		return nil, domain.ErrValidationFailed.Extend("artifact manifest reader is required")
	}
	if client == nil {
		return nil, domain.ErrValidationFailed.Extend("kuberay client is required")
	}
	return &KubeRayExecutor{
		namespace:               strings.TrimSpace(config.Namespace),
		rayVersion:              strings.TrimSpace(config.RayVersion),
		image:                   strings.TrimSpace(config.Image),
		imagePullPolicy:         withDefaultString(config.ImagePullPolicy, "IfNotPresent"),
		serviceAccountName:      strings.TrimSpace(config.ServiceAccountName),
		ttlSecondsAfterFinished: config.TTLSecondsAfterFinished,
		workerReplicas:          config.WorkerReplicas,
		cpu:                     strings.TrimSpace(config.CPU),
		memory:                  strings.TrimSpace(config.Memory),
		gpuResource:             strings.TrimSpace(config.GPUResource),
		gpu:                     strings.TrimSpace(config.GPU),
		trainingEntrypoint:      strings.TrimSpace(config.TrainingEntrypoint),
		evaluationEntrypoint:    strings.TrimSpace(config.EvaluationEntrypoint),
		promotionEntrypoint:     strings.TrimSpace(config.PromotionEntrypoint),
		pollInterval:            config.PollInterval,
		client:                  client,
		manifestReader:          manifestReader,
	}, nil
}

func (e *KubeRayExecutor) RunTrainingJob(ctx context.Context, spec model.TrainingJobSpec) (*model.TrainedModelArtifact, error) {
	log.Trace("KubeRayExecutor RunTrainingJob")

	return waitForKubeRayJob(ctx, e, domain.ErrTrainModel, spec.SubmissionID, e.trainingEntrypoint, trainingEnv(spec), func(ctx context.Context) (*model.TrainedModelArtifact, error) {
		return readTrainingArtifact(ctx, e.manifestReader, spec.ArtifactManifestURI, spec.TrainingRunID)
	})
}

func (e *KubeRayExecutor) EvaluateModel(ctx context.Context, spec model.EvaluationJobSpec) (*model.EvaluationReport, error) {
	log.Trace("KubeRayExecutor EvaluateModel")

	return waitForKubeRayJob(ctx, e, domain.ErrEvaluateModel, spec.SubmissionID, e.evaluationEntrypoint, evaluationEnv(spec), func(ctx context.Context) (*model.EvaluationReport, error) {
		return readEvaluationReport(ctx, e.manifestReader, spec.ReportManifestURI, spec.TrainingRunID)
	})
}

func (e *KubeRayExecutor) RunPromotionReport(ctx context.Context, spec model.PromotionReportJobSpec) (*model.PromotionReport, error) {
	log.Trace("KubeRayExecutor RunPromotionReport")

	entrypoint, err := promotionReportEntrypoint(e.promotionEntrypoint, spec)
	if err != nil {
		return nil, err
	}
	return waitForKubeRayJob(ctx, e, domain.ErrEvaluateModel, spec.SubmissionID, entrypoint, promotionReportEnv(spec), func(ctx context.Context) (*model.PromotionReport, error) {
		return readPromotionReport(ctx, e.manifestReader, spec.ReportManifestURI, spec.ModelID)
	})
}

func waitForKubeRayJob[T any](ctx context.Context, executor *KubeRayExecutor, failureError *domain.ServiceError, submissionID, entrypoint string, envVars map[string]string, readResult func(context.Context) (*T, error)) (*T, error) {
	log.Trace("waitForKubeRayJob")

	name := KubeRayJobName(submissionID)
	if err := executor.ensureRayJob(ctx, name, submissionID, entrypoint, envVars); err != nil {
		return nil, err
	}
	missingAfterCreate := 0
	for {
		status, found, err := executor.jobStatus(ctx, name)
		if err != nil {
			return nil, err
		}
		if !found {
			if result, resultErr := readResult(ctx); resultErr == nil {
				return result, nil
			}
			if missingAfterCreate < rayJobRegistrationAttempts {
				missingAfterCreate++
				recordKubeRayHeartbeat(ctx, name)
				if err := sleepContext(ctx, executor.pollInterval); err != nil {
					return nil, err
				}
				continue
			}
			return nil, failureError.Extend("kuberay job disappeared after create")
		}
		switch kubeRayTerminalStatus(status.Status) {
		case kubeRayTerminalSucceeded:
			return readResult(ctx)
		case kubeRayTerminalFailed:
			return nil, failureError.Extend("kuberay job " + strings.ToLower(status.Status) + ": " + status.Message)
		default:
			recordKubeRayHeartbeat(ctx, name)
			if err := sleepContext(ctx, executor.pollInterval); err != nil {
				return nil, err
			}
		}
	}
}

func (e *KubeRayExecutor) ensureRayJob(ctx context.Context, name, submissionID, entrypoint string, envVars map[string]string) error {
	log.Trace("KubeRayExecutor ensureRayJob")

	obj := e.rayJobObject(name, submissionID, entrypoint, envVars)
	_, err := e.client.Resource(rayJobGVR).Namespace(e.namespace).Create(ctx, obj, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func (e *KubeRayExecutor) jobStatus(ctx context.Context, name string) (kubeRayJobStatus, bool, error) {
	log.Trace("KubeRayExecutor jobStatus")

	obj, err := e.client.Resource(rayJobGVR).Namespace(e.namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return kubeRayJobStatus{}, false, nil
	}
	if err != nil {
		return kubeRayJobStatus{}, false, err
	}
	status, _, _ := unstructured.NestedString(obj.Object, "status", "jobStatus")
	if status == "" {
		status, _, _ = unstructured.NestedString(obj.Object, "status", "jobDeploymentStatus")
	}
	message, _, _ := unstructured.NestedString(obj.Object, "status", "message")
	if message == "" {
		message, _, _ = unstructured.NestedString(obj.Object, "status", "reason")
	}
	return kubeRayJobStatus{Status: status, Message: message}, true, nil
}

func kubeRayTerminalStatus(status string) kubeRayTerminal {
	log.Trace("kubeRayTerminalStatus")

	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "SUCCEEDED", "SUCCESS", "COMPLETED", "COMPLETE":
		return kubeRayTerminalSucceeded
	case "FAILED", "FAIL", "STOPPED":
		return kubeRayTerminalFailed
	default:
		return kubeRayTerminalRunning
	}
}

func (e *KubeRayExecutor) rayJobObject(name, submissionID, entrypoint string, envVars map[string]string) *unstructured.Unstructured {
	log.Trace("KubeRayExecutor rayJobObject")

	spec := map[string]any{
		"entrypoint":               entrypoint,
		"runtimeEnvYAML":           runtimeEnvYAML(envVars),
		"shutdownAfterJobFinishes": true,
		"rayClusterSpec":           e.rayClusterSpec(),
	}
	if e.ttlSecondsAfterFinished > 0 {
		spec["ttlSecondsAfterFinished"] = int64(e.ttlSecondsAfterFinished)
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "ray.io/v1",
		"kind":       "RayJob",
		"metadata": map[string]any{
			"name":      name,
			"namespace": e.namespace,
			"labels": map[string]any{
				"app.kubernetes.io/name":       "training-job",
				"app.kubernetes.io/managed-by": "training-service",
				"bighill.io/submission-id":     name,
			},
		},
		"spec": spec,
	}}
}

func (e *KubeRayExecutor) rayClusterSpec() map[string]any {
	log.Trace("KubeRayExecutor rayClusterSpec")

	return map[string]any{
		"rayVersion": e.rayVersion,
		"headGroupSpec": map[string]any{
			"rayStartParams": map[string]any{},
			"template": map[string]any{
				"spec": e.podSpec("ray-head", false),
			},
		},
		"workerGroupSpecs": []any{
			map[string]any{
				"groupName":      "gpu-workers",
				"replicas":       int64(e.workerReplicas),
				"minReplicas":    int64(e.workerReplicas),
				"maxReplicas":    int64(e.workerReplicas),
				"rayStartParams": map[string]any{},
				"template": map[string]any{
					"spec": e.podSpec("ray-worker", true),
				},
			},
		},
	}
}

func (e *KubeRayExecutor) podSpec(containerName string, includeGPU bool) map[string]any {
	log.Trace("KubeRayExecutor podSpec")

	spec := map[string]any{
		"containers": []any{
			map[string]any{
				"name":            containerName,
				"image":           e.image,
				"imagePullPolicy": e.imagePullPolicy,
				"resources":       e.resources(includeGPU),
			},
		},
	}
	if e.serviceAccountName != "" {
		spec["serviceAccountName"] = e.serviceAccountName
	}
	return spec
}

func (e *KubeRayExecutor) resources(includeGPU bool) map[string]any {
	log.Trace("KubeRayExecutor resources")

	limits := map[string]any{"cpu": e.cpu, "memory": e.memory}
	requests := map[string]any{"cpu": e.cpu, "memory": e.memory}
	if includeGPU && e.gpuResource != "" && e.gpu != "" {
		limits[e.gpuResource] = e.gpu
		requests[e.gpuResource] = e.gpu
	}
	return map[string]any{
		"limits":   limits,
		"requests": requests,
	}
}

func runtimeEnvYAML(envVars map[string]string) string {
	log.Trace("runtimeEnvYAML")

	keys := make([]string, 0, len(envVars))
	for key := range envVars {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var out strings.Builder
	out.WriteString("env_vars:\n")
	for _, key := range keys {
		out.WriteString("  ")
		out.WriteString(key)
		out.WriteString(": ")
		out.WriteString(strconv.Quote(envVars[key]))
		out.WriteString("\n")
	}
	return out.String()
}

func recordKubeRayHeartbeat(ctx context.Context, jobName string) {
	log.Trace("recordKubeRayHeartbeat")

	if activity.IsActivity(ctx) {
		activity.RecordHeartbeat(ctx, jobName)
	}
}

type kubeRayJobStatus struct {
	Status  string
	Message string
}

type kubeRayTerminal int

const (
	kubeRayTerminalRunning kubeRayTerminal = iota
	kubeRayTerminalSucceeded
	kubeRayTerminalFailed
)

func KubeRayJobName(submissionID string) string {
	log.Trace("KubeRayJobName")

	name := strings.ToLower(submissionID)
	name = invalidKubeNameChars.ReplaceAllString(name, "-")
	name = strings.Trim(name, "-")
	if name == "" {
		name = "training-job"
	}
	if len(name) <= maxKubeRayNameLength {
		return name
	}
	sum := sha1.Sum([]byte(name))
	suffix := hex.EncodeToString(sum[:])[:10]
	prefix := strings.Trim(name[:maxKubeRayNameLength-len(suffix)-1], "-")
	if prefix == "" {
		prefix = "training-job"
	}
	return prefix + "-" + suffix
}

func newKubeRayDynamicClient() (dynamic.Interface, error) {
	log.Trace("newKubeRayDynamicClient")

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
			return nil, fmt.Errorf("%w: create kuberay client config: %w", domain.ErrTrainModel, err)
		}
	}
	client, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("%w: create kuberay client: %w", domain.ErrTrainModel, err)
	}
	return client, nil
}

func withDefaultString(value, fallback string) string {
	log.Trace("withDefaultString")

	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return strings.TrimSpace(value)
}

var invalidKubeNameChars = regexp.MustCompile(`[^a-z0-9-]+`)

const maxKubeRayNameLength = 63
