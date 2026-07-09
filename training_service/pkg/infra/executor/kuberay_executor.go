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

	"training_service/pkg/app"
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

const (
	kubeRayAPIGroup                   = "ray.io"
	kubeRayAPIVersion                 = "v1"
	kubeRayRayJobResource             = "rayjobs"
	kubeRayRayJobAPIVersion           = "ray.io/v1"
	kubeRayRayJobKind                 = "RayJob"
	kubeRayObjectAPIVersion           = "apiVersion"
	kubeRayObjectKind                 = "kind"
	kubeRayObjectMetadata             = "metadata"
	kubeRayObjectSpec                 = "spec"
	kubeRayObjectStatus               = "status"
	kubeRayMetadataName               = "name"
	kubeRayMetadataNamespace          = "namespace"
	kubeRayMetadataLabels             = "labels"
	kubeRayLabelAppName               = "app.kubernetes.io/name"
	kubeRayLabelManagedBy             = "app.kubernetes.io/managed-by"
	kubeRayLabelSubmissionID          = "bighill.io/submission-id"
	kubeRayLabelTrainingJob           = "training-job"
	kubeRayManagedByTrainingService   = "training-service"
	kubeRaySpecEntrypoint             = "entrypoint"
	kubeRaySpecRuntimeEnvYAML         = "runtimeEnvYAML"
	kubeRaySpecShutdownAfterJob       = "shutdownAfterJobFinishes"
	kubeRaySpecRayCluster             = "rayClusterSpec"
	kubeRaySpecTTLSeconds             = "ttlSecondsAfterFinished"
	kubeRaySpecRayVersion             = "rayVersion"
	kubeRaySpecHeadGroup              = "headGroupSpec"
	kubeRaySpecWorkerGroups           = "workerGroupSpecs"
	kubeRaySpecRayStartParams         = "rayStartParams"
	kubeRaySpecTemplate               = "template"
	kubeRaySpecGroupName              = "groupName"
	kubeRaySpecReplicas               = "replicas"
	kubeRaySpecMinReplicas            = "minReplicas"
	kubeRaySpecMaxReplicas            = "maxReplicas"
	kubeRayWorkerGroupName            = "gpu-workers"
	kubeRayHeadContainerName          = "ray-head"
	kubeRayWorkerContainerName        = "ray-worker"
	kubeRayPodContainers              = "containers"
	kubeRayPodServiceAccountName      = "serviceAccountName"
	kubeRayPodNodeSelector            = "nodeSelector"
	kubeRayPodTolerations             = "tolerations"
	kubeRayContainerName              = "name"
	kubeRayContainerImage             = "image"
	kubeRayContainerImagePullPolicy   = "imagePullPolicy"
	kubeRayContainerResources         = "resources"
	kubeRayResourcesLimits            = "limits"
	kubeRayResourcesRequests          = "requests"
	kubeRayResourceCPU                = "cpu"
	kubeRayResourceMemory             = "memory"
	kubeRayNodeSelectorWorkload       = "workload"
	kubeRayNodeSelectorWorkloadGPU    = "gpu"
	kubeRayTolerationKey              = "key"
	kubeRayTolerationOperator         = "operator"
	kubeRayTolerationOperatorEqual    = "Equal"
	kubeRayTolerationValue            = "value"
	kubeRayTolerationValueTrue        = "true"
	kubeRayTolerationEffect           = "effect"
	kubeRayTolerationEffectNoSchedule = "NoSchedule"
	kubeRayGPUResourceName            = "nvidia.com/gpu"
	kubeRayStatusJobStatus            = "jobStatus"
	kubeRayStatusJobDeploymentStatus  = "jobDeploymentStatus"
	kubeRayStatusMessage              = "message"
	kubeRayStatusReason               = "reason"
	defaultKubeRayJobName             = "training-job"
	maxKubeRayNameLength              = 63
	kubeRayDeleteTimeout              = 30 * time.Second
)

type kubeRayJobStatusValue string

const (
	kubeRayJobStatusRunning   kubeRayJobStatusValue = "RUNNING"
	kubeRayJobStatusPending   kubeRayJobStatusValue = "PENDING"
	kubeRayJobStatusSucceeded kubeRayJobStatusValue = "SUCCEEDED"
	kubeRayJobStatusSuccess   kubeRayJobStatusValue = "SUCCESS"
	kubeRayJobStatusCompleted kubeRayJobStatusValue = "COMPLETED"
	kubeRayJobStatusComplete  kubeRayJobStatusValue = "COMPLETE"
	kubeRayJobStatusFailed    kubeRayJobStatusValue = "FAILED"
	kubeRayJobStatusFail      kubeRayJobStatusValue = "FAIL"
	kubeRayJobStatusStopped   kubeRayJobStatusValue = "STOPPED"
)

type kubeRayJobStatus struct {
	Status  kubeRayJobStatusValue
	Message string
}

type kubeRayTerminal int

const (
	kubeRayTerminalRunning kubeRayTerminal = iota
	kubeRayTerminalSucceeded
	kubeRayTerminalFailed
)

var rayJobGVR = schema.GroupVersionResource{Group: kubeRayAPIGroup, Version: kubeRayAPIVersion, Resource: kubeRayRayJobResource}

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
	manifestReader          app.ManifestReader
}

func NewKubeRayExecutor(config KubeRayExecutorConfig, manifestReader app.ManifestReader) (*KubeRayExecutor, error) {
	log.Trace("NewKubeRayExecutor")

	client, err := newKubeRayDynamicClient()
	if err != nil {
		return nil, err
	}
	return NewKubeRayExecutorWithClient(config, manifestReader, client)
}

func NewKubeRayExecutorWithClient(config KubeRayExecutorConfig, manifestReader app.ManifestReader, client dynamic.Interface) (*KubeRayExecutor, error) {
	log.Trace("NewKubeRayExecutorWithClient")

	if manifestReader == nil {
		return nil, domain.ErrValidationFailed.Extend("kuberay executor manifest reader is required")
	}
	if client == nil {
		return nil, domain.ErrValidationFailed.Extend("kuberay dynamic client is required")
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return &KubeRayExecutor{
		namespace:               strings.TrimSpace(config.Namespace),
		rayVersion:              strings.TrimSpace(config.RayVersion),
		image:                   strings.TrimSpace(config.Image),
		imagePullPolicy:         strings.TrimSpace(config.ImagePullPolicy),
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

func (config KubeRayExecutorConfig) Validate() error {
	log.Trace("KubeRayExecutorConfig Validate")

	if strings.TrimSpace(config.Namespace) == "" {
		return domain.ErrValidationFailed.Extend("kuberay namespace is required")
	}
	if strings.TrimSpace(config.RayVersion) == "" {
		return domain.ErrValidationFailed.Extend("kuberay ray version is required")
	}
	if strings.TrimSpace(config.Image) == "" {
		return domain.ErrValidationFailed.Extend("kuberay image is required")
	}
	if strings.TrimSpace(config.ImagePullPolicy) == "" {
		return domain.ErrValidationFailed.Extend("kuberay image pull policy is required")
	}
	if config.WorkerReplicas <= 0 {
		return domain.ErrValidationFailed.Extend("kuberay worker replicas must be greater than zero")
	}
	if strings.TrimSpace(config.CPU) == "" {
		return domain.ErrValidationFailed.Extend("kuberay cpu is required")
	}
	if strings.TrimSpace(config.Memory) == "" {
		return domain.ErrValidationFailed.Extend("kuberay memory is required")
	}
	if strings.TrimSpace(config.TrainingEntrypoint) == "" {
		return domain.ErrValidationFailed.Extend("kuberay training entrypoint is required")
	}
	if strings.TrimSpace(config.EvaluationEntrypoint) == "" {
		return domain.ErrValidationFailed.Extend("kuberay evaluation entrypoint is required")
	}
	if strings.TrimSpace(config.PromotionEntrypoint) == "" {
		return domain.ErrValidationFailed.Extend("kuberay promotion entrypoint is required")
	}
	if config.PollInterval <= 0 {
		return domain.ErrValidationFailed.Extend("kuberay poll interval must be greater than zero")
	}
	return nil
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
		return readPromotionReport(ctx, e.manifestReader, spec.ReportManifestURI, spec.ModelID, spec.OrgID)
	})
}

func waitForKubeRayJob[T any](ctx context.Context, executor *KubeRayExecutor, failureError *domain.ServiceError, submissionID, entrypoint string, envVars map[string]string, readResult func(context.Context) (*T, error)) (result *T, err error) {
	log.Trace("waitForKubeRayJob")

	name := KubeRayJobName(submissionID)
	defer func() {
		if err != nil && ctx.Err() != nil {
			deleteCtx, cancel := context.WithTimeout(context.Background(), kubeRayDeleteTimeout)
			defer cancel()
			if deleteErr := executor.deleteRayJob(deleteCtx, name); deleteErr != nil {
				log.WithContext(ctx).WithError(deleteErr).WithField("ray_job", name).Warn("failed to delete canceled kuberay job")
			}
		}
	}()
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
			return nil, failureError.Extend("kuberay job " + strings.ToLower(string(status.Status)) + ": " + status.Message)
		default:
			if status.Status != "" && status.Status != kubeRayJobStatusRunning && status.Status != kubeRayJobStatusPending {
				log.WithContext(ctx).WithField("ray_job", name).WithField("status", status.Status).Warn("unrecognized kuberay job status; continuing to poll")
			}
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

func (e *KubeRayExecutor) deleteRayJob(ctx context.Context, name string) error {
	log.Trace("KubeRayExecutor deleteRayJob")

	propagation := metav1.DeletePropagationBackground
	err := e.client.Resource(rayJobGVR).Namespace(e.namespace).Delete(ctx, name, metav1.DeleteOptions{
		PropagationPolicy: &propagation,
	})
	if apierrors.IsNotFound(err) {
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
	status, _, _ := unstructured.NestedString(obj.Object, kubeRayObjectStatus, kubeRayStatusJobStatus)
	if status == "" {
		status, _, _ = unstructured.NestedString(obj.Object, kubeRayObjectStatus, kubeRayStatusJobDeploymentStatus)
	}
	message, _, _ := unstructured.NestedString(obj.Object, kubeRayObjectStatus, kubeRayStatusMessage)
	if message == "" {
		message, _, _ = unstructured.NestedString(obj.Object, kubeRayObjectStatus, kubeRayStatusReason)
	}
	return kubeRayJobStatus{Status: kubeRayJobStatusValueFromString(status), Message: message}, true, nil
}

func kubeRayJobStatusValueFromString(status string) kubeRayJobStatusValue {
	log.Trace("kubeRayJobStatusValueFromString")

	return kubeRayJobStatusValue(strings.ToUpper(strings.TrimSpace(status)))
}

func kubeRayTerminalStatus(status kubeRayJobStatusValue) kubeRayTerminal {
	log.Trace("kubeRayTerminalStatus")

	switch status {
	case kubeRayJobStatusSucceeded, kubeRayJobStatusSuccess, kubeRayJobStatusCompleted, kubeRayJobStatusComplete:
		return kubeRayTerminalSucceeded
	case kubeRayJobStatusFailed, kubeRayJobStatusFail, kubeRayJobStatusStopped:
		return kubeRayTerminalFailed
	default:
		return kubeRayTerminalRunning
	}
}

func (e *KubeRayExecutor) rayJobObject(name, submissionID, entrypoint string, envVars map[string]string) *unstructured.Unstructured {
	log.Trace("KubeRayExecutor rayJobObject")

	spec := map[string]any{
		kubeRaySpecEntrypoint:       entrypoint,
		kubeRaySpecRuntimeEnvYAML:   runtimeEnvYAML(envVars),
		kubeRaySpecShutdownAfterJob: true,
		kubeRaySpecRayCluster:       e.rayClusterSpec(),
	}
	if e.ttlSecondsAfterFinished > 0 {
		spec[kubeRaySpecTTLSeconds] = int64(e.ttlSecondsAfterFinished)
	}
	return &unstructured.Unstructured{Object: map[string]any{
		kubeRayObjectAPIVersion: kubeRayRayJobAPIVersion,
		kubeRayObjectKind:       kubeRayRayJobKind,
		kubeRayObjectMetadata: map[string]any{
			kubeRayMetadataName:      name,
			kubeRayMetadataNamespace: e.namespace,
			kubeRayMetadataLabels: map[string]any{
				kubeRayLabelAppName:      kubeRayLabelTrainingJob,
				kubeRayLabelManagedBy:    kubeRayManagedByTrainingService,
				kubeRayLabelSubmissionID: name,
			},
		},
		kubeRayObjectSpec: spec,
	}}
}

func (e *KubeRayExecutor) rayClusterSpec() map[string]any {
	log.Trace("KubeRayExecutor rayClusterSpec")

	return map[string]any{
		kubeRaySpecRayVersion: e.rayVersion,
		kubeRaySpecHeadGroup: map[string]any{
			kubeRaySpecRayStartParams: map[string]any{},
			kubeRaySpecTemplate: map[string]any{
				kubeRayObjectSpec: e.podSpec(kubeRayHeadContainerName, false),
			},
		},
		kubeRaySpecWorkerGroups: []any{
			map[string]any{
				kubeRaySpecGroupName:      kubeRayWorkerGroupName,
				kubeRaySpecReplicas:       int64(e.workerReplicas),
				kubeRaySpecMinReplicas:    int64(e.workerReplicas),
				kubeRaySpecMaxReplicas:    int64(e.workerReplicas),
				kubeRaySpecRayStartParams: map[string]any{},
				kubeRaySpecTemplate: map[string]any{
					kubeRayObjectSpec: e.podSpec(kubeRayWorkerContainerName, true),
				},
			},
		},
	}
}

func (e *KubeRayExecutor) podSpec(containerName string, includeGPU bool) map[string]any {
	log.Trace("KubeRayExecutor podSpec")

	spec := map[string]any{
		kubeRayPodContainers: []any{
			map[string]any{
				kubeRayContainerName:            containerName,
				kubeRayContainerImage:           e.image,
				kubeRayContainerImagePullPolicy: e.imagePullPolicy,
				kubeRayContainerResources:       e.resources(includeGPU),
			},
		},
	}
	if e.serviceAccountName != "" {
		spec[kubeRayPodServiceAccountName] = e.serviceAccountName
	}
	if includeGPU && e.gpuResource != "" && e.gpu != "" {
		spec[kubeRayPodNodeSelector] = map[string]any{kubeRayNodeSelectorWorkload: kubeRayNodeSelectorWorkloadGPU}
		spec[kubeRayPodTolerations] = []any{
			map[string]any{
				kubeRayTolerationKey:      kubeRayGPUResourceName,
				kubeRayTolerationOperator: kubeRayTolerationOperatorEqual,
				kubeRayTolerationValue:    kubeRayTolerationValueTrue,
				kubeRayTolerationEffect:   kubeRayTolerationEffectNoSchedule,
			},
		}
	}
	return spec
}

func (e *KubeRayExecutor) resources(includeGPU bool) map[string]any {
	log.Trace("KubeRayExecutor resources")

	limits := map[string]any{kubeRayResourceCPU: e.cpu, kubeRayResourceMemory: e.memory}
	requests := map[string]any{kubeRayResourceCPU: e.cpu, kubeRayResourceMemory: e.memory}
	if includeGPU && e.gpuResource != "" && e.gpu != "" {
		limits[e.gpuResource] = e.gpu
		requests[e.gpuResource] = e.gpu
	}
	return map[string]any{
		kubeRayResourcesLimits:   limits,
		kubeRayResourcesRequests: requests,
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

func KubeRayJobName(submissionID string) string {
	log.Trace("KubeRayJobName")

	name := strings.ToLower(submissionID)
	name = invalidKubeNameChars.ReplaceAllString(name, "-")
	name = strings.Trim(name, "-")
	if name == "" {
		name = defaultKubeRayJobName
	}
	if len(name) <= maxKubeRayNameLength {
		return name
	}
	sum := sha1.Sum([]byte(name))
	suffix := hex.EncodeToString(sum[:])[:10]
	prefix := strings.Trim(name[:maxKubeRayNameLength-len(suffix)-1], "-")
	if prefix == "" {
		prefix = defaultKubeRayJobName
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

var invalidKubeNameChars = regexp.MustCompile(`[^a-z0-9-]+`)
