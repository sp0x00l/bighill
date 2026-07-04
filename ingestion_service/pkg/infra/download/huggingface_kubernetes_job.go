package download

import (
	"context"
	"encoding/json"
	"fmt"
	"ingestion_service/pkg/domain"
	"ingestion_service/pkg/domain/model"
	corebucket "lib/shared_lib/bucket"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

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

type HuggingFaceKubernetesJobDownloaderConfig struct {
	Namespace               string
	Image                   string
	ImagePullPolicy         string
	ServiceAccountName      string
	Command                 string
	OutputURI               string
	TTLSecondsAfterFinished int
	BackoffLimit            int
	CPU                     string
	Memory                  string
	PollInterval            time.Duration
	Timeout                 time.Duration
	EnvKeys                 HuggingFaceJobEnvKeys
}

type HuggingFaceKubernetesJobDownloader struct {
	namespace               string
	image                   string
	imagePullPolicy         string
	serviceAccountName      string
	command                 []string
	outputURI               string
	ttlSecondsAfterFinished int
	backoffLimit            int
	cpu                     string
	memory                  string
	pollInterval            time.Duration
	timeout                 time.Duration
	envKeys                 HuggingFaceJobEnvKeys
	client                  dynamic.Interface
	manifestReader          modelManifestReader
}

type modelManifestReader interface {
	ReadManifest(context.Context, string) ([]byte, error)
}

var kubernetesJobGVR = schema.GroupVersionResource{Group: "batch", Version: "v1", Resource: "jobs"}

func NewHuggingFaceKubernetesJobDownloader(config HuggingFaceKubernetesJobDownloaderConfig, manifestReader modelManifestReader) (*HuggingFaceKubernetesJobDownloader, error) {
	log.Trace("NewHuggingFaceKubernetesJobDownloader")

	client, err := newDynamicClient()
	if err != nil {
		return nil, err
	}
	return NewHuggingFaceKubernetesJobDownloaderWithClient(config, manifestReader, client)
}

func NewHuggingFaceKubernetesJobDownloaderWithClient(config HuggingFaceKubernetesJobDownloaderConfig, manifestReader modelManifestReader, client dynamic.Interface) (*HuggingFaceKubernetesJobDownloader, error) {
	log.Trace("NewHuggingFaceKubernetesJobDownloaderWithClient")

	return &HuggingFaceKubernetesJobDownloader{
		namespace:               strings.TrimSpace(config.Namespace),
		image:                   strings.TrimSpace(config.Image),
		imagePullPolicy:         strings.TrimSpace(config.ImagePullPolicy),
		serviceAccountName:      strings.TrimSpace(config.ServiceAccountName),
		command:                 strings.Fields(config.Command),
		outputURI:               strings.TrimRight(strings.TrimSpace(config.OutputURI), "/"),
		ttlSecondsAfterFinished: config.TTLSecondsAfterFinished,
		backoffLimit:            config.BackoffLimit,
		cpu:                     strings.TrimSpace(config.CPU),
		memory:                  strings.TrimSpace(config.Memory),
		pollInterval:            config.PollInterval,
		timeout:                 config.Timeout,
		envKeys:                 config.EnvKeys.trimmed(),
		client:                  client,
		manifestReader:          manifestReader,
	}, nil
}

func (d *HuggingFaceKubernetesJobDownloader) DownloadHuggingFaceModel(ctx context.Context, request model.OnboardHuggingFaceModelRequest) (*model.OnboardedModelArtifact, error) {
	log.Trace("HuggingFaceKubernetesJobDownloader DownloadHuggingFaceModel")

	runCtx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()
	name := HuggingFaceJobName(request.ResourceID)
	manifestURI := d.manifestURI(request.ResourceID)
	if err := d.ensureJob(runCtx, name, request); err != nil {
		return nil, fmt.Errorf("%w: create hugging face download job: %w", domain.ErrValidationFailed, err)
	}
	for {
		status, err := d.jobStatus(runCtx, name)
		if err != nil {
			return nil, fmt.Errorf("%w: read hugging face download job status: %w", domain.ErrValidationFailed, err)
		}
		if status.succeeded {
			return d.readManifest(runCtx, request, manifestURI)
		}
		if status.failed {
			return nil, fmt.Errorf("%w: hugging face download job failed: %s", domain.ErrValidationFailed, status.message)
		}
		if err := sleepContext(runCtx, d.pollInterval); err != nil {
			return nil, fmt.Errorf("%w: wait for hugging face download job: %w", domain.ErrValidationFailed, err)
		}
	}
}

func (d *HuggingFaceKubernetesJobDownloader) ensureJob(ctx context.Context, name string, request model.OnboardHuggingFaceModelRequest) error {
	log.Trace("HuggingFaceKubernetesJobDownloader ensureJob")

	_, err := d.client.Resource(kubernetesJobGVR).Namespace(d.namespace).Create(ctx, d.jobObject(name, request), metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func (d *HuggingFaceKubernetesJobDownloader) jobObject(name string, request model.OnboardHuggingFaceModelRequest) *unstructured.Unstructured {
	log.Trace("HuggingFaceKubernetesJobDownloader jobObject")

	envValues := d.envKeys.envValues(request, request.HuggingFaceToken, d.outputURI)
	env := make([]any, 0, len(envValues))
	for key, value := range envValues {
		env = append(env, map[string]any{"name": key, "value": value})
	}
	container := map[string]any{
		"name":            "huggingface-download",
		"image":           d.image,
		"imagePullPolicy": d.imagePullPolicy,
		"command":         stringSliceToAny(d.command),
		"env":             env,
	}
	if d.cpu != "" || d.memory != "" {
		resources := map[string]any{}
		requests := map[string]any{}
		limits := map[string]any{}
		if d.cpu != "" {
			requests["cpu"] = d.cpu
			limits["cpu"] = d.cpu
		}
		if d.memory != "" {
			requests["memory"] = d.memory
			limits["memory"] = d.memory
		}
		resources["requests"] = requests
		resources["limits"] = limits
		container["resources"] = resources
	}
	podSpec := map[string]any{
		"restartPolicy": "Never",
		"containers":    []any{container},
	}
	if d.serviceAccountName != "" {
		podSpec["serviceAccountName"] = d.serviceAccountName
	}
	spec := map[string]any{
		"backoffLimit": int64(d.backoffLimit),
		"template": map[string]any{
			"metadata": map[string]any{
				"labels": map[string]any{
					"app":                 "huggingface-model-onboard",
					"bighill.io/model-id": request.ResourceID.String(),
				},
			},
			"spec": podSpec,
		},
	}
	if d.ttlSecondsAfterFinished > 0 {
		spec["ttlSecondsAfterFinished"] = int64(d.ttlSecondsAfterFinished)
	}
	return &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "batch/v1",
			"kind":       "Job",
			"metadata": map[string]any{
				"name":      name,
				"namespace": d.namespace,
				"labels": map[string]any{
					"app":                 "huggingface-model-onboard",
					"bighill.io/model-id": request.ResourceID.String(),
				},
			},
			"spec": spec,
		},
	}
}

func (d *HuggingFaceKubernetesJobDownloader) jobStatus(ctx context.Context, name string) (huggingFaceJobStatus, error) {
	log.Trace("HuggingFaceKubernetesJobDownloader jobStatus")

	obj, err := d.client.Resource(kubernetesJobGVR).Namespace(d.namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return huggingFaceJobStatus{}, err
	}
	if succeeded, _, _ := unstructured.NestedInt64(obj.Object, "status", "succeeded"); succeeded > 0 {
		return huggingFaceJobStatus{succeeded: true}, nil
	}
	if failed, _, _ := unstructured.NestedInt64(obj.Object, "status", "failed"); failed > 0 {
		return huggingFaceJobStatus{failed: true, message: jobConditionMessage(obj, "Failed")}, nil
	}
	if conditionStatus(obj, "Complete") {
		return huggingFaceJobStatus{succeeded: true}, nil
	}
	if conditionStatus(obj, "Failed") {
		return huggingFaceJobStatus{failed: true, message: jobConditionMessage(obj, "Failed")}, nil
	}
	return huggingFaceJobStatus{}, nil
}

func (d *HuggingFaceKubernetesJobDownloader) manifestURI(resourceID uuid.UUID) string {
	log.Trace("HuggingFaceKubernetesJobDownloader manifestURI")

	return d.outputURI + "/" + resourceID.String() + "/manifest.json"
}

func (d *HuggingFaceKubernetesJobDownloader) readManifest(ctx context.Context, request model.OnboardHuggingFaceModelRequest, manifestURI string) (*model.OnboardedModelArtifact, error) {
	log.Trace("HuggingFaceKubernetesJobDownloader readManifest")

	data, err := d.manifestReader.ReadManifest(ctx, manifestURI)
	if err != nil {
		return nil, fmt.Errorf("%w: read hugging face manifest: %w", domain.ErrValidationFailed, err)
	}
	var result huggingFaceDownloadResult
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("%w: parse hugging face manifest: %w", domain.ErrValidationFailed, err)
	}
	return downloadResultToArtifact(request, result)
}

type huggingFaceJobStatus struct {
	succeeded bool
	failed    bool
	message   string
}

func conditionStatus(obj *unstructured.Unstructured, conditionType string) bool {
	log.Trace("conditionStatus")

	conditions, _, _ := unstructured.NestedSlice(obj.Object, "status", "conditions")
	for _, item := range conditions {
		condition, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if strings.EqualFold(fmt.Sprint(condition["type"]), conditionType) && strings.EqualFold(fmt.Sprint(condition["status"]), "True") {
			return true
		}
	}
	return false
}

func jobConditionMessage(obj *unstructured.Unstructured, conditionType string) string {
	log.Trace("jobConditionMessage")

	conditions, _, _ := unstructured.NestedSlice(obj.Object, "status", "conditions")
	for _, item := range conditions {
		condition, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if strings.EqualFold(fmt.Sprint(condition["type"]), conditionType) {
			return strings.TrimSpace(fmt.Sprint(condition["message"]))
		}
	}
	return ""
}

func HuggingFaceJobName(resourceID uuid.UUID) string {
	log.Trace("HuggingFaceJobName")

	name := "hf-model-" + resourceID.String()
	name = invalidKubernetesNameChars.ReplaceAllString(strings.ToLower(name), "-")
	name = strings.Trim(name, "-")
	if len(name) <= 63 {
		return name
	}
	return strings.Trim(name[:63], "-")
}

func newDynamicClient() (dynamic.Interface, error) {
	log.Trace("newDynamicClient")

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
			return nil, fmt.Errorf("%w: create kubernetes client config: %w", domain.ErrValidationFailed, err)
		}
	}
	client, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("%w: create kubernetes client: %w", domain.ErrValidationFailed, err)
	}
	return client, nil
}

type ObjectModelManifestReader struct {
	bucket objectManifestBucket
}

type objectManifestBucket interface {
	HeadObject(ctx context.Context, bucket, key string) (*corebucket.ObjectInfo, error)
	ReadObjectRange(ctx context.Context, bucket, key string, offset, maxBytes int64) ([]byte, error)
}

func NewObjectModelManifestReader(bucket objectManifestBucket) *ObjectModelManifestReader {
	log.Trace("NewObjectModelManifestReader")

	return &ObjectModelManifestReader{bucket: bucket}
}

func (r *ObjectModelManifestReader) ReadManifest(ctx context.Context, location string) ([]byte, error) {
	log.Trace("ObjectModelManifestReader ReadManifest")

	parsed, err := url.Parse(strings.TrimSpace(location))
	if err != nil {
		return nil, fmt.Errorf("%w: parse manifest location: %w", domain.ErrValidationFailed, err)
	}
	if parsed.Scheme != "s3" {
		return nil, fmt.Errorf("%w: unsupported manifest scheme %q", domain.ErrValidationFailed, parsed.Scheme)
	}
	bucket := parsed.Host
	key := strings.TrimPrefix(parsed.Path, "/")
	info, err := r.bucket.HeadObject(ctx, bucket, key)
	if err != nil {
		return nil, fmt.Errorf("%w: head manifest object: %w", domain.ErrValidationFailed, err)
	}
	data, err := r.bucket.ReadObjectRange(ctx, bucket, key, 0, info.Size)
	if err != nil {
		return nil, fmt.Errorf("%w: read manifest object: %w", domain.ErrValidationFailed, err)
	}
	return data, nil
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

func stringSliceToAny(values []string) []any {
	log.Trace("stringSliceToAny")

	out := make([]any, 0, len(values))
	for _, value := range values {
		out = append(out, value)
	}
	return out
}

var invalidKubernetesNameChars = regexp.MustCompile(`[^a-z0-9-]+`)
