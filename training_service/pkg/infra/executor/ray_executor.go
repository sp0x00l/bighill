package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"training_service/pkg/domain"
	"training_service/pkg/domain/model"

	corebucket "lib/shared_lib/bucket"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	log "github.com/sirupsen/logrus"
	"go.temporal.io/sdk/activity"
)

type ManifestReader interface {
	Read(ctx context.Context, location string) ([]byte, error)
}

type RayExecutorConfig struct {
	URL                  string
	TrainingEntrypoint   string
	EvaluationEntrypoint string
	RequestTimeout       time.Duration
	PollInterval         time.Duration
}

type RayExecutor struct {
	url                  string
	trainingEntrypoint   string
	evaluationEntrypoint string
	pollInterval         time.Duration
	client               *http.Client
	manifestReader       ManifestReader
}

func NewRayExecutor(config RayExecutorConfig, manifestReader ManifestReader) (*RayExecutor, error) {
	log.Trace("NewRayExecutor")

	return NewRayExecutorWithClient(config, manifestReader, nil)
}

func NewRayExecutorWithClient(config RayExecutorConfig, manifestReader ManifestReader, client *http.Client) (*RayExecutor, error) {
	log.Trace("NewRayExecutorWithClient")

	rayURL := strings.TrimRight(strings.TrimSpace(config.URL), "/")
	if rayURL == "" {
		return nil, domain.ErrValidationFailed.Extend("ray jobs url is required")
	}
	if strings.TrimSpace(config.TrainingEntrypoint) == "" {
		return nil, domain.ErrValidationFailed.Extend("ray training entrypoint is required")
	}
	if strings.TrimSpace(config.EvaluationEntrypoint) == "" {
		return nil, domain.ErrValidationFailed.Extend("ray evaluation entrypoint is required")
	}
	if config.RequestTimeout <= 0 {
		return nil, domain.ErrValidationFailed.Extend("ray request timeout is required")
	}
	if config.PollInterval <= 0 {
		return nil, domain.ErrValidationFailed.Extend("ray poll interval is required")
	}
	if manifestReader == nil {
		return nil, domain.ErrValidationFailed.Extend("artifact manifest reader is required")
	}
	if client == nil {
		client = &http.Client{Timeout: config.RequestTimeout}
	}
	return &RayExecutor{
		url:                  rayURL,
		trainingEntrypoint:   strings.TrimSpace(config.TrainingEntrypoint),
		evaluationEntrypoint: strings.TrimSpace(config.EvaluationEntrypoint),
		pollInterval:         config.PollInterval,
		client:               client,
		manifestReader:       manifestReader,
	}, nil
}

func (e *RayExecutor) RunTrainingJob(ctx context.Context, spec model.TrainingJobSpec) (*model.TrainedModelArtifact, error) {
	log.Trace("RayExecutor RunTrainingJob")

	return waitForRayJob(ctx, e, domain.ErrTrainModel, spec.SubmissionID, e.trainingEntrypoint, trainingEnv(spec), func(ctx context.Context) (*model.TrainedModelArtifact, error) {
		return e.readTrainingArtifact(ctx, spec.ArtifactManifestURI, spec.TrainingRunID)
	})
}

func (e *RayExecutor) EvaluateModel(ctx context.Context, spec model.EvaluationJobSpec) (*model.EvaluationReport, error) {
	log.Trace("RayExecutor EvaluateModel")

	return waitForRayJob(ctx, e, domain.ErrEvaluateModel, spec.SubmissionID, e.evaluationEntrypoint, evaluationEnv(spec), func(ctx context.Context) (*model.EvaluationReport, error) {
		return e.readEvaluationReport(ctx, spec.ReportManifestURI, spec.TrainingRunID)
	})
}

func waitForRayJob[T any](ctx context.Context, executor *RayExecutor, failureError *domain.ServiceError, submissionID, entrypoint string, envVars map[string]string, readResult func(context.Context) (*T, error)) (*T, error) {
	log.Trace("waitForRayJob")

	status, found, err := executor.jobStatus(ctx, submissionID)
	if err != nil {
		return nil, err
	}
	if !found {
		if err := executor.submit(ctx, submissionID, entrypoint, envVars); err != nil {
			return nil, err
		}
	}

	for {
		status, found, err = executor.jobStatus(ctx, submissionID)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, failureError.Extend("ray job disappeared after submit")
		}
		switch status.Status {
		case "SUCCEEDED":
			return readResult(ctx)
		case "FAILED", "STOPPED":
			return nil, failureError.Extend("ray job " + strings.ToLower(status.Status) + ": " + status.Message)
		default:
			activity.RecordHeartbeat(ctx, submissionID)
			if err := sleepContext(ctx, executor.pollInterval); err != nil {
				return nil, err
			}
		}
	}
}

func (e *RayExecutor) submit(ctx context.Context, submissionID, entrypoint string, envVars map[string]string) error {
	log.Trace("RayExecutor submit")

	body, err := json.Marshal(raySubmitRequest{
		SubmissionID: submissionID,
		Entrypoint:   entrypoint,
		RuntimeEnv: rayRuntimeEnv{
			EnvVars: envVars,
		},
		Metadata: map[string]string{
			"submission_id": submissionID,
		},
	})
	if err != nil {
		return fmt.Errorf("%w: marshal ray submit request: %w", domain.ErrTrainModel, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.url+"/api/jobs/", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("%w: build ray submit request: %w", domain.ErrTrainModel, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := e.client.Do(req)
	if err != nil {
		return fmt.Errorf("%w: submit ray job: %w", domain.ErrTrainModel, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusConflict {
		return nil
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("%w: ray submit failed with status %d: %s", domain.ErrTrainModel, resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	return nil
}

func (e *RayExecutor) jobStatus(ctx context.Context, submissionID string) (rayJobStatusResponse, bool, error) {
	log.Trace("RayExecutor jobStatus")

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, e.url+"/api/jobs/"+url.PathEscape(submissionID), nil)
	if err != nil {
		return rayJobStatusResponse{}, false, fmt.Errorf("%w: build ray status request: %w", domain.ErrTrainModel, err)
	}
	resp, err := e.client.Do(req)
	if err != nil {
		return rayJobStatusResponse{}, false, fmt.Errorf("%w: get ray job status: %w", domain.ErrTrainModel, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return rayJobStatusResponse{}, false, nil
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return rayJobStatusResponse{}, false, fmt.Errorf("%w: ray status failed with status %d: %s", domain.ErrTrainModel, resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	var parsed rayJobStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return rayJobStatusResponse{}, false, fmt.Errorf("%w: decode ray job status: %w", domain.ErrTrainModel, err)
	}
	return parsed, true, nil
}

func (e *RayExecutor) readTrainingArtifact(ctx context.Context, location string, trainingRunID string) (*model.TrainedModelArtifact, error) {
	log.Trace("RayExecutor readTrainingArtifact")

	raw, err := e.manifestReader.Read(ctx, location)
	if err != nil {
		return nil, fmt.Errorf("%w: read training artifact manifest: %w", domain.ErrTrainModel, err)
	}
	var artifact model.TrainedModelArtifact
	if err := json.Unmarshal(raw, &artifact); err != nil {
		return nil, fmt.Errorf("%w: decode training artifact manifest: %w", domain.ErrTrainModel, err)
	}
	if strings.TrimSpace(artifact.TrainingRunID) != trainingRunID {
		return nil, domain.ErrTrainModel.Extend("training artifact manifest has mismatched training run id")
	}
	if strings.TrimSpace(artifact.ModelURI) == "" || strings.TrimSpace(artifact.ArtifactChecksum) == "" || artifact.ArtifactSizeBytes <= 0 {
		return nil, domain.ErrTrainModel.Extend("training artifact manifest is incomplete")
	}
	return &artifact, nil
}

func (e *RayExecutor) readEvaluationReport(ctx context.Context, location string, trainingRunID string) (*model.EvaluationReport, error) {
	log.Trace("RayExecutor readEvaluationReport")

	raw, err := e.manifestReader.Read(ctx, location)
	if err != nil {
		return nil, fmt.Errorf("%w: read evaluation report manifest: %w", domain.ErrEvaluateModel, err)
	}
	var report model.EvaluationReport
	if err := json.Unmarshal(raw, &report); err != nil {
		return nil, fmt.Errorf("%w: decode evaluation report manifest: %w", domain.ErrEvaluateModel, err)
	}
	if strings.TrimSpace(report.TrainingRunID) != trainingRunID {
		return nil, domain.ErrEvaluateModel.Extend("evaluation report manifest has mismatched training run id")
	}
	if strings.TrimSpace(report.ReportURI) == "" {
		return nil, domain.ErrEvaluateModel.Extend("evaluation report manifest is incomplete")
	}
	return &report, nil
}

func trainingEnv(spec model.TrainingJobSpec) map[string]string {
	log.Trace("trainingEnv")

	return map[string]string{
		"TRAINING_RUN_ID":                spec.TrainingRunID,
		"TRAINING_DATASET_URI":           spec.DatasetURI,
		"TRAINING_MODEL_NAME":            spec.ModelName,
		"TRAINING_MODEL_VERSION":         spec.ModelVersion,
		"TRAINING_BASE_MODEL":            spec.BaseModel,
		"TRAINING_MODEL_URI":             spec.ModelURI,
		"TRAINING_ARTIFACT_MANIFEST_URI": spec.ArtifactManifestURI,
		"TRAINING_RECIPE_YAML":           spec.RecipeYAML,
		"TRAINING_RECIPE_HASH":           spec.RecipeHash,
	}
}

func evaluationEnv(spec model.EvaluationJobSpec) map[string]string {
	log.Trace("evaluationEnv")

	return map[string]string{
		"TRAINING_RUN_ID":                  spec.TrainingRunID,
		"TRAINING_MODEL_URI":               spec.ModelURI,
		"TRAINING_EVALUATION_PROFILE":      spec.EvaluationProfile,
		"TRAINING_EVALUATION_REPORT_URI":   spec.ReportURI,
		"TRAINING_EVALUATION_MANIFEST_URI": spec.ReportManifestURI,
	}
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

type raySubmitRequest struct {
	SubmissionID string            `json:"submission_id"`
	Entrypoint   string            `json:"entrypoint"`
	RuntimeEnv   rayRuntimeEnv     `json:"runtime_env"`
	Metadata     map[string]string `json:"metadata"`
}

type rayRuntimeEnv struct {
	EnvVars map[string]string `json:"env_vars"`
}

type rayJobStatusResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

type ObjectManifestReader struct {
	region     string
	downloader s3Downloader
	client     *http.Client
}

type s3Downloader interface {
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

func NewObjectManifestReader(ctx context.Context, region string, client *http.Client) (*ObjectManifestReader, error) {
	log.Trace("NewObjectManifestReader")

	region = strings.TrimSpace(region)
	if region == "" {
		return nil, domain.ErrValidationFailed.Extend("artifact bucket region is required")
	}
	if client == nil {
		client = http.DefaultClient
	}
	reader := &ObjectManifestReader{
		region: region,
		client: client,
	}
	if region != corebucket.LocalDevS3Region {
		awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
		if err != nil {
			return nil, fmt.Errorf("load aws config: %w", err)
		}
		reader.downloader = s3.NewFromConfig(awsCfg)
	}
	return reader, nil
}

func (r *ObjectManifestReader) Read(ctx context.Context, location string) ([]byte, error) {
	log.Trace("ObjectManifestReader Read")

	parsed, err := url.Parse(strings.TrimSpace(location))
	if err != nil {
		return nil, fmt.Errorf("parse manifest location: %w", err)
	}
	switch parsed.Scheme {
	case "s3":
		return r.readS3(ctx, parsed.Host, strings.TrimPrefix(parsed.Path, "/"))
	case "http", "https":
		return r.readHTTP(ctx, location)
	case "file":
		return os.ReadFile(filepath.Clean(parsed.Path))
	case "":
		return os.ReadFile(filepath.Clean(location))
	default:
		return nil, fmt.Errorf("unsupported manifest scheme %q", parsed.Scheme)
	}
}

func (r *ObjectManifestReader) readS3(ctx context.Context, bucketName, key string) ([]byte, error) {
	log.Trace("ObjectManifestReader readS3")

	if r.region == corebucket.LocalDevS3Region {
		return os.ReadFile(filepath.Clean(filepath.Join(corebucket.StorageDir, bucketName, key)))
	}
	if r.downloader == nil {
		return nil, fmt.Errorf("s3 downloader is not configured")
	}
	output, err := r.downloader.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("get s3 object: %w", err)
	}
	defer output.Body.Close()
	return io.ReadAll(output.Body)
}

func (r *ObjectManifestReader) readHTTP(ctx context.Context, location string) ([]byte, error) {
	log.Trace("ObjectManifestReader readHTTP")

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, location, nil)
	if err != nil {
		return nil, err
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("manifest read failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	return io.ReadAll(resp.Body)
}
