package executor

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"training_service/pkg/app"
	"training_service/pkg/domain"
	"training_service/pkg/domain/model"

	corebucket "lib/shared_lib/bucket"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.temporal.io/sdk/activity"
)

const (
	rayJobRegistrationAttempts = 3
	rayStopTimeout             = 30 * time.Second
)

type rayJobStatusValue string

const (
	rayJobStatusSucceeded rayJobStatusValue = "SUCCEEDED"
	rayJobStatusFailed    rayJobStatusValue = "FAILED"
	rayJobStatusStopped   rayJobStatusValue = "STOPPED"
)

type rayTerminalStatus int

const (
	rayTerminalStatusRunning rayTerminalStatus = iota
	rayTerminalStatusSucceeded
	rayTerminalStatusFailed
)

type promotionReportJobRequest struct {
	UserID                   string `json:"user_id"`
	OrgID                    string `json:"org_id"`
	ModelID                  string `json:"model_id"`
	TrainingRunID            string `json:"training_run_id"`
	CandidateReportURI       string `json:"candidate_report_uri"`
	CandidateMetricsMetadata string `json:"candidate_metrics_metadata"`
	ChampionModelID          string `json:"champion_model_id"`
	ChampionReportURI        string `json:"champion_report_uri"`
	ChampionMetricsMetadata  string `json:"champion_metrics_metadata"`
	PromotionProfile         string `json:"promotion_profile"`
	ReportURI                string `json:"report_uri"`
	ReportManifestURI        string `json:"report_manifest_uri"`
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
	Status  rayJobStatusValue `json:"status"`
	Message string            `json:"message"`
}

type RayExecutorConfig struct {
	URL                  string
	TrainingEntrypoint   string
	EvaluationEntrypoint string
	PromotionEntrypoint  string
	RequestTimeout       time.Duration
	PollInterval         time.Duration
}

type RayExecutor struct {
	url                  string
	trainingEntrypoint   string
	evaluationEntrypoint string
	promotionEntrypoint  string
	pollInterval         time.Duration
	client               *http.Client
	manifestReader       app.ManifestReader
}

func NewRayExecutor(config RayExecutorConfig, manifestReader app.ManifestReader) (*RayExecutor, error) {
	log.Trace("NewRayExecutor")

	return NewRayExecutorWithClient(config, manifestReader, nil)
}

func NewRayExecutorWithClient(config RayExecutorConfig, manifestReader app.ManifestReader, client *http.Client) (*RayExecutor, error) {
	log.Trace("NewRayExecutorWithClient")

	if manifestReader == nil {
		panic("ray executor manifest reader is nil")
	}
	rayURL := strings.TrimRight(config.URL, "/")
	if client == nil {
		client = &http.Client{
			Timeout:   config.RequestTimeout,
			Transport: otelhttp.NewTransport(http.DefaultTransport),
		}
	}
	return &RayExecutor{
		url:                  rayURL,
		trainingEntrypoint:   config.TrainingEntrypoint,
		evaluationEntrypoint: config.EvaluationEntrypoint,
		promotionEntrypoint:  config.PromotionEntrypoint,
		pollInterval:         config.PollInterval,
		client:               client,
		manifestReader:       manifestReader,
	}, nil
}

func (e *RayExecutor) RunTrainingJob(ctx context.Context, spec model.TrainingJobSpec) (*model.TrainedModelArtifact, error) {
	log.Trace("RayExecutor RunTrainingJob")

	return waitForRayJob(ctx, e, domain.ErrTrainModel, spec.SubmissionID, e.trainingEntrypoint, trainingEnv(spec), func(ctx context.Context) (*model.TrainedModelArtifact, error) {
		return readTrainingArtifact(ctx, e.manifestReader, spec.ArtifactManifestURI, spec.TrainingRunID)
	})
}

func (e *RayExecutor) EvaluateModel(ctx context.Context, spec model.EvaluationJobSpec) (*model.EvaluationReport, error) {
	log.Trace("RayExecutor EvaluateModel")

	return waitForRayJob(ctx, e, domain.ErrEvaluateModel, spec.SubmissionID, e.evaluationEntrypoint, evaluationEnv(spec), func(ctx context.Context) (*model.EvaluationReport, error) {
		return readEvaluationReport(ctx, e.manifestReader, spec.ReportManifestURI, spec.TrainingRunID)
	})
}

func (e *RayExecutor) RunPromotionReport(ctx context.Context, spec model.PromotionReportJobSpec) (*model.PromotionReport, error) {
	log.Trace("RayExecutor RunPromotionReport")

	entrypoint, err := promotionReportEntrypoint(e.promotionEntrypoint, spec)
	if err != nil {
		return nil, err
	}
	return waitForRayJob(ctx, e, domain.ErrEvaluateModel, spec.SubmissionID, entrypoint, promotionReportEnv(spec), func(ctx context.Context) (*model.PromotionReport, error) {
		return readPromotionReport(ctx, e.manifestReader, spec.ReportManifestURI, spec.ModelID, spec.OrgID)
	})
}

func waitForRayJob[T any](ctx context.Context, executor *RayExecutor, failureError *domain.ServiceError, submissionID, entrypoint string, envVars map[string]string, readResult func(context.Context) (*T, error)) (result *T, err error) {
	log.Trace("waitForRayJob")

	defer func() {
		if err != nil && ctx.Err() != nil {
			stopCtx, cancel := context.WithTimeout(context.Background(), rayStopTimeout)
			defer cancel()
			if stopErr := executor.stop(stopCtx, submissionID); stopErr != nil {
				log.WithContext(ctx).WithError(stopErr).WithField("submission_id", submissionID).Warn("failed to stop canceled ray job")
			}
		}
	}()

	_, found, err := executor.jobStatus(ctx, submissionID)
	if err != nil {
		return nil, err
	}
	submitted := false
	if !found {
		if err := executor.submit(ctx, submissionID, entrypoint, envVars); err != nil {
			return nil, err
		}
		submitted = true
	}

	missingAfterSubmit := 0
	for {
		status, found, err := executor.jobStatus(ctx, submissionID)
		if err != nil {
			return nil, err
		}
		if !found {
			if submitted && missingAfterSubmit < rayJobRegistrationAttempts {
				missingAfterSubmit++
				recordRayHeartbeat(ctx, submissionID)
				if err := sleepContext(ctx, executor.pollInterval); err != nil {
					return nil, err
				}
				continue
			}
			return nil, failureError.Extend("ray job disappeared after submit")
		}
		switch rayJobTerminalStatus(status.Status) {
		case rayTerminalStatusSucceeded:
			return readResult(ctx)
		case rayTerminalStatusFailed:
			return nil, failureError.Extend("ray job " + strings.ToLower(string(status.Status)) + ": " + status.Message)
		default:
			recordRayHeartbeat(ctx, submissionID)
			if err := sleepContext(ctx, executor.pollInterval); err != nil {
				return nil, err
			}
		}
	}
}

func recordRayHeartbeat(ctx context.Context, submissionID string) {
	log.Trace("recordRayHeartbeat")

	if activity.IsActivity(ctx) {
		activity.RecordHeartbeat(ctx, submissionID)
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
			"job_submission_id": submissionID,
			"submission_id":     submissionID,
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
	parsed.Status = rayJobStatusValueFromString(string(parsed.Status))
	return parsed, true, nil
}

func rayJobStatusValueFromString(status string) rayJobStatusValue {
	log.Trace("rayJobStatusValueFromString")

	return rayJobStatusValue(strings.ToUpper(strings.TrimSpace(status)))
}

func rayJobTerminalStatus(status rayJobStatusValue) rayTerminalStatus {
	log.Trace("rayJobTerminalStatus")

	switch status {
	case rayJobStatusSucceeded:
		return rayTerminalStatusSucceeded
	case rayJobStatusFailed, rayJobStatusStopped:
		return rayTerminalStatusFailed
	default:
		return rayTerminalStatusRunning
	}
}

func (e *RayExecutor) stop(ctx context.Context, submissionID string) error {
	log.Trace("RayExecutor stop")

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.url+"/api/jobs/"+url.PathEscape(submissionID)+"/stop", nil)
	if err != nil {
		return fmt.Errorf("%w: build ray stop request: %w", domain.ErrTrainModel, err)
	}
	resp, err := e.client.Do(req)
	if err != nil {
		return fmt.Errorf("%w: stop ray job: %w", domain.ErrTrainModel, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("%w: ray stop failed with status %d: %s", domain.ErrTrainModel, resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	return nil
}

func readTrainingArtifact(ctx context.Context, manifestReader app.ManifestReader, location string, trainingRunID string) (*model.TrainedModelArtifact, error) {
	log.Trace("readTrainingArtifact")

	raw, err := manifestReader.Read(ctx, location)
	if err != nil {
		return nil, fmt.Errorf("%w: read training artifact manifest: %w", domain.ErrTrainModel, err)
	}
	var dto trainedModelArtifactDTO
	if err := json.Unmarshal(raw, &dto); err != nil {
		return nil, fmt.Errorf("%w: decode training artifact manifest: %w", domain.ErrTrainModel, err)
	}
	artifact := newTrainingManifestDTOAdapter().ToTrainedModelArtifact(ctx, dto)
	if strings.TrimSpace(artifact.TrainingRunID) != trainingRunID {
		return nil, domain.ErrTrainModel.Extend("training artifact manifest has mismatched training run id")
	}
	if strings.TrimSpace(artifact.ModelURI) == "" || strings.TrimSpace(artifact.ArtifactChecksum) == "" || artifact.ArtifactSizeBytes <= 0 {
		return nil, domain.ErrTrainModel.Extend("training artifact manifest is incomplete")
	}
	info, err := manifestReader.Stat(ctx, artifact.ModelURI)
	if err != nil {
		return nil, fmt.Errorf("%w: verify training artifact: %w", domain.ErrTrainModel, err)
	}
	if info.SizeBytes != artifact.ArtifactSizeBytes {
		return nil, domain.ErrTrainModel.Extend(fmt.Sprintf("training artifact size mismatch: manifest=%d actual=%d", artifact.ArtifactSizeBytes, info.SizeBytes))
	}
	if info.Checksum != "" && info.Checksum != artifact.ArtifactChecksum {
		return nil, domain.ErrTrainModel.Extend("training artifact checksum mismatch")
	}
	return artifact, nil
}

func readEvaluationReport(ctx context.Context, manifestReader app.ManifestReader, location string, trainingRunID string) (*model.EvaluationReport, error) {
	log.Trace("readEvaluationReport")

	raw, err := manifestReader.Read(ctx, location)
	if err != nil {
		return nil, fmt.Errorf("%w: read evaluation report manifest: %w", domain.ErrEvaluateModel, err)
	}
	var dto evaluationReportDTO
	if err := json.Unmarshal(raw, &dto); err != nil {
		return nil, fmt.Errorf("%w: decode evaluation report manifest: %w", domain.ErrEvaluateModel, err)
	}
	report := newTrainingManifestDTOAdapter().ToEvaluationReport(ctx, dto)
	if strings.TrimSpace(report.TrainingRunID) != trainingRunID {
		return nil, domain.ErrEvaluateModel.Extend("evaluation report manifest has mismatched training run id")
	}
	if strings.TrimSpace(report.ReportURI) == "" {
		return nil, domain.ErrEvaluateModel.Extend("evaluation report manifest is incomplete")
	}
	if !report.Passed && strings.TrimSpace(report.FailureReason) == "" {
		return nil, domain.ErrEvaluateModel.Extend("evaluation report failure reason is required")
	}
	info, err := manifestReader.Stat(ctx, report.ReportURI)
	if err != nil {
		return nil, fmt.Errorf("%w: verify evaluation report: %w", domain.ErrEvaluateModel, err)
	}
	if info.SizeBytes <= 0 {
		return nil, domain.ErrEvaluateModel.Extend("evaluation report is empty")
	}
	return report, nil
}

func readPromotionReport(ctx context.Context, manifestReader app.ManifestReader, location string, modelID string, orgID string) (*model.PromotionReport, error) {
	log.Trace("readPromotionReport")

	raw, err := manifestReader.Read(ctx, location)
	if err != nil {
		return nil, fmt.Errorf("%w: read promotion report manifest: %w", domain.ErrEvaluateModel, err)
	}
	var dto promotionReportDTO
	if err := json.Unmarshal(raw, &dto); err != nil {
		return nil, fmt.Errorf("%w: decode promotion report manifest: %w", domain.ErrEvaluateModel, err)
	}
	report := newTrainingManifestDTOAdapter().ToPromotionReport(ctx, dto)
	if strings.TrimSpace(report.ModelID) != modelID {
		return nil, domain.ErrEvaluateModel.Extend("promotion report manifest has mismatched model id")
	}
	if strings.TrimSpace(report.OrgID) != orgID {
		return nil, domain.ErrEvaluateModel.Extend("promotion report manifest has mismatched org id")
	}
	if strings.TrimSpace(report.PromotionReportURI) == "" {
		return nil, domain.ErrEvaluateModel.Extend("promotion report manifest is incomplete")
	}
	info, err := manifestReader.Stat(ctx, report.PromotionReportURI)
	if err != nil {
		return nil, fmt.Errorf("%w: verify promotion report: %w", domain.ErrEvaluateModel, err)
	}
	if info.SizeBytes <= 0 {
		return nil, domain.ErrEvaluateModel.Extend("promotion report is empty")
	}
	return report, nil
}

func trainingEnv(spec model.TrainingJobSpec) map[string]string {
	log.Trace("trainingEnv")

	return map[string]string{
		"TRAINING_RUN_ID":                 spec.TrainingRunID,
		"TRAINING_DATASET_URI":            spec.DatasetURI,
		"TRAINING_MODEL_NAME":             spec.ModelName,
		"TRAINING_MODEL_VERSION":          spec.ModelVersion,
		"TRAINING_BASE_MODEL":             spec.BaseModel,
		"TRAINING_MODEL_URI":              spec.ModelURI,
		"TRAINING_ADAPTER_URI":            spec.AdapterURI,
		"TRAINING_SERVING_TARGET":         spec.ServingTarget,
		"TRAINING_SERVING_MODEL":          spec.ServingModel,
		"TRAINING_SERVING_LOAD_STATUS":    spec.ServingLoadStatus,
		"TRAINING_ARTIFACT_FORMAT":        spec.ArtifactFormat,
		"TRAINING_ARTIFACT_MANIFEST_URI":  spec.ArtifactManifestURI,
		"TRAINING_ARTIFACT_BUCKET_REGION": spec.ArtifactBucketRegion,
		"TRAINING_AXOLOTL_COMMAND":        spec.AxolotlCommand,
		"TRAINING_RECIPE_YAML":            spec.RecipeYAML,
		"TRAINING_RECIPE_HASH":            spec.RecipeHash,
	}
}

func evaluationEnv(spec model.EvaluationJobSpec) map[string]string {
	log.Trace("evaluationEnv")

	return map[string]string{
		"TRAINING_RUN_ID":                  spec.TrainingRunID,
		"TRAINING_MODEL_URI":               spec.ModelURI,
		"TRAINING_ARTIFACT_BUCKET_REGION":  spec.ArtifactBucketRegion,
		"TRAINING_EVALUATION_PROFILE":      spec.EvaluationProfile,
		"TRAINING_EVALUATION_REPORT_URI":   spec.ReportURI,
		"TRAINING_EVALUATION_MANIFEST_URI": spec.ReportManifestURI,
	}
}

func promotionReportEnv(spec model.PromotionReportJobSpec) map[string]string {
	log.Trace("promotionReportEnv")

	return map[string]string{
		"TRAINING_ARTIFACT_BUCKET_REGION": spec.ArtifactBucketRegion,
	}
}

func promotionReportEntrypoint(entrypoint string, spec model.PromotionReportJobSpec) (string, error) {
	log.Trace("promotionReportEntrypoint")

	raw, err := json.Marshal(promotionReportJobRequest{
		UserID:                   spec.UserID,
		OrgID:                    spec.OrgID,
		ModelID:                  spec.ModelID,
		TrainingRunID:            spec.TrainingRunID,
		CandidateReportURI:       spec.CandidateReportURI,
		CandidateMetricsMetadata: spec.CandidateMetricsMetadata,
		ChampionModelID:          spec.ChampionModelID,
		ChampionReportURI:        spec.ChampionReportURI,
		ChampionMetricsMetadata:  spec.ChampionMetricsMetadata,
		PromotionProfile:         spec.PromotionProfile,
		ReportURI:                spec.ReportURI,
		ReportManifestURI:        spec.ReportManifestURI,
	})
	if err != nil {
		return "", fmt.Errorf("%w: marshal promotion report job spec: %w", domain.ErrEvaluateModel, err)
	}
	encoded := base64.RawURLEncoding.EncodeToString(raw)
	return strings.TrimSpace(entrypoint) + " --job-spec-b64 " + encoded, nil
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

type ObjectManifestReader struct {
	region     string
	downloader s3Downloader
	client     *http.Client
}

type s3Downloader interface {
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

func NewObjectManifestReader(ctx context.Context, region string, client *http.Client) (*ObjectManifestReader, error) {
	log.Trace("NewObjectManifestReader")

	region = strings.TrimSpace(region)
	if region == "" {
		return nil, domain.ErrValidationFailed.Extend("artifact bucket region is required")
	}
	if client == nil {
		client = &http.Client{
			Transport: otelhttp.NewTransport(http.DefaultTransport),
		}
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

func (r *ObjectManifestReader) Stat(ctx context.Context, location string) (model.ObjectInfo, error) {
	log.Trace("ObjectManifestReader Stat")

	parsed, err := url.Parse(strings.TrimSpace(location))
	if err != nil {
		return model.ObjectInfo{}, fmt.Errorf("parse artifact location: %w", err)
	}
	switch parsed.Scheme {
	case "s3":
		return r.statS3(ctx, parsed.Host, strings.TrimPrefix(parsed.Path, "/"), location)
	case "http", "https":
		return r.statHTTP(ctx, location)
	case "file":
		return statPath(filepath.Clean(parsed.Path), location)
	case "":
		return statPath(filepath.Clean(location), location)
	default:
		return model.ObjectInfo{}, fmt.Errorf("unsupported artifact scheme %q", parsed.Scheme)
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

func (r *ObjectManifestReader) statS3(ctx context.Context, bucketName, key, location string) (model.ObjectInfo, error) {
	log.Trace("ObjectManifestReader statS3")

	if r.region == corebucket.LocalDevS3Region {
		return statPath(filepath.Clean(filepath.Join(corebucket.StorageDir, bucketName, key)), location)
	}
	if r.downloader == nil {
		return model.ObjectInfo{}, fmt.Errorf("s3 downloader is not configured")
	}
	head, err := r.downloader.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	if err == nil {
		return model.ObjectInfo{Location: location, SizeBytes: aws.ToInt64(head.ContentLength), Checksum: s3Checksum(head)}, nil
	}
	return r.statS3Prefix(ctx, bucketName, strings.TrimSuffix(key, "/")+"/", location)
}

func (r *ObjectManifestReader) statS3Prefix(ctx context.Context, bucketName, prefix, location string) (model.ObjectInfo, error) {
	log.Trace("ObjectManifestReader statS3Prefix")

	var total int64
	var found bool
	var continuation *string
	for {
		output, err := r.downloader.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucketName),
			Prefix:            aws.String(prefix),
			ContinuationToken: continuation,
		})
		if err != nil {
			return model.ObjectInfo{}, fmt.Errorf("list s3 artifact prefix: %w", err)
		}
		for _, object := range output.Contents {
			found = true
			total += aws.ToInt64(object.Size)
		}
		if output.NextContinuationToken == nil {
			break
		}
		continuation = output.NextContinuationToken
	}
	if !found {
		return model.ObjectInfo{}, fmt.Errorf("artifact not found at %s", location)
	}
	return model.ObjectInfo{Location: location, SizeBytes: total}, nil
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

func (r *ObjectManifestReader) statHTTP(ctx context.Context, location string) (model.ObjectInfo, error) {
	log.Trace("ObjectManifestReader statHTTP")

	req, err := http.NewRequestWithContext(ctx, http.MethodHead, location, nil)
	if err != nil {
		return model.ObjectInfo{}, err
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return model.ObjectInfo{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return model.ObjectInfo{}, fmt.Errorf("artifact stat failed with status %d", resp.StatusCode)
	}
	if resp.ContentLength <= 0 {
		return model.ObjectInfo{}, fmt.Errorf("artifact at %s is empty or missing content length", location)
	}
	return model.ObjectInfo{Location: location, SizeBytes: resp.ContentLength}, nil
}

func statPath(path, location string) (model.ObjectInfo, error) {
	log.Trace("statPath")

	info, err := os.Stat(path)
	if err != nil {
		return model.ObjectInfo{}, err
	}
	if !info.IsDir() {
		return model.ObjectInfo{Location: location, SizeBytes: info.Size()}, nil
	}
	var total int64
	if err := filepath.WalkDir(path, func(next string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		fileInfo, err := entry.Info()
		if err != nil {
			return err
		}
		total += fileInfo.Size()
		return nil
	}); err != nil {
		return model.ObjectInfo{}, err
	}
	if total <= 0 {
		return model.ObjectInfo{}, fmt.Errorf("artifact at %s is empty", location)
	}
	return model.ObjectInfo{Location: location, SizeBytes: total}, nil
}

func s3Checksum(output *s3.HeadObjectOutput) string {
	log.Trace("s3Checksum")

	if output == nil || output.ChecksumSHA256 == nil || strings.TrimSpace(*output.ChecksumSHA256) == "" {
		return ""
	}
	return "sha256:" + strings.TrimSpace(*output.ChecksumSHA256)
}
