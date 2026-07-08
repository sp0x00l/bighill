package download

import (
	"context"
	"encoding/json"
	"fmt"
	"ingestion_service/pkg/domain"
	"ingestion_service/pkg/domain/model"
	"os"
	"strconv"
	"strings"
	"time"

	"lib/shared_lib/processrunner"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type HuggingFaceCommandDownloader struct {
	command          []string
	workingDirectory string
	outputURI        string
	timeout          time.Duration
	envKeys          HuggingFaceJobEnvKeys
}

type HuggingFaceCommandDownloaderConfig struct {
	Command          string
	WorkingDirectory string
	OutputURI        string
	Timeout          time.Duration
	EnvKeys          HuggingFaceJobEnvKeys
}

type HuggingFaceJobEnvKeys struct {
	ResourceID     string
	ModelName      string
	ModelVersion   string
	BaseModel      string
	ArtifactType   string
	ArtifactFormat string
	FileName       string
	RepoID         string
	Revision       string
	Token          string
	OutputURI      string
}

type huggingFaceDownloadResult struct {
	ResourceID        string `json:"resource_id"`
	StorageLocation   string `json:"storage_location"`
	ManifestLocation  string `json:"manifest_location"`
	ArtifactType      string `json:"artifact_type"`
	ArtifactFormat    string `json:"artifact_format"`
	ArtifactSizeBytes int64  `json:"artifact_size_bytes"`
	ArtifactChecksum  string `json:"artifact_checksum"`
	ModelName         string `json:"model_name"`
	ModelVersion      string `json:"model_version"`
	BaseModel         string `json:"base_model"`
	SourceURI         string `json:"source_uri"`
	HFRepoID          string `json:"hf_repo_id"`
	HFRevision        string `json:"hf_revision"`
	HFCommitSHA       string `json:"hf_commit_sha"`
}

type huggingFaceCommandError struct {
	Provider   string `json:"provider"`
	HTTPStatus int    `json:"http_status"`
	ErrorCode  string `json:"error_code"`
	Message    string `json:"message"`
	RepoID     string `json:"repo_id"`
	Revision   string `json:"revision"`
}

func NewHuggingFaceCommandDownloader(config HuggingFaceCommandDownloaderConfig) (*HuggingFaceCommandDownloader, error) {
	log.Trace("NewHuggingFaceCommandDownloader")

	return &HuggingFaceCommandDownloader{
		command:          strings.Fields(config.Command),
		workingDirectory: strings.TrimSpace(config.WorkingDirectory),
		outputURI:        strings.TrimRight(strings.TrimSpace(config.OutputURI), "/"),
		timeout:          config.Timeout,
		envKeys:          config.EnvKeys.trimmed(),
	}, nil
}

func (d *HuggingFaceCommandDownloader) DownloadHuggingFaceModel(ctx context.Context, request model.OnboardHuggingFaceModelRequest) (*model.OnboardedModelArtifact, error) {
	log.Trace("HuggingFaceCommandDownloader DownloadHuggingFaceModel")

	runResult, err := processrunner.Run(ctx, processrunner.Command{
		Name:    d.command[0],
		Args:    d.command[1:],
		Dir:     d.workingDirectory,
		Env:     append(os.Environ(), d.envKeys.commandEnv(request, request.HuggingFaceToken, d.outputURI)...),
		Timeout: d.timeout,
	})
	if err != nil {
		if providerErr, ok := providerErrorFromCommandStderr(runResult.Stderr); ok {
			return nil, providerErr
		}
		return nil, fmt.Errorf("%w: hugging face download command failed: %w: %s", domain.ErrValidationFailed, err, strings.TrimSpace(runResult.Stderr))
	}
	var downloadResult huggingFaceDownloadResult
	if err := json.Unmarshal([]byte(strings.TrimSpace(string(runResult.Stdout))), &downloadResult); err != nil {
		return nil, fmt.Errorf("%w: parse hugging face download result: %w", domain.ErrValidationFailed, err)
	}
	return downloadResultToArtifact(request, downloadResult)
}

func providerErrorFromCommandStderr(stderr string) (*domain.ExternalProviderError, bool) {
	log.Trace("providerErrorFromCommandStderr")

	lines := strings.Split(strings.TrimSpace(stderr), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if line == "" || !strings.HasPrefix(line, "{") {
			continue
		}
		var payload huggingFaceCommandError
		if err := json.Unmarshal([]byte(line), &payload); err != nil {
			continue
		}
		if strings.TrimSpace(payload.ErrorCode) == "" && payload.HTTPStatus == 0 {
			continue
		}
		provider := strings.TrimSpace(payload.Provider)
		if provider == "" {
			provider = "Hugging Face"
		}
		return &domain.ExternalProviderError{
			Provider:   provider,
			StatusCode: payload.HTTPStatus,
			Code:       strings.TrimSpace(payload.ErrorCode),
			Message:    strings.TrimSpace(payload.Message),
		}, true
	}
	return nil, false
}

func (k HuggingFaceJobEnvKeys) trimmed() HuggingFaceJobEnvKeys {
	log.Trace("HuggingFaceJobEnvKeys trimmed")

	return HuggingFaceJobEnvKeys{
		ResourceID:     strings.TrimSpace(k.ResourceID),
		ModelName:      strings.TrimSpace(k.ModelName),
		ModelVersion:   strings.TrimSpace(k.ModelVersion),
		BaseModel:      strings.TrimSpace(k.BaseModel),
		ArtifactType:   strings.TrimSpace(k.ArtifactType),
		ArtifactFormat: strings.TrimSpace(k.ArtifactFormat),
		FileName:       strings.TrimSpace(k.FileName),
		RepoID:         strings.TrimSpace(k.RepoID),
		Revision:       strings.TrimSpace(k.Revision),
		Token:          strings.TrimSpace(k.Token),
		OutputURI:      strings.TrimSpace(k.OutputURI),
	}
}

func (k HuggingFaceJobEnvKeys) commandEnv(request model.OnboardHuggingFaceModelRequest, token string, outputURI string) []string {
	log.Trace("HuggingFaceJobEnvKeys commandEnv")

	env := k.envValues(request, token, outputURI)
	out := make([]string, 0, len(env))
	for key, value := range env {
		out = append(out, key+"="+value)
	}
	return out
}

func (k HuggingFaceJobEnvKeys) envValues(request model.OnboardHuggingFaceModelRequest, token string, outputURI string) map[string]string {
	log.Trace("HuggingFaceJobEnvKeys envValues")

	keys := k.trimmed()
	return map[string]string{
		keys.ResourceID:     request.ResourceID.String(),
		keys.ModelName:      strings.TrimSpace(request.ModelName),
		keys.ModelVersion:   strings.TrimSpace(request.ModelVersion),
		keys.BaseModel:      strings.TrimSpace(request.BaseModel),
		keys.ArtifactType:   strings.TrimSpace(request.ArtifactType),
		keys.ArtifactFormat: strings.TrimSpace(request.ArtifactFormat),
		keys.FileName:       strings.TrimSpace(request.HuggingFaceFile),
		keys.RepoID:         strings.TrimSpace(request.RepoID),
		keys.Revision:       strings.TrimSpace(request.Revision),
		keys.Token:          strings.TrimSpace(token),
		keys.OutputURI:      strings.TrimSpace(outputURI),
	}
}

func downloadResultToArtifact(request model.OnboardHuggingFaceModelRequest, result huggingFaceDownloadResult) (*model.OnboardedModelArtifact, error) {
	log.Trace("downloadResultToArtifact")

	if err := validateDownloadResult(result); err != nil {
		return nil, err
	}
	resourceID := request.ResourceID
	parsed, err := uuid.Parse(strings.TrimSpace(result.ResourceID))
	if err != nil {
		return nil, fmt.Errorf("%w: download result resource_id is invalid: %w", domain.ErrValidationFailed, err)
	}
	resourceID = parsed
	return &model.OnboardedModelArtifact{
		ResourceID:        resourceID,
		StorageLocation:   strings.TrimSpace(result.StorageLocation),
		ManifestLocation:  strings.TrimSpace(result.ManifestLocation),
		ArtifactType:      strings.TrimSpace(result.ArtifactType),
		ArtifactFormat:    strings.TrimSpace(result.ArtifactFormat),
		ArtifactSizeBytes: result.ArtifactSizeBytes,
		ArtifactChecksum:  strings.TrimSpace(result.ArtifactChecksum),
		ModelName:         strings.TrimSpace(result.ModelName),
		ModelVersion:      strings.TrimSpace(result.ModelVersion),
		BaseModel:         strings.TrimSpace(result.BaseModel),
		SourceURI:         strings.TrimSpace(result.SourceURI),
		HFRepoID:          strings.TrimSpace(result.HFRepoID),
		HFRevision:        strings.TrimSpace(result.HFRevision),
		HFCommitSHA:       strings.TrimSpace(result.HFCommitSHA),
	}, nil
}

func validateDownloadResult(result huggingFaceDownloadResult) error {
	log.Trace("validateDownloadResult")

	required := map[string]string{
		"resource_id":       result.ResourceID,
		"storage_location":  result.StorageLocation,
		"manifest_location": result.ManifestLocation,
		"artifact_type":     result.ArtifactType,
		"artifact_format":   result.ArtifactFormat,
		"artifact_checksum": result.ArtifactChecksum,
		"model_name":        result.ModelName,
		"model_version":     result.ModelVersion,
		"base_model":        result.BaseModel,
		"source_uri":        result.SourceURI,
		"hf_repo_id":        result.HFRepoID,
		"hf_revision":       result.HFRevision,
		"hf_commit_sha":     result.HFCommitSHA,
	}
	for field, value := range required {
		if strings.TrimSpace(value) == "" {
			return domain.ErrValidationFailed.Extend("hugging face manifest field is required: " + field)
		}
	}
	if result.ArtifactSizeBytes <= 0 {
		return domain.ErrValidationFailed.Extend("hugging face manifest artifact_size_bytes must be greater than zero")
	}
	version, err := strconv.Atoi(strings.TrimSpace(result.ModelVersion))
	if err != nil || version <= 0 {
		return domain.ErrValidationFailed.Extend("hugging face manifest model_version must be a positive integer")
	}
	return nil
}
