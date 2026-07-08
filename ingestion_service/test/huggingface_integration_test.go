package integration_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"ingestion_service/pkg/domain"
	"ingestion_service/pkg/domain/model"
	"ingestion_service/pkg/infra/download"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Hugging Face onboarding integration", func() {
	It("downloads and validates a real Hugging Face model file when explicitly enabled", func() {
		if !realHuggingFaceE2EEnabled() {
			Skip("set BIGHILL_E2E_HUGGINGFACE_REAL_DOWNLOAD=true to run real Hugging Face onboarding integration")
		}
		token := requiredHFEnv("BIGHILL_E2E_HUGGINGFACE_TOKEN")
		repoID := requiredHFEnv("BIGHILL_E2E_HUGGINGFACE_REPO_ID")
		fileName := requiredHFEnv("BIGHILL_E2E_HUGGINGFACE_FILE")
		revision := envOrDefault("BIGHILL_E2E_HUGGINGFACE_REVISION", "main")
		artifactFormat := envOrDefault("BIGHILL_E2E_HUGGINGFACE_ARTIFACT_FORMAT", "GGUF_MODEL")
		resourceID := uuid.New()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
		defer cancel()

		downloader := realHuggingFaceCommandDownloader("file://" + GinkgoT().TempDir())
		result, err := downloader.DownloadHuggingFaceModel(ctx, model.OnboardHuggingFaceModelRequest{
			ResourceID:       resourceID,
			RepoID:           repoID,
			Revision:         revision,
			ModelName:        "hf-real-integration",
			ModelVersion:     "1",
			BaseModel:        repoID,
			ArtifactType:     "BASE_MODEL",
			ArtifactFormat:   artifactFormat,
			HuggingFaceFile:  fileName,
			HuggingFaceToken: token,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(result.ResourceID).To(Equal(resourceID))
		Expect(result.ArtifactFormat).To(Equal(strings.ToUpper(strings.ReplaceAll(artifactFormat, "-", "_"))))
		Expect(result.ArtifactChecksum).To(HavePrefix("sha256:"))
		Expect(result.ArtifactSizeBytes).To(BeNumerically(">", 0))
		Expect(result.HFRepoID).To(Equal(repoID))
		Expect(result.HFRevision).To(Equal(revision))
		Expect(result.HFCommitSHA).To(MatchRegexp(`^[0-9a-f]{40}$`))
		Expect(result.StorageLocation).To(ContainSubstring(resourceID.String()))
		Expect(result.StorageLocation).To(HaveSuffix("/" + filepath.Base(fileName)))
		Expect(result.ManifestLocation).To(ContainSubstring(resourceID.String()))
	})

	It("maps real Hugging Face provider failures to external provider errors when explicitly enabled", func() {
		if !realHuggingFaceE2EEnabled() {
			Skip("set BIGHILL_E2E_HUGGINGFACE_REAL_DOWNLOAD=true to run real Hugging Face provider error integration")
		}
		token := requiredHFEnv("BIGHILL_E2E_HUGGINGFACE_TOKEN")
		repoID := "bighill/non-existent-model-" + uuid.NewString()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		downloader := realHuggingFaceCommandDownloader("file://" + GinkgoT().TempDir())
		_, err := downloader.DownloadHuggingFaceModel(ctx, model.OnboardHuggingFaceModelRequest{
			ResourceID:       uuid.New(),
			RepoID:           repoID,
			Revision:         "main",
			ModelName:        "missing",
			ModelVersion:     "1",
			BaseModel:        repoID,
			ArtifactType:     "BASE_MODEL",
			ArtifactFormat:   "GGUF_MODEL",
			HuggingFaceFile:  "missing.gguf",
			HuggingFaceToken: token,
		})

		var providerErr *domain.ExternalProviderError
		Expect(errors.As(err, &providerErr)).To(BeTrue())
		Expect(providerErr.Provider).To(Equal("Hugging Face"))
		Expect(providerErr.Code).NotTo(BeEmpty())
		Expect(providerErr.Error()).To(ContainSubstring("Hugging Face returned"))
	})
})

func realHuggingFaceCommandDownloader(outputURI string) *download.HuggingFaceCommandDownloader {
	downloader, err := download.NewHuggingFaceCommandDownloader(download.HuggingFaceCommandDownloaderConfig{
		Command:          huggingFaceE2EPython() + " -m training_jobs.model_onboard",
		WorkingDirectory: filepath.Join(repoRoot(), "training_service", "training_jobs"),
		OutputURI:        outputURI,
		Timeout:          20 * time.Minute,
		EnvKeys: download.HuggingFaceJobEnvKeys{
			ResourceID:     "INGESTION_SERVICE_MODEL_RESOURCE_ID",
			ModelName:      "INGESTION_SERVICE_MODEL_NAME",
			ModelVersion:   "INGESTION_SERVICE_MODEL_VERSION",
			BaseModel:      "INGESTION_SERVICE_MODEL_BASE_MODEL",
			ArtifactType:   "INGESTION_SERVICE_MODEL_ARTIFACT_TYPE",
			ArtifactFormat: "INGESTION_SERVICE_MODEL_ARTIFACT_FORMAT",
			FileName:       "INGESTION_SERVICE_HUGGINGFACE_FILE",
			RepoID:         "INGESTION_SERVICE_HUGGINGFACE_REPO_ID",
			Revision:       "INGESTION_SERVICE_HUGGINGFACE_REVISION",
			Token:          "INGESTION_SERVICE_HUGGINGFACE_TOKEN",
			OutputURI:      "INGESTION_SERVICE_HUGGINGFACE_OUTPUT_URI",
		},
	})
	Expect(err).NotTo(HaveOccurred())
	return downloader
}

func realHuggingFaceE2EEnabled() bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv("BIGHILL_E2E_HUGGINGFACE_REAL_DOWNLOAD")), "true")
}

func requiredHFEnv(name string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		Skip(name + " is required for real Hugging Face onboarding integration")
	}
	return value
}

func huggingFaceE2EPython() string {
	if configured := strings.TrimSpace(os.Getenv("BIGHILL_E2E_HUGGINGFACE_PYTHON")); configured != "" {
		return configured
	}
	pyenv := filepath.Join(os.Getenv("HOME"), ".pyenv", "versions", "3.11.9", "bin", "python")
	if _, err := os.Stat(pyenv); err == nil {
		return pyenv
	}
	if found, err := exec.LookPath("python3.11"); err == nil {
		return found
	}
	if found, err := exec.LookPath("python3"); err == nil {
		return found
	}
	return "python"
}

func repoRoot() string {
	wd, err := os.Getwd()
	Expect(err).NotTo(HaveOccurred())
	for dir := wd; dir != filepath.Dir(dir); dir = filepath.Dir(dir) {
		if _, err := os.Stat(filepath.Join(dir, "shared_py")); err == nil {
			return dir
		}
	}
	Fail("repository root not found")
	return ""
}

func envOrDefault(name string, fallback string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	return value
}
