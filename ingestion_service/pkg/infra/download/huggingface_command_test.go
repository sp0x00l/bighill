package download_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"ingestion_service/pkg/domain"
	"ingestion_service/pkg/domain/model"
	"ingestion_service/pkg/infra/download"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestDownload(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Ingestion download adapter suite")
}

var _ = Describe("HuggingFaceCommandDownloader", func() {
	It("runs the configured command and maps the JSON result", func() {
		dir := GinkgoT().TempDir()
		workingDirectory := filepath.Join(dir, "workdir")
		Expect(os.MkdirAll(workingDirectory, 0o755)).To(Succeed())
		Expect(os.WriteFile(filepath.Join(workingDirectory, ".hf-working-directory"), []byte("ok"), 0o644)).To(Succeed())
		script := filepath.Join(dir, "hf-download")
		Expect(os.WriteFile(script, []byte(`#!/usr/bin/env sh
test "$INGESTION_SERVICE_HUGGINGFACE_TOKEN" = "hf-token" || exit 7
test -f .hf-working-directory || exit 8
printf '{"resource_id":"%s","storage_location":"s3://bucket/models/%s/snapshot","manifest_location":"s3://bucket/models/%s/manifest.json","artifact_type":"BASE_MODEL","artifact_format":"HF_MODEL","artifact_size_bytes":12,"artifact_checksum":"sha256:test","model_name":"llama","model_version":"1","base_model":"meta-llama/Llama","source_uri":"https://huggingface.co/meta-llama/Llama","hf_repo_id":"meta-llama/Llama","hf_revision":"main","hf_commit_sha":"abc"}' "$INGESTION_SERVICE_MODEL_RESOURCE_ID" "$INGESTION_SERVICE_MODEL_RESOURCE_ID" "$INGESTION_SERVICE_MODEL_RESOURCE_ID"
`), 0o755)).To(Succeed())
		resourceID := uuid.New()
		downloader, err := download.NewHuggingFaceCommandDownloader(download.HuggingFaceCommandDownloaderConfig{
			Command:          script,
			WorkingDirectory: workingDirectory,
			OutputURI:        "s3://bucket/models",
			Timeout:          10 * time.Second,
			EnvKeys:          testHuggingFaceJobEnvKeys(),
		})
		Expect(err).NotTo(HaveOccurred())

		result, err := downloader.DownloadHuggingFaceModel(context.Background(), model.OnboardHuggingFaceModelRequest{
			ResourceID:       resourceID,
			RepoID:           "meta-llama/Llama",
			Revision:         "main",
			ModelName:        "llama",
			ModelVersion:     "1",
			BaseModel:        "meta-llama/Llama",
			HuggingFaceToken: "hf-token",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(result.ResourceID).To(Equal(resourceID))
		Expect(result.StorageLocation).To(Equal("s3://bucket/models/" + resourceID.String() + "/snapshot"))
		Expect(result.HFCommitSHA).To(Equal("abc"))
	})

	It("maps structured Hugging Face command errors to provider errors", func() {
		dir := GinkgoT().TempDir()
		script := filepath.Join(dir, "hf-download")
		Expect(os.WriteFile(script, []byte(`#!/usr/bin/env sh
printf 'progress before failure\n' >&2
printf '{"provider":"Hugging Face","http_status":403,"error_code":"GATED_REPO","message":"Access to model meta-llama/Meta-Llama-3-8B is restricted","repo_id":"meta-llama/Meta-Llama-3-8B","revision":"main"}\n' >&2
exit 1
`), 0o755)).To(Succeed())
		downloader, err := download.NewHuggingFaceCommandDownloader(download.HuggingFaceCommandDownloaderConfig{
			Command:   script,
			OutputURI: "s3://bucket/models",
			Timeout:   10 * time.Second,
			EnvKeys:   testHuggingFaceJobEnvKeys(),
		})
		Expect(err).NotTo(HaveOccurred())

		_, err = downloader.DownloadHuggingFaceModel(context.Background(), model.OnboardHuggingFaceModelRequest{
			ResourceID:       uuid.New(),
			RepoID:           "meta-llama/Meta-Llama-3-8B",
			Revision:         "main",
			ModelName:        "llama",
			ModelVersion:     "1",
			BaseModel:        "meta-llama/Meta-Llama-3-8B",
			HuggingFaceToken: "hf-token",
		})

		var providerErr *domain.ExternalProviderError
		Expect(errors.As(err, &providerErr)).To(BeTrue())
		Expect(providerErr.Provider).To(Equal("Hugging Face"))
		Expect(providerErr.StatusCode).To(Equal(403))
		Expect(providerErr.Code).To(Equal("GATED_REPO"))
		Expect(providerErr.Error()).To(ContainSubstring("Hugging Face returned 403 (GATED_REPO)"))
	})
})

func testHuggingFaceJobEnvKeys() download.HuggingFaceJobEnvKeys {
	return download.HuggingFaceJobEnvKeys{
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
	}
}
