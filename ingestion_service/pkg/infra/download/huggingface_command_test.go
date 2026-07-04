package download_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

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
		script := filepath.Join(dir, "hf-download")
		Expect(os.WriteFile(script, []byte(`#!/usr/bin/env sh
test "$INGESTION_SERVICE_HUGGINGFACE_TOKEN" = "hf-token" || exit 7
printf '{"resource_id":"%s","storage_location":"s3://bucket/models/%s/snapshot","manifest_location":"s3://bucket/models/%s/manifest.json","artifact_type":"BASE_MODEL","artifact_format":"HF_MODEL","artifact_size_bytes":12,"artifact_checksum":"sha256:test","model_name":"llama","model_version":"1","base_model":"meta-llama/Llama","source_uri":"https://huggingface.co/meta-llama/Llama","hf_repo_id":"meta-llama/Llama","hf_revision":"main","hf_commit_sha":"abc"}' "$INGESTION_SERVICE_MODEL_RESOURCE_ID" "$INGESTION_SERVICE_MODEL_RESOURCE_ID" "$INGESTION_SERVICE_MODEL_RESOURCE_ID"
`), 0o755)).To(Succeed())
		resourceID := uuid.New()
		downloader, err := download.NewHuggingFaceCommandDownloader(download.HuggingFaceCommandDownloaderConfig{
			Command:   script,
			OutputURI: "s3://bucket/models",
			Timeout:   time.Second,
			EnvKeys:   testHuggingFaceJobEnvKeys(),
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
})

func testHuggingFaceJobEnvKeys() download.HuggingFaceJobEnvKeys {
	return download.HuggingFaceJobEnvKeys{
		ResourceID:     "INGESTION_SERVICE_MODEL_RESOURCE_ID",
		ModelName:      "INGESTION_SERVICE_MODEL_NAME",
		ModelVersion:   "INGESTION_SERVICE_MODEL_VERSION",
		BaseModel:      "INGESTION_SERVICE_MODEL_BASE_MODEL",
		ArtifactType:   "INGESTION_SERVICE_MODEL_ARTIFACT_TYPE",
		ArtifactFormat: "INGESTION_SERVICE_MODEL_ARTIFACT_FORMAT",
		RepoID:         "INGESTION_SERVICE_HUGGINGFACE_REPO_ID",
		Revision:       "INGESTION_SERVICE_HUGGINGFACE_REVISION",
		Token:          "INGESTION_SERVICE_HUGGINGFACE_TOKEN",
		OutputURI:      "INGESTION_SERVICE_HUGGINGFACE_OUTPUT_URI",
	}
}
