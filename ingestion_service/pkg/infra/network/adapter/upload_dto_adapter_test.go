package adapter

import (
	"context"
	"errors"
	"testing"

	"ingestion_service/pkg/domain"
	"ingestion_service/pkg/domain/model"
	serializers "lib/shared_lib/serializer"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestUploadDTOAdapter(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Upload DTO adapter suite")
}

var _ = Describe("UploadDTOAdapter", func() {
	var adapter *UploadDTOAdapter

	BeforeEach(func() {
		adapter = NewUploadDTOAdapter(serializers.NewJSONSerializer())
	})

	It("maps model upload DTOs to normalized domain requests", func() {
		userID := uuid.New()
		resourceID := uuid.New()
		datasetID := uuid.New()

		request, err := adapter.FromInitiateModelUploadDTO(context.Background(), []byte(`{
			"resource_id":"`+resourceID.String()+`",
			"dataset_id":"`+datasetID.String()+`",
			"file_name":"adapter.safetensors",
			"artifact_type":"lora-adapter",
			"artifact_format":"safetensors",
			"declared_size_bytes":512,
			"client_nonce":"model-retry-1",
			"model_name":"movie-twin",
			"model_version":"v3",
			"base_model":"meta-llama/Llama-3.1-8B"
		}`), userID, 1024)

		Expect(err).NotTo(HaveOccurred())
		Expect(request.ResourceID).To(Equal(resourceID))
		Expect(request.DatasetID).To(Equal(datasetID))
		Expect(request.UserID).To(Equal(userID))
		Expect(request.ArtifactType).To(Equal("LORA_ADAPTER"))
		Expect(request.ArtifactFormat).To(Equal("SAFETENSORS"))
		Expect(request.DeclaredContentType).To(Equal("application/octet-stream"))
		Expect(request.ModelVersion).To(Equal("3"))
	})

	It("maps Hugging Face onboarding DTOs to base model requests", func() {
		userID := uuid.New()

		request, err := adapter.FromOnboardHuggingFaceModelDTO(context.Background(), []byte(`{
			"repo_id":"meta-llama/Llama-3.1-8B",
			"client_nonce":"hf-1",
			"model_name":"llama",
			"model_version":"1",
			"base_model":"meta-llama/Llama-3.1-8B"
		}`), userID)

		Expect(err).NotTo(HaveOccurred())
		Expect(request.UserID).To(Equal(userID))
		Expect(request.Revision).To(Equal("main"))
		Expect(request.ArtifactType).To(Equal("BASE_MODEL"))
		Expect(request.ArtifactFormat).To(Equal("HF_MODEL"))
	})

	It("maps Hugging Face exact-file GGUF onboarding DTOs", func() {
		userID := uuid.New()

		request, err := adapter.FromOnboardHuggingFaceModelDTO(context.Background(), []byte(`{
			"repo_id":"QuantFactory/Meta-Llama-3-8B-Instruct-GGUF",
			"revision":"main",
			"hf_file":"Meta-Llama-3-8B-Instruct.Q4_K_M.gguf",
			"client_nonce":"hf-gguf-1",
			"model_name":"llama-gguf",
			"model_version":"1",
			"base_model":"QuantFactory/Meta-Llama-3-8B-Instruct-GGUF",
			"artifact_type":"BASE_MODEL",
			"artifact_format":"GGUF_MODEL"
		}`), userID)

		Expect(err).NotTo(HaveOccurred())
		Expect(request.UserID).To(Equal(userID))
		Expect(request.HuggingFaceFile).To(Equal("Meta-Llama-3-8B-Instruct.Q4_K_M.gguf"))
		Expect(request.ArtifactType).To(Equal("BASE_MODEL"))
		Expect(request.ArtifactFormat).To(Equal("GGUF_MODEL"))
	})

	It("maps data upload DTOs through the supplied format resolver", func() {
		userID := uuid.New()
		datasetID := uuid.New()
		dataset := &model.Dataset{
			TableNamespace:    "features",
			TableName:         "movies",
			TableFormat:       "PARQUET",
			CatalogProvider:   "LOCAL",
			ProcessingProfile: "TEXT_RAG_PROCESSING_PROFILE",
		}

		request, err := adapter.FromInitiateUploadDTO(context.Background(), []byte(`{
			"file_name":"dataset.csv",
			"declared_size_bytes":100,
			"client_nonce":"data-1"
		}`), datasetID, userID, dataset, func(context.Context, string, string, string) (string, string, error) {
			return "csv", "text/csv", nil
		}, 1024)

		Expect(err).NotTo(HaveOccurred())
		Expect(request.DatasetID).To(Equal(datasetID))
		Expect(request.UserID).To(Equal(userID))
		Expect(request.DeclaredFormat).To(Equal("csv"))
		Expect(request.DeclaredContentType).To(Equal("text/csv"))
		Expect(request.TableName).To(Equal("movies"))
	})

	It("rejects invalid model upload DTOs before they reach the usecase", func() {
		_, err := adapter.FromInitiateModelUploadDTO(context.Background(), []byte(`{
			"file_name":"adapter.safetensors",
			"artifact_type":"unknown",
			"artifact_format":"safetensors",
			"declared_size_bytes":512,
			"client_nonce":"model-retry-1",
			"model_name":"movie-twin",
			"model_version":"abc",
			"base_model":"meta-llama/Llama-3.1-8B"
		}`), uuid.New(), 1024)

		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
	})
})
