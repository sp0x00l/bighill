package messaging_test

import (
	"context"
	"errors"

	"model_registry_service/pkg/domain/model"
	registrymessaging "model_registry_service/pkg/infra/network/messaging"

	ingestionpb "lib/data_contracts_lib/ingestion"
	shared "lib/shared_lib/messaging"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Model artifact ingested listener mapping and error policy", func() {
	var (
		artifactID uuid.UUID
		uploadID   uuid.UUID
		userID     uuid.UUID
		orgID      uuid.UUID
		datasetID  uuid.UUID
		uc         *recordingModelRegistryUsecase
		listener   interface {
			MsgType() shared.MsgType
			NewMessage() *ingestionpb.ModelArtifactIngestedEvent
			Handle(context.Context, uuid.UUID, *ingestionpb.ModelArtifactIngestedEvent) error
		}
		validArtifactEvent func(source string, artifactType string) *ingestionpb.ModelArtifactIngestedEvent
	)

	BeforeEach(func() {
		artifactID = uuid.New()
		uploadID = uuid.New()
		userID = uuid.New()
		orgID = uuid.New()
		datasetID = uuid.New()
		uc = &recordingModelRegistryUsecase{}
		listener = registrymessaging.NewModelArtifactIngestedEventListener(uc)
		validArtifactEvent = func(source string, artifactType string) *ingestionpb.ModelArtifactIngestedEvent {
			return &ingestionpb.ModelArtifactIngestedEvent{
				ArtifactId:        artifactID.String(),
				UploadId:          uploadID.String(),
				UserId:            userID.String(),
				OrgId:             orgID.String(),
				DatasetId:         datasetID.String(),
				Source:            source,
				StorageLocation:   "s3://local-dev-bucket/models/artifacts/" + artifactID.String(),
				ArtifactType:      artifactType,
				ArtifactFormat:    "GGUF_MODEL",
				ArtifactSizeBytes: 4096,
				ArtifactChecksum:  "sha256:artifact",
				ModelName:         "llama-local",
				ModelVersion:      "3",
				BaseModel:         "meta-llama/Llama-3.1-8B-Instruct",
			}
		}
	})

	It("declares the message contract it handles", func() {
		Expect(listener.MsgType()).To(Equal(shared.MsgTypeModelArtifactIngested))
		Expect(listener.NewMessage()).To(BeAssignableToTypeOf(&ingestionpb.ModelArtifactIngestedEvent{}))
	})

	It("maps Hugging Face GGUF artifacts to owned base model records", func() {
		err := listener.Handle(context.Background(), artifactID, validArtifactEvent("hugging_face", "GGUF"))

		Expect(err).NotTo(HaveOccurred())
		Expect(uc.idempotencyKey).To(Equal(uploadID))
		Expect(uc.ingestedModel.ModelID).To(Equal(artifactID))
		Expect(uc.ingestedModel.UserID).To(Equal(userID))
		Expect(uc.ingestedModel.OrgID).To(Equal(orgID))
		Expect(uc.ingestedModel.DatasetID).To(Equal(datasetID))
		Expect(uc.ingestedModel.ModelKind).To(Equal(model.ModelKindBase))
		Expect(uc.ingestedModel.Source).To(Equal(model.ModelSourceHuggingFace))
		Expect(uc.ingestedModel.SourceMetadata).To(Equal("{}"))
		Expect(uc.ingestedModel.ArtifactFormat).To(Equal("GGUF_MODEL"))
		Expect(uc.ingestedModel.ArtifactChecksum).To(Equal("sha256:artifact"))
		Expect(uc.ingestedModel.ServingLoadStatus).To(Equal(model.ModelLoadStatusNotLoaded))
	})

	It("maps LoRA adapter artifacts to fine-tuned records with adapter URI", func() {
		event := validArtifactEvent("upload", "LORA_ADAPTER")
		event.ArtifactFormat = "HF_PEFT_ADAPTER"
		event.AdapterRank = 16

		err := listener.Handle(context.Background(), artifactID, event)

		Expect(err).NotTo(HaveOccurred())
		Expect(uc.ingestedModel.ModelKind).To(Equal(model.ModelKindFineTuned))
		Expect(uc.ingestedModel.Source).To(Equal(model.ModelSourceUpload))
		Expect(uc.ingestedModel.AdapterURI).To(Equal(event.StorageLocation))
		Expect(uc.ingestedModel.AdapterRank).To(Equal(16))
	})

	It("marks LoRA adapter artifacts without rank as non-retryable", func() {
		event := validArtifactEvent("upload", "LORA_ADAPTER")
		event.ArtifactFormat = "HF_PEFT_ADAPTER"

		err := listener.Handle(context.Background(), artifactID, event)

		Expect(err).To(MatchError(ContainSubstring("adapter rank is required")))
		Expect(shared.IsNonRetryable(err)).To(BeTrue())
		Expect(uc.ingestedModel).To(BeNil())
	})

	It("maps merged model artifacts to fine-tuned records without adapter URI", func() {
		event := validArtifactEvent("upload", "MERGED_MODEL")

		err := listener.Handle(context.Background(), artifactID, event)

		Expect(err).NotTo(HaveOccurred())
		Expect(uc.ingestedModel.ModelKind).To(Equal(model.ModelKindFineTuned))
		Expect(uc.ingestedModel.AdapterURI).To(BeEmpty())
	})

	It("marks malformed artifact events as non-retryable", func() {
		event := validArtifactEvent("upload", "BASE_MODEL")
		event.ArtifactId = uuid.NewString()

		err := listener.Handle(context.Background(), artifactID, event)

		Expect(err).To(MatchError(ContainSubstring("resource key does not match artifact id")))
		Expect(shared.IsNonRetryable(err)).To(BeTrue())
		Expect(uc.ingestedModel).To(BeNil())
	})

	It("marks unsupported artifact types as non-retryable", func() {
		event := validArtifactEvent("upload", "UNKNOWN_ARTIFACT")

		err := listener.Handle(context.Background(), artifactID, event)

		Expect(err).To(MatchError(ContainSubstring("artifact type is invalid")))
		Expect(shared.IsNonRetryable(err)).To(BeTrue())
		Expect(uc.ingestedModel).To(BeNil())
	})

	It("propagates usecase errors as retryable", func() {
		expectedErr := errors.New("tenant projection not ready")
		uc.err = expectedErr

		err := listener.Handle(context.Background(), artifactID, validArtifactEvent("upload", "BASE_MODEL"))

		Expect(errors.Is(err, expectedErr)).To(BeTrue())
		Expect(shared.IsNonRetryable(err)).To(BeFalse())
	})

})
