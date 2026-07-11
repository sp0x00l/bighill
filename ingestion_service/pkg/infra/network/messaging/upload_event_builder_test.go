package messaging_test

import (
	"encoding/json"
	"time"

	"ingestion_service/pkg/domain/model"
	ingestionmessaging "ingestion_service/pkg/infra/network/messaging"
	ingestionpb "lib/data_contracts_lib/ingestion"
	shared "lib/shared_lib/messaging"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
)

var _ = Describe("UploadEventBuilder", func() {
	const topic = "ingestion"

	var (
		builder *ingestionmessaging.UploadEventBuilder
		session *model.UploadSession
	)

	BeforeEach(func() {
		builder = ingestionmessaging.NewUploadEventBuilder(topic)
		session = &model.UploadSession{
			UploadID:            uuid.New(),
			ResourceType:        model.UploadResourceDataFile,
			ResourceID:          uuid.New(),
			DatasetID:           uuid.New(),
			UserID:              uuid.New(),
			OrgID:               uuid.New(),
			FileName:            "movies.csv",
			StorageLocation:     "s3://local-dev-bucket/uploads/movies.csv",
			ManifestLocation:    "s3://local-dev-bucket/manifests/model.json",
			DeclaredFormat:      "csv",
			DeclaredContentType: "text/csv",
			ActualSizeBytes:     1024,
			Checksum:            "sha256:abc",
			TableNamespace:      "raw",
			TableName:           "movies",
			TableFormat:         "PARQUET",
			CatalogProvider:     "LOCAL",
			ProcessingProfile:   "TEXT_RAG_PROCESSING_PROFILE",
			ArtifactType:        "MODEL",
			ModelName:           "movie-ranker",
			ModelVersion:        "1",
			BaseModel:           "llama3",
			AdapterRank:         16,
			SourceURI:           "hf://repo/model",
			HFRepoID:            "QuantFactory/Meta-Llama-3-8B-Instruct-GGUF",
			HFRevision:          "main",
			HFCommitSHA:         "abc123",
			CreatedAt:           time.Date(2026, 7, 10, 8, 0, 0, 0, time.UTC),
		}
	})

	It("builds dataset file uploaded events", func() {
		outbound := builder.DatasetFileUploadedMessage(session)

		Expect(outbound.Topic).To(Equal(topic))
		Expect(outbound.DispatchKey).To(Equal("dataset_file_uploaded:" + session.UploadID.String()))
		Expect(outbound.Message.ResourceKey).To(Equal(session.DatasetID))
		Expect(outbound.Message.MsgType).To(Equal(shared.MsgTypeDatasetFileUploaded))

		var event ingestionpb.DatasetFileUploadedEvent
		Expect(proto.Unmarshal(outbound.Message.Payload, &event)).To(Succeed())
		Expect(event.DatasetId).To(Equal(session.DatasetID.String()))
		Expect(event.UserId).To(Equal(session.UserID.String()))
		Expect(event.OrgId).To(Equal(session.OrgID.String()))
		Expect(event.StorageLocation).To(Equal(session.StorageLocation))
		Expect(event.ContentType).To(Equal("text/csv"))
		Expect(event.FileExtension).To(Equal("csv"))
		Expect(event.TableNamespace).To(Equal("raw"))
		Expect(event.TableName).To(Equal("movies"))
		Expect(event.TableFormat).To(Equal("PARQUET"))
		Expect(event.CatalogProvider).To(Equal("LOCAL"))
		Expect(event.ProcessingProfile).To(Equal("TEXT_RAG_PROCESSING_PROFILE"))
		Expect(event.SourceType).To(Equal("upload"))
	})

	It("builds model artifact ingested events with source metadata", func() {
		session.ResourceType = model.UploadResourceModelArtifact
		session.Source = ""

		outbound := builder.ModelArtifactIngestedMessage(session)

		Expect(outbound.Topic).To(Equal(topic))
		Expect(outbound.DispatchKey).To(Equal("model_artifact_ingested:" + session.UploadID.String()))
		Expect(outbound.Message.ResourceKey).To(Equal(session.ResourceID))
		Expect(outbound.Message.MsgType).To(Equal(shared.MsgTypeModelArtifactIngested))

		var event ingestionpb.ModelArtifactIngestedEvent
		Expect(proto.Unmarshal(outbound.Message.Payload, &event)).To(Succeed())
		Expect(event.ArtifactId).To(Equal(session.ResourceID.String()))
		Expect(event.UploadId).To(Equal(session.UploadID.String()))
		Expect(event.UserId).To(Equal(session.UserID.String()))
		Expect(event.OrgId).To(Equal(session.OrgID.String()))
		Expect(event.DatasetId).To(Equal(session.DatasetID.String()))
		Expect(event.Source).To(Equal("UPLOAD"))
		Expect(event.StorageLocation).To(Equal(session.StorageLocation))
		Expect(event.ManifestLocation).To(Equal(session.ManifestLocation))
		Expect(event.ArtifactType).To(Equal("MODEL"))
		Expect(event.ArtifactFormat).To(Equal("csv"))
		Expect(event.ArtifactSizeBytes).To(Equal(int64(1024)))
		Expect(event.ArtifactChecksum).To(Equal("sha256:abc"))
		Expect(event.FileName).To(Equal("movies.csv"))
		Expect(event.ModelName).To(Equal("movie-ranker"))
		Expect(event.ModelVersion).To(Equal("1"))
		Expect(event.BaseModel).To(Equal("llama3"))
		Expect(event.AdapterRank).To(Equal(int32(16)))
		Expect(event.HfRepoId).To(Equal(session.HFRepoID))
		Expect(event.HfRevision).To(Equal("main"))
		Expect(event.HfCommitSha).To(Equal("abc123"))
		Expect(event.CreatedAt).To(Equal("2026-07-10T08:00:00Z"))

		var metadata map[string]string
		Expect(json.Unmarshal([]byte(event.SourceMetadata), &metadata)).To(Succeed())
		Expect(metadata["upload_id"]).To(Equal(session.UploadID.String()))
		Expect(metadata["manifest_location"]).To(Equal(session.ManifestLocation))
		Expect(metadata["hf_repo_id"]).To(Equal(session.HFRepoID))
	})

	It("omits model artifact dataset id when the upload is not dataset-scoped", func() {
		session.DatasetID = uuid.Nil

		var event ingestionpb.ModelArtifactIngestedEvent
		Expect(proto.Unmarshal(builder.ModelArtifactIngestedMessage(session).Message.Payload, &event)).To(Succeed())
		Expect(event.DatasetId).To(BeEmpty())
	})

	It("routes promoted sessions by resource type", func() {
		dataEvent := builder.UploadSessionPromotedMessage(session)
		Expect(dataEvent.Message.MsgType).To(Equal(shared.MsgTypeDatasetFileUploaded))

		session.ResourceType = model.UploadResourceModelArtifact
		modelEvent := builder.UploadSessionPromotedMessage(session)
		Expect(modelEvent.Message.MsgType).To(Equal(shared.MsgTypeModelArtifactIngested))
	})
})
