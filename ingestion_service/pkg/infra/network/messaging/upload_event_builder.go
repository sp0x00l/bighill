package messaging

import (
	"fmt"
	"time"

	"ingestion_service/pkg/domain/model"

	ingestionpb "lib/data_contracts_lib/ingestion"
	msgConn "lib/shared_lib/messaging"
	"lib/shared_lib/uuidutil"

	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

type UploadEventBuilder struct {
	topic string
}

func NewUploadEventBuilder(topic string) *UploadEventBuilder {
	log.Trace("NewUploadEventBuilder")

	return &UploadEventBuilder{topic: topic}
}

func (b *UploadEventBuilder) DatasetFileUploadedMessage(session *model.UploadSession) msgConn.OutboundMessage {
	log.Trace("UploadEventBuilder DatasetFileUploadedMessage")

	payload := mustMarshalUpload(&ingestionpb.DatasetFileUploadedEvent{
		DatasetId:         session.DatasetID.String(),
		UserId:            session.UserID.String(),
		StorageLocation:   session.StorageLocation,
		ContentType:       session.DeclaredContentType,
		FileExtension:     session.DeclaredFormat,
		TableNamespace:    session.TableNamespace,
		TableName:         session.TableName,
		TableFormat:       session.TableFormat,
		CatalogProvider:   session.CatalogProvider,
		ProcessingProfile: session.ProcessingProfile,
		SourceType:        "upload",
	})
	return msgConn.OutboundMessage{
		Topic: b.topic,
		Message: msgConn.Message{
			ResourceKey: session.DatasetID,
			MsgType:     msgConn.MsgTypeDatasetFileUploaded,
			Payload:     payload,
		},
		DispatchKey: "dataset_file_uploaded:" + session.UploadID.String(),
	}
}

func (b *UploadEventBuilder) ModelArtifactIngestedMessage(session *model.UploadSession) msgConn.OutboundMessage {
	log.Trace("UploadEventBuilder ModelArtifactIngestedMessage")

	sourceMetadata := fmt.Sprintf(
		`{"upload_id":%q,"file_name":%q,"content_type":%q,"manifest_location":%q,"hf_repo_id":%q,"hf_revision":%q,"hf_commit_sha":%q}`,
		session.UploadID.String(),
		session.FileName,
		session.DeclaredContentType,
		session.ManifestLocation,
		session.HFRepoID,
		session.HFRevision,
		session.HFCommitSHA,
	)
	payload := mustMarshalUpload(&ingestionpb.ModelArtifactIngestedEvent{
		ArtifactId:        session.ResourceID.String(),
		UploadId:          session.UploadID.String(),
		UserId:            session.UserID.String(),
		DatasetId:         uuidutil.StringOrEmpty(session.DatasetID),
		Source:            sourceOrDefault(session.Source),
		StorageLocation:   session.StorageLocation,
		ManifestLocation:  session.ManifestLocation,
		ArtifactType:      session.ArtifactType,
		ArtifactFormat:    session.DeclaredFormat,
		ArtifactSizeBytes: session.ActualSizeBytes,
		ArtifactChecksum:  session.Checksum,
		FileName:          session.FileName,
		ModelName:         session.ModelName,
		ModelVersion:      session.ModelVersion,
		BaseModel:         session.BaseModel,
		ContentType:       session.DeclaredContentType,
		SourceUri:         session.SourceURI,
		HfRepoId:          session.HFRepoID,
		HfRevision:        session.HFRevision,
		HfCommitSha:       session.HFCommitSHA,
		CreatedAt:         session.CreatedAt.Format(time.RFC3339),
		SourceMetadata:    sourceMetadata,
	})
	return msgConn.OutboundMessage{
		Topic: b.topic,
		Message: msgConn.Message{
			ResourceKey: session.ResourceID,
			MsgType:     msgConn.MsgTypeModelArtifactIngested,
			Payload:     payload,
		},
		DispatchKey: "model_artifact_ingested:" + session.UploadID.String(),
	}
}

func sourceOrDefault(value string) string {
	log.Trace("sourceOrDefault")

	if value == "" {
		return "UPLOAD"
	}
	return value
}

func mustMarshalUpload(payload proto.Message) []byte {
	log.Trace("mustMarshalUpload")

	out, err := proto.Marshal(payload)
	if err != nil {
		panic(err)
	}
	return out
}

func (b *UploadEventBuilder) UploadSessionPromotedMessage(session *model.UploadSession) msgConn.OutboundMessage {
	log.Trace("UploadEventBuilder UploadSessionPromotedMessage")

	if session.ResourceType == model.UploadResourceModelArtifact {
		return b.ModelArtifactIngestedMessage(session)
	}
	return b.DatasetFileUploadedMessage(session)
}
