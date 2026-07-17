package messaging

import (
	"fmt"

	"data_registry_service/pkg/domain/model"
	datasetpb "lib/data_contracts_lib/data_registry"
	msgConn "lib/shared_lib/messaging"
	"lib/shared_lib/uuidutil"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

type DatasetEventBuilder struct {
	topic string
}

func NewDatasetEventBuilder(topic string) *DatasetEventBuilder {
	log.Trace("NewDatasetEventBuilder")

	return &DatasetEventBuilder{topic: topic}
}

func (b *DatasetEventBuilder) DatasetUpdatedMessage(dataset *model.Dataset) msgConn.OutboundMessage {
	log.Trace("DatasetEventBuilder DatasetUpdatedMessage")

	payload := mustMarshalDataset(&datasetpb.DatasetUpdatedEvent{
		DatasetId:                dataset.ID.String(),
		UserId:                   dataset.UserID.String(),
		OrgId:                    dataset.OrgID.String(),
		DatasetVersion:           int32(dataset.DatasetVersion),
		ProcessingState:          dataset.ProcessingState.String(),
		StorageLocation:          dataset.Location,
		TableNamespace:           dataset.TableNamespace,
		TableName:                dataset.TableName,
		TableFormat:              dataset.TableFormat.String(),
		CatalogProvider:          dataset.CatalogProvider.String(),
		ProcessingProfile:        dataset.ProcessingProfile.String(),
		SchemaVersion:            int32(dataset.SchemaVersion),
		SchemaMetadata:           dataset.SchemaMetadata,
		RawSnapshotId:            uuidutil.StringOrEmpty(dataset.RawSnapshotID),
		FeatureSnapshotId:        uuidutil.StringOrEmpty(dataset.FeatureSnapshotID),
		EmbeddingSnapshotId:      uuidutil.StringOrEmpty(dataset.EmbeddingSnapshotID),
		VectorStore:              dataset.VectorStore,
		CollectionName:           dataset.CollectionName,
		EmbeddingDimensions:      int32(dataset.EmbeddingDimensions),
		EmbeddingCount:           dataset.EmbeddingCount,
		EmbeddingStrategyVersion: dataset.EmbeddingStrategyVersion,
		EmbeddingChunkerName:     dataset.EmbeddingChunkerName,
		EmbeddingChunkerVersion:  dataset.EmbeddingChunkerVersion,
		EmbeddingChunkSize:       int32(dataset.EmbeddingChunkSize),
		EmbeddingChunkOverlap:    int32(dataset.EmbeddingChunkOverlap),
		EmbeddingProvider:        dataset.EmbeddingProvider,
		EmbeddingModel:           dataset.EmbeddingModel,
		GraphSnapshotId:          uuidutil.StringOrEmpty(dataset.GraphSnapshotID),
		GraphProvenanceHash:      dataset.GraphProvenanceHash,
		GraphNodeCount:           dataset.GraphNodeCount,
		GraphEdgeCount:           dataset.GraphEdgeCount,
		SourceType:               datasetSourceType(dataset),
		SourceConnectorId:        uuidutil.StringOrEmpty(dataset.SourceConnectorID),
		SourceQuery:              dataset.SourceQuery,
		SourceDatabase:           dataset.SourceDatabase,
		SourceCollection:         dataset.SourceCollection,
	})
	return msgConn.OutboundMessage{
		Topic: b.topic,
		Message: msgConn.Message{
			ResourceKey: dataset.ID,
			MsgType:     msgConn.MsgTypeDatasetUpdated,
			Payload:     payload,
		},
		DispatchKey: fmt.Sprintf("dataset_updated:%s:%d", dataset.ID, dataset.DatasetVersion),
	}
}

func (b *DatasetEventBuilder) DatasetCreatedMessage(dataset *model.Dataset) msgConn.OutboundMessage {
	log.Trace("DatasetEventBuilder DatasetCreatedMessage")

	payload := mustMarshalDataset(&datasetpb.DatasetCreatedEvent{
		DatasetId:         dataset.ID.String(),
		UserId:            dataset.UserID.String(),
		OrgId:             dataset.OrgID.String(),
		DatasetVersion:    int32(dataset.DatasetVersion),
		ProcessingState:   dataset.ProcessingState.String(),
		StorageLocation:   dataset.Location,
		TableNamespace:    dataset.TableNamespace,
		TableName:         dataset.TableName,
		TableFormat:       dataset.TableFormat.String(),
		CatalogProvider:   dataset.CatalogProvider.String(),
		ProcessingProfile: dataset.ProcessingProfile.String(),
		SchemaVersion:     int32(dataset.SchemaVersion),
		SchemaMetadata:    dataset.SchemaMetadata,
		SourceType:        datasetSourceType(dataset),
		SourceConnectorId: uuidutil.StringOrEmpty(dataset.SourceConnectorID),
		SourceQuery:       dataset.SourceQuery,
		SourceDatabase:    dataset.SourceDatabase,
		SourceCollection:  dataset.SourceCollection,
	})
	return msgConn.OutboundMessage{
		Topic: b.topic,
		Message: msgConn.Message{
			ResourceKey: dataset.ID,
			MsgType:     msgConn.MsgTypeDatasetCreated,
			Payload:     payload,
		},
		DispatchKey: fmt.Sprintf("dataset_created:%s:%d", dataset.ID, dataset.DatasetVersion),
	}
}

func (b *DatasetEventBuilder) DatasetDeletedMessage(datasetID uuid.UUID, userID uuid.UUID, orgID uuid.UUID) msgConn.OutboundMessage {
	log.Trace("DatasetEventBuilder DatasetDeletedMessage")

	payload := mustMarshalDataset(&datasetpb.DatasetDeletedEvent{
		DatasetId: datasetID.String(),
		UserId:    userID.String(),
		OrgId:     orgID.String(),
	})
	return msgConn.OutboundMessage{
		Topic: b.topic,
		Message: msgConn.Message{
			ResourceKey: datasetID,
			MsgType:     msgConn.MsgTypeDatasetDeleted,
			Payload:     payload,
		},
		DispatchKey: fmt.Sprintf("dataset_deleted:%s", datasetID),
	}
}

func datasetSourceType(dataset *model.Dataset) string {
	log.Trace("datasetSourceType")

	if dataset == nil || dataset.SourceConnectorID == uuid.Nil {
		return ""
	}
	return dataset.SourceType.String()
}

func mustMarshalDataset(payload proto.Message) []byte {
	log.Trace("mustMarshalDataset")

	out, err := proto.Marshal(payload)
	if err != nil {
		log.Fatalf("marshal dataset event: %v", err)
	}
	return out
}
