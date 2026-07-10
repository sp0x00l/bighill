package messaging_test

import (
	"data_registry_service/pkg/domain/model"
	registrymessaging "data_registry_service/pkg/infra/network/messaging"
	datasetpb "lib/data_contracts_lib/data_registry"
	msgConn "lib/shared_lib/messaging"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
)

var _ = Describe("DatasetEventBuilder", func() {
	const topic = "data_registry"

	var (
		builder *registrymessaging.DatasetEventBuilder
		dataset *model.Dataset
	)

	BeforeEach(func() {
		builder = registrymessaging.NewDatasetEventBuilder(topic)
		dataset = &model.Dataset{
			ID:                       uuid.New(),
			UserID:                   uuid.New(),
			OrgID:                    uuid.New(),
			Location:                 "s3://local-dev-bucket/lakehouse/features/data.parquet",
			SourceType:               model.MongoDB,
			SourceConnectorID:        uuid.New(),
			SourceQuery:              `{"rating":{"$gte":8}}`,
			SourceDatabase:           "movies",
			SourceCollection:         "ratings",
			TableNamespace:           "features",
			TableName:                "movies",
			TableFormat:              model.Parquet,
			CatalogProvider:          model.LocalCatalog,
			ProcessingProfile:        model.TextRAGProfile,
			SchemaVersion:            2,
			SchemaMetadata:           `{"columns":[{"name":"text"}]}`,
			ProcessingState:          model.DatasetProcessingEmbeddingsMaterialized,
			DatasetVersion:           7,
			RawSnapshotID:            uuid.New(),
			FeatureSnapshotID:        uuid.New(),
			EmbeddingSnapshotID:      uuid.New(),
			VectorStore:              "pgvector",
			CollectionName:           "movies",
			EmbeddingDimensions:      384,
			EmbeddingCount:           12,
			EmbeddingStrategyVersion: "rag-v1",
			EmbeddingChunkerName:     "go-token-window",
			EmbeddingChunkerVersion:  "v1",
			EmbeddingChunkSize:       384,
			EmbeddingChunkOverlap:    64,
			EmbeddingProvider:        "ollama",
			EmbeddingModel:           "bge-small-en-v1.5",
		}
	})

	It("builds dataset created events with envelope and source fields", func() {
		outbound := builder.DatasetCreatedMessage(dataset)

		Expect(outbound.Topic).To(Equal(topic))
		Expect(outbound.DispatchKey).To(Equal("dataset_created:" + dataset.ID.String() + ":7"))
		Expect(outbound.Message.ResourceKey).To(Equal(dataset.ID))
		Expect(outbound.Message.MsgType).To(Equal(msgConn.MsgTypeDatasetCreated))

		var event datasetpb.DatasetCreatedEvent
		Expect(proto.Unmarshal(outbound.Message.Payload, &event)).To(Succeed())
		Expect(event.DatasetId).To(Equal(dataset.ID.String()))
		Expect(event.UserId).To(Equal(dataset.UserID.String()))
		Expect(event.OrgId).To(Equal(dataset.OrgID.String()))
		Expect(event.DatasetVersion).To(Equal(int32(7)))
		Expect(event.ProcessingState).To(Equal("EMBEDDINGS_MATERIALIZED"))
		Expect(event.StorageLocation).To(Equal(dataset.Location))
		Expect(event.TableNamespace).To(Equal("features"))
		Expect(event.TableName).To(Equal("movies"))
		Expect(event.TableFormat).To(Equal("PARQUET"))
		Expect(event.CatalogProvider).To(Equal("LOCAL"))
		Expect(event.ProcessingProfile).To(Equal("TEXT_RAG_PROCESSING_PROFILE"))
		Expect(event.SchemaVersion).To(Equal(int32(2)))
		Expect(event.SchemaMetadata).To(Equal(dataset.SchemaMetadata))
		Expect(event.SourceType).To(Equal(dataset.SourceType.String()))
		Expect(event.SourceConnectorId).To(Equal(dataset.SourceConnectorID.String()))
		Expect(event.SourceQuery).To(Equal(dataset.SourceQuery))
		Expect(event.SourceDatabase).To(Equal(dataset.SourceDatabase))
		Expect(event.SourceCollection).To(Equal(dataset.SourceCollection))
	})

	It("builds dataset updated events with materialization and embedding metadata", func() {
		outbound := builder.DatasetUpdatedMessage(dataset)

		Expect(outbound.Topic).To(Equal(topic))
		Expect(outbound.DispatchKey).To(Equal("dataset_updated:" + dataset.ID.String() + ":7"))
		Expect(outbound.Message.ResourceKey).To(Equal(dataset.ID))
		Expect(outbound.Message.MsgType).To(Equal(msgConn.MsgTypeDatasetUpdated))

		var event datasetpb.DatasetUpdatedEvent
		Expect(proto.Unmarshal(outbound.Message.Payload, &event)).To(Succeed())
		Expect(event.DatasetId).To(Equal(dataset.ID.String()))
		Expect(event.UserId).To(Equal(dataset.UserID.String()))
		Expect(event.OrgId).To(Equal(dataset.OrgID.String()))
		Expect(event.DatasetVersion).To(Equal(int32(7)))
		Expect(event.RawSnapshotId).To(Equal(dataset.RawSnapshotID.String()))
		Expect(event.FeatureSnapshotId).To(Equal(dataset.FeatureSnapshotID.String()))
		Expect(event.EmbeddingSnapshotId).To(Equal(dataset.EmbeddingSnapshotID.String()))
		Expect(event.VectorStore).To(Equal("pgvector"))
		Expect(event.CollectionName).To(Equal("movies"))
		Expect(event.EmbeddingDimensions).To(Equal(int32(384)))
		Expect(event.EmbeddingCount).To(Equal(int64(12)))
		Expect(event.EmbeddingStrategyVersion).To(Equal("rag-v1"))
		Expect(event.EmbeddingChunkerName).To(Equal("go-token-window"))
		Expect(event.EmbeddingChunkerVersion).To(Equal("v1"))
		Expect(event.EmbeddingChunkSize).To(Equal(int32(384)))
		Expect(event.EmbeddingChunkOverlap).To(Equal(int32(64)))
		Expect(event.EmbeddingProvider).To(Equal("ollama"))
		Expect(event.EmbeddingModel).To(Equal("bge-small-en-v1.5"))
		Expect(event.SourceConnectorId).To(Equal(dataset.SourceConnectorID.String()))
	})

	It("omits optional UUID fields with the shared empty-string representation", func() {
		dataset.SourceConnectorID = uuid.Nil
		dataset.RawSnapshotID = uuid.Nil
		dataset.FeatureSnapshotID = uuid.Nil
		dataset.EmbeddingSnapshotID = uuid.Nil

		var created datasetpb.DatasetCreatedEvent
		Expect(proto.Unmarshal(builder.DatasetCreatedMessage(dataset).Message.Payload, &created)).To(Succeed())
		Expect(created.SourceConnectorId).To(BeEmpty())

		var updated datasetpb.DatasetUpdatedEvent
		Expect(proto.Unmarshal(builder.DatasetUpdatedMessage(dataset).Message.Payload, &updated)).To(Succeed())
		Expect(updated.SourceConnectorId).To(BeEmpty())
		Expect(updated.RawSnapshotId).To(BeEmpty())
		Expect(updated.FeatureSnapshotId).To(BeEmpty())
		Expect(updated.EmbeddingSnapshotId).To(BeEmpty())
	})

	It("builds dataset deleted events", func() {
		outbound := builder.DatasetDeletedMessage(dataset.ID, dataset.UserID, dataset.OrgID)

		Expect(outbound.Topic).To(Equal(topic))
		Expect(outbound.DispatchKey).To(Equal("dataset_deleted:" + dataset.ID.String()))
		Expect(outbound.Message.ResourceKey).To(Equal(dataset.ID))
		Expect(outbound.Message.MsgType).To(Equal(msgConn.MsgTypeDatasetDeleted))

		var event datasetpb.DatasetDeletedEvent
		Expect(proto.Unmarshal(outbound.Message.Payload, &event)).To(Succeed())
		Expect(event.DatasetId).To(Equal(dataset.ID.String()))
		Expect(event.UserId).To(Equal(dataset.UserID.String()))
		Expect(event.OrgId).To(Equal(dataset.OrgID.String()))
	})
})
