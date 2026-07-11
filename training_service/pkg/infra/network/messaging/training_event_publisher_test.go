package messaging_test

import (
	"context"

	"training_service/pkg/domain/model"
	trainingmessaging "training_service/pkg/infra/network/messaging"

	trainingpb "lib/data_contracts_lib/training"
	shared "lib/shared_lib/messaging"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
)

type trainingPublishClientStub struct {
	topic   string
	message shared.Message
}

func (s *trainingPublishClientStub) Publish(_ context.Context, topic string, message shared.Message, payload proto.Message) error {
	s.topic = topic
	s.message = message
	return nil
}

func (s *trainingPublishClientStub) Close() {}

var _ = Describe("TrainingEventPublisher", func() {
	It("publishes completed model training facts to the training topic", func() {
		datasetID := uuid.New()
		userID := uuid.New()
		orgID := uuid.New()
		modelID := uuid.New()
		client := &trainingPublishClientStub{}
		publisher := trainingmessaging.NewTrainingEventPublisher(client, trainingmessaging.TrainingTopics{
			Training: "training",
		})

		err := publisher.PublishModelTrainingCompleted(context.Background(), &model.TrainingRunResult{
			TrainingRunID:     uuid.NewString(),
			UserID:            userID.String(),
			OrgID:             orgID.String(),
			DatasetID:         datasetID.String(),
			DatasetVersion:    "4",
			FeatureSnapshotID: uuid.NewString(),
			ModelID:           modelID.String(),
			ModelURI:          "s3://local-dev-bucket/models/run",
			ModelName:         "movie-ranker",
			ModelVersion:      "4",
			BaseModel:         "mistral-7b",
			ArtifactFormat:    "HF_PEFT_ADAPTER",
			ArtifactChecksum:  "sha256:abc",
			ArtifactSizeBytes: 128,
			AdapterURI:        "s3://local-dev-bucket/models/run",
			AdapterRank:       16,
			ServingTarget:     "vllm-local",
			ServingModel:      "movie-ranker-v4",
			ServingLoadStatus: "LOADED",
			MetricsMetadata:   `{"eval_loss":0.12}`,
			ReportURI:         "s3://local-dev-bucket/evals/run.json",
			Status:            model.TrainingRunStatusCompleted,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(client.topic).To(Equal("training"))
		Expect(client.message.ResourceKey).To(Equal(datasetID))
		Expect(client.message.MsgType).To(Equal(shared.MsgTypeModelTrainingCompleted))
		event := &trainingpb.ModelTrainingCompletedEvent{}
		Expect(proto.Unmarshal(client.message.Payload, event)).To(Succeed())
		Expect(event.UserId).To(Equal(userID.String()))
		Expect(event.OrgId).To(Equal(orgID.String()))
		Expect(event.DatasetId).To(Equal(datasetID.String()))
		Expect(event.ModelId).To(Equal(modelID.String()))
		Expect(event.ModelName).To(Equal("movie-ranker"))
		Expect(event.ArtifactLocation).To(Equal("s3://local-dev-bucket/models/run"))
		Expect(event.AdapterUri).To(Equal("s3://local-dev-bucket/models/run"))
		Expect(event.AdapterRank).To(Equal(int32(16)))
		Expect(event.ServingTarget).To(Equal("vllm-local"))
		Expect(event.ServingModel).To(Equal("movie-ranker-v4"))
		Expect(event.ServingLoadStatus).To(Equal("LOADED"))
	})

	It("publishes failed model training facts to the training topic", func() {
		datasetID := uuid.New()
		userID := uuid.New()
		orgID := uuid.New()
		modelID := uuid.New()
		client := &trainingPublishClientStub{}
		publisher := trainingmessaging.NewTrainingEventPublisher(client, trainingmessaging.TrainingTopics{
			Training: "training",
		})

		err := publisher.PublishModelTrainingFailed(context.Background(), &model.TrainingRunResult{
			TrainingRunID:     uuid.NewString(),
			UserID:            userID.String(),
			OrgID:             orgID.String(),
			DatasetID:         datasetID.String(),
			DatasetVersion:    "4",
			FeatureSnapshotID: uuid.NewString(),
			ModelID:           modelID.String(),
			ModelName:         "movie-ranker",
			ModelVersion:      "4",
			BaseModel:         "mistral-7b",
			FailureReason:     "model evaluation failed",
			Status:            model.TrainingRunStatusFailed,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(client.topic).To(Equal("training"))
		Expect(client.message.ResourceKey).To(Equal(datasetID))
		Expect(client.message.MsgType).To(Equal(shared.MsgTypeModelTrainingFailed))
		event := &trainingpb.ModelTrainingFailedEvent{}
		Expect(proto.Unmarshal(client.message.Payload, event)).To(Succeed())
		Expect(event.UserId).To(Equal(userID.String()))
		Expect(event.OrgId).To(Equal(orgID.String()))
		Expect(event.DatasetId).To(Equal(datasetID.String()))
		Expect(event.ModelId).To(Equal(modelID.String()))
		Expect(event.ModelName).To(Equal("movie-ranker"))
		Expect(event.FailureReason).To(Equal("model evaluation failed"))
	})

	It("publishes promotion report facts to the training topic", func() {
		userID := uuid.New()
		orgID := uuid.New()
		modelID := uuid.New()
		trainingRunID := uuid.New()
		client := &trainingPublishClientStub{}
		publisher := trainingmessaging.NewTrainingEventPublisher(client, trainingmessaging.TrainingTopics{
			Training: "training",
		})

		err := publisher.PublishPromotionReportReady(context.Background(), &model.PromotionReport{
			UserID:             userID.String(),
			OrgID:              orgID.String(),
			ModelID:            modelID.String(),
			TrainingRunID:      trainingRunID.String(),
			PromotionReportURI: "s3://local-dev-bucket/promotion/model.json",
			DeepchecksPassed:   true,
			EvidentlyPassed:    true,
			Deltas:             map[string]float64{"faithfulness": 0.1},
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(client.topic).To(Equal("training"))
		Expect(client.message.ResourceKey).To(Equal(modelID))
		Expect(client.message.MsgType).To(Equal(shared.MsgTypePromotionReportReady))
		event := &trainingpb.PromotionReportReadyEvent{}
		Expect(proto.Unmarshal(client.message.Payload, event)).To(Succeed())
		Expect(event.UserId).To(Equal(userID.String()))
		Expect(event.OrgId).To(Equal(orgID.String()))
		Expect(event.ModelId).To(Equal(modelID.String()))
		Expect(event.TrainingRunId).To(Equal(trainingRunID.String()))
		Expect(event.PromotionReportUri).To(Equal("s3://local-dev-bucket/promotion/model.json"))
		Expect(event.PromotionDeltas).To(MatchJSON(`{"faithfulness":0.1}`))
	})
})
