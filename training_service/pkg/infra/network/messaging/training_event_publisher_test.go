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
	payload proto.Message
}

func (s *trainingPublishClientStub) Publish(_ context.Context, topic string, message shared.Message, payload proto.Message) error {
	s.topic = topic
	s.message = message
	s.payload = payload
	return nil
}

func (s *trainingPublishClientStub) Close() {}

var _ = Describe("TrainingEventPublisher", func() {
	It("publishes completed model training facts to the training topic", func() {
		datasetID := uuid.New()
		modelID := uuid.New()
		client := &trainingPublishClientStub{}
		publisher := trainingmessaging.NewTrainingEventPublisher(client, trainingmessaging.TrainingTopics{
			Training: "training",
		})

		err := publisher.PublishModelTrainingCompleted(context.Background(), &model.TrainingRunResult{
			TrainingRunID:     uuid.NewString(),
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
		event, ok := client.payload.(*trainingpb.ModelTrainingCompletedEvent)
		Expect(ok).To(BeTrue())
		Expect(event.DatasetId).To(Equal(datasetID.String()))
		Expect(event.ModelId).To(Equal(modelID.String()))
		Expect(event.ModelName).To(Equal("movie-ranker"))
		Expect(event.ArtifactLocation).To(Equal("s3://local-dev-bucket/models/run"))
		Expect(event.AdapterUri).To(Equal("s3://local-dev-bucket/models/run"))
		Expect(event.ServingTarget).To(Equal("vllm-local"))
		Expect(event.ServingModel).To(Equal("movie-ranker-v4"))
		Expect(event.ServingLoadStatus).To(Equal("LOADED"))
	})

	It("publishes failed model training facts to the training topic", func() {
		datasetID := uuid.New()
		modelID := uuid.New()
		client := &trainingPublishClientStub{}
		publisher := trainingmessaging.NewTrainingEventPublisher(client, trainingmessaging.TrainingTopics{
			Training: "training",
		})

		err := publisher.PublishModelTrainingFailed(context.Background(), &model.TrainingRunResult{
			TrainingRunID:     uuid.NewString(),
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
		event, ok := client.payload.(*trainingpb.ModelTrainingFailedEvent)
		Expect(ok).To(BeTrue())
		Expect(event.DatasetId).To(Equal(datasetID.String()))
		Expect(event.ModelId).To(Equal(modelID.String()))
		Expect(event.ModelName).To(Equal("movie-ranker"))
		Expect(event.FailureReason).To(Equal("model evaluation failed"))
	})
})
