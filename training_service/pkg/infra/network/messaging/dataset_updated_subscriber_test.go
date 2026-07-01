package messaging_test

import (
	"context"
	"testing"

	"training_service/pkg/domain/model"
	trainingmessaging "training_service/pkg/infra/network/messaging"

	datasetpb "lib/data_contracts_lib/dataset"
	shared "lib/shared_lib/messaging"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestMessaging(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Training service messaging unit test suite")
}

type recordingTrainingWorkflowStarter struct {
	request model.TrainingRunRequest
	err     error
	calls   int
}

func (s *recordingTrainingWorkflowStarter) StartTrainingWorkflow(_ context.Context, request model.TrainingRunRequest) error {
	s.request = request
	s.calls++
	return s.err
}

var _ = Describe("DatasetUpdatedEventListener", func() {
	It("starts training when a parquet feature snapshot is ready", func() {
		datasetID := uuid.New()
		featureSnapshotID := uuid.New()
		starter := &recordingTrainingWorkflowStarter{}
		listener := trainingmessaging.NewDatasetUpdatedEventListener(starter, "mistral-7b")

		err := listener.Handle(context.Background(), datasetID, &datasetpb.DatasetUpdatedEvent{
			DatasetId:         datasetID.String(),
			UserId:            uuid.NewString(),
			DatasetVersion:    4,
			ProcessingState:   "FEATURE_MATERIALIZED",
			StorageLocation:   "s3://local-dev-bucket/features/data.parquet",
			TableName:         "movie_features",
			TableFormat:       "PARQUET",
			FeatureSnapshotId: featureSnapshotID.String(),
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(starter.calls).To(Equal(1))
		Expect(starter.request.DatasetID).To(Equal(datasetID.String()))
		Expect(starter.request.DatasetVersion).To(Equal("4"))
		Expect(starter.request.FeatureSnapshotID).To(Equal(featureSnapshotID.String()))
		Expect(starter.request.ModelName).To(Equal("movie_features"))
		Expect(starter.request.ModelVersion).To(Equal("4"))
		Expect(starter.request.BaseModel).To(Equal("mistral-7b"))
	})

	It("ignores non-feature-ready dataset updates", func() {
		datasetID := uuid.New()
		starter := &recordingTrainingWorkflowStarter{}
		listener := trainingmessaging.NewDatasetUpdatedEventListener(starter, "mistral-7b")

		err := listener.Handle(context.Background(), datasetID, &datasetpb.DatasetUpdatedEvent{
			DatasetId:       datasetID.String(),
			UserId:          uuid.NewString(),
			DatasetVersion:  2,
			ProcessingState: "RAW_MATERIALIZED",
			TableFormat:     "PARQUET",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(starter.calls).To(Equal(0))
	})

	It("rejects ready non-parquet dataset updates", func() {
		datasetID := uuid.New()
		listener := trainingmessaging.NewDatasetUpdatedEventListener(&recordingTrainingWorkflowStarter{}, "mistral-7b")

		err := listener.Handle(context.Background(), datasetID, &datasetpb.DatasetUpdatedEvent{
			DatasetId:         datasetID.String(),
			UserId:            uuid.NewString(),
			DatasetVersion:    2,
			ProcessingState:   "FEATURE_MATERIALIZED",
			TableFormat:       "ICEBERG",
			FeatureSnapshotId: uuid.NewString(),
		})

		Expect(err).To(HaveOccurred())
		Expect(shared.IsNonRetryable(err)).To(BeTrue())
	})
})
