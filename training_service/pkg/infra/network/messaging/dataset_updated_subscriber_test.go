package messaging_test

import (
	"context"
	"testing"

	"training_service/pkg/domain/model"
	trainingmessaging "training_service/pkg/infra/network/messaging"

	datasetpb "lib/data_contracts_lib/data_registry"
	inferencepb "lib/data_contracts_lib/inference"
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
		profile := trainingProfile()
		profile.PreferenceDatasetURI = "s3://local-dev-bucket/preferences/{dataset_id}/preference_dataset.jsonl"
		listener := trainingmessaging.NewDatasetUpdatedEventListener(starter, "mistral-7b", profile, `{"evaluator":"ragas","dataset_uri":"s3://evals/{dataset_id}.jsonl"}`)

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
		Expect(starter.request.TrainingProfile.PreferenceDatasetURI).To(Equal("s3://local-dev-bucket/preferences/" + datasetID.String() + "/preference_dataset.jsonl"))
		Expect(starter.request.EvaluationProfile).To(ContainSubstring(`s3://evals/` + datasetID.String() + `.jsonl`))
	})

	It("ignores non-feature-ready dataset updates", func() {
		datasetID := uuid.New()
		starter := &recordingTrainingWorkflowStarter{}
		listener := trainingmessaging.NewDatasetUpdatedEventListener(starter, "mistral-7b", trainingProfile(), "smoke")

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
		listener := trainingmessaging.NewDatasetUpdatedEventListener(&recordingTrainingWorkflowStarter{}, "mistral-7b", trainingProfile(), "smoke")

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

var _ = Describe("PreferenceDatasetReadyEventListener", func() {
	It("starts DPO training from a preference dataset artifact", func() {
		datasetID := uuid.New()
		modelID := uuid.New()
		preferenceDatasetID := uuid.New()
		sourceRequestID := uuid.New()
		starter := &recordingTrainingWorkflowStarter{}
		profile := trainingProfile()
		listener := trainingmessaging.NewPreferenceDatasetReadyEventListener(starter, "mistral-7b", profile, "ragas")

		err := listener.Handle(context.Background(), datasetID, &inferencepb.PreferenceDatasetReadyEvent{
			PreferenceDatasetId: preferenceDatasetID.String(),
			DatasetId:           datasetID.String(),
			ModelId:             modelID.String(),
			SourceRequestId:     sourceRequestID.String(),
			OutputUri:           "s3://local-dev-bucket/preferences/dpo.jsonl",
			EvaluationOutputUri: "s3://local-dev-bucket/preferences/dpo-eval.jsonl",
			ExampleCount:        12,
			Format:              "DPO_JSONL",
			MinExamples:         10,
			Limit:               1000,
			ParentAdapterUri:    "s3://models/parent-adapter",
			ParentBaseModel:     "mistral-7b",
			ParentModelVersion:  7,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(starter.calls).To(Equal(1))
		Expect(starter.request.DatasetID).To(Equal(datasetID.String()))
		Expect(starter.request.DatasetVersion).To(Equal(""))
		Expect(starter.request.FeatureSnapshotID).To(Equal(""))
		Expect(starter.request.PreferenceDatasetID).To(Equal(preferenceDatasetID.String()))
		Expect(starter.request.PreferenceDatasetURI).To(Equal("s3://local-dev-bucket/preferences/dpo.jsonl"))
		Expect(starter.request.ParentModelID).To(Equal(modelID.String()))
		Expect(starter.request.ParentModelVersion).To(Equal("7"))
		Expect(starter.request.ParentAdapterURI).To(Equal("s3://models/parent-adapter"))
		Expect(starter.request.ModelVersion).To(Equal("8"))
		Expect(starter.request.BaseModel).To(Equal("mistral-7b"))
		Expect(starter.request.EvaluationProfile).To(Equal(`{"dataset_mode":"heldout_preference","dataset_uri":"s3://local-dev-bucket/preferences/dpo-eval.jsonl","evaluator_name":"ragas","evaluator_version":"v1","metric_suite":"preference"}`))
		Expect(starter.request.TrainingProfile.Trainer).To(Equal("dpo"))
		Expect(starter.request.TrainingProfile.PreferenceDatasetURI).To(Equal("s3://local-dev-bucket/preferences/dpo.jsonl"))
	})

	It("does not start DPO training below the minimum example threshold", func() {
		datasetID := uuid.New()
		starter := &recordingTrainingWorkflowStarter{}
		listener := trainingmessaging.NewPreferenceDatasetReadyEventListener(starter, "mistral-7b", trainingProfile(), "ragas")

		err := listener.Handle(context.Background(), datasetID, &inferencepb.PreferenceDatasetReadyEvent{
			PreferenceDatasetId: uuid.NewString(),
			DatasetId:           datasetID.String(),
			ModelId:             uuid.NewString(),
			SourceRequestId:     uuid.NewString(),
			OutputUri:           "s3://local-dev-bucket/preferences/dpo.jsonl",
			EvaluationOutputUri: "s3://local-dev-bucket/preferences/dpo-eval.jsonl",
			ExampleCount:        3,
			Format:              "DPO_JSONL",
			MinExamples:         10,
			Limit:               1000,
			ParentAdapterUri:    "s3://models/parent-adapter",
			ParentBaseModel:     "mistral-7b",
			ParentModelVersion:  7,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(starter.calls).To(Equal(0))
	})

	It("treats missing parent metadata as retryable", func() {
		datasetID := uuid.New()
		starter := &recordingTrainingWorkflowStarter{}
		listener := trainingmessaging.NewPreferenceDatasetReadyEventListener(starter, "mistral-7b", trainingProfile(), "ragas")

		err := listener.Handle(context.Background(), datasetID, &inferencepb.PreferenceDatasetReadyEvent{
			PreferenceDatasetId: uuid.NewString(),
			DatasetId:           datasetID.String(),
			ModelId:             uuid.NewString(),
			SourceRequestId:     uuid.NewString(),
			OutputUri:           "s3://local-dev-bucket/preferences/dpo.jsonl",
			ExampleCount:        12,
			Format:              "DPO_JSONL",
			MinExamples:         10,
			Limit:               1000,
			ParentBaseModel:     "mistral-7b",
			ParentModelVersion:  7,
		})

		Expect(err).To(HaveOccurred())
		Expect(shared.IsNonRetryable(err)).To(BeFalse())
		Expect(starter.calls).To(Equal(0))
	})
})

func trainingProfile() model.TrainingProfile {
	return model.TrainingProfile{
		Name:                      "profile-v1",
		Trainer:                   "sft",
		Adapter:                   "qlora",
		Quantization:              "4bit",
		DPOBeta:                   0.1,
		SequenceLength:            2048,
		SamplePacking:             true,
		LearningRate:              0.0002,
		Epochs:                    3,
		MicroBatchSize:            1,
		GradientAccumulationSteps: 4,
		LoRAR:                     16,
		LoRAAlpha:                 32,
	}
}
