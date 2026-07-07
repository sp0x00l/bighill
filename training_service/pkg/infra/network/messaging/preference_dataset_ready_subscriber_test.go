package messaging_test

import (
	"context"
	"errors"
	"testing"

	"training_service/pkg/domain"
	"training_service/pkg/domain/model"
	trainingmessaging "training_service/pkg/infra/network/messaging"

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

type recordingTrainingProfileCatalog struct {
	trainingProfileName   string
	evaluationProfileName string
	trainingProfile       model.TrainingProfile
	evaluationProfile     string
	err                   error
}

func (c *recordingTrainingProfileCatalog) ResolveTrainingProfile(_ context.Context, name string) (model.TrainingProfile, error) {
	c.trainingProfileName = name
	if c.err != nil {
		return model.TrainingProfile{}, c.err
	}
	return c.trainingProfile, nil
}

func (c *recordingTrainingProfileCatalog) ResolveEvaluationProfile(_ context.Context, name string) (string, error) {
	c.evaluationProfileName = name
	if c.err != nil {
		return "", c.err
	}
	return c.evaluationProfile, nil
}

var _ = Describe("PreferenceDatasetReadyEventListener", func() {
	It("starts DPO training from a preference dataset artifact", func() {
		datasetID := uuid.New()
		userID := uuid.New()
		orgID := uuid.New()
		modelID := uuid.New()
		preferenceDatasetID := uuid.New()
		sourceRequestID := uuid.New()
		starter := &recordingTrainingWorkflowStarter{}
		profileCatalog := trainingProfileCatalog()
		listener := trainingmessaging.NewPreferenceDatasetReadyEventListener(starter, profileCatalog, "dpo-default@v1", "dpo-eval@v1")

		err := listener.Handle(context.Background(), datasetID, &inferencepb.PreferenceDatasetReadyEvent{
			PreferenceDatasetId: preferenceDatasetID.String(),
			UserId:              userID.String(),
			OrgId:               orgID.String(),
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
		Expect(profileCatalog.trainingProfileName).To(Equal("dpo-default@v1"))
		Expect(profileCatalog.evaluationProfileName).To(Equal("dpo-eval@v1"))
		Expect(starter.calls).To(Equal(1))
		Expect(starter.request.UserID).To(Equal(userID.String()))
		Expect(starter.request.OrgID).To(Equal(orgID.String()))
		Expect(starter.request.DatasetID).To(Equal(datasetID.String()))
		Expect(starter.request.DatasetVersion).To(Equal(""))
		Expect(starter.request.FeatureSnapshotID).To(Equal(""))
		Expect(starter.request.PreferenceDatasetID).To(Equal(preferenceDatasetID.String()))
		Expect(starter.request.PreferenceDatasetURI).To(Equal("s3://local-dev-bucket/preferences/dpo.jsonl"))
		Expect(starter.request.ParentModelID).To(Equal(modelID.String()))
		Expect(starter.request.ParentModelVersion).To(Equal("7"))
		Expect(starter.request.ParentAdapterURI).To(Equal("s3://models/parent-adapter"))
		Expect(starter.request.SourceModelID).To(Equal(modelID.String()))
		Expect(starter.request.SourceArtifactURI).To(Equal("s3://models/parent-adapter"))
		Expect(starter.request.SourceModelKind).To(Equal("FINE_TUNED"))
		Expect(starter.request.ModelVersion).To(Equal("8"))
		Expect(starter.request.BaseModel).To(Equal("mistral-7b"))
		Expect(starter.request.EvaluationProfile).To(Equal(`{"dataset_mode":"heldout_preference","dataset_uri":"s3://local-dev-bucket/preferences/dpo-eval.jsonl","evaluator_name":"ragas","evaluator_version":"v1","metric_suite":"preference"}`))
		Expect(starter.request.TrainingProfile.Trainer).To(Equal("dpo"))
		Expect(starter.request.TrainingProfile.PreferenceDatasetURI).To(Equal("s3://local-dev-bucket/preferences/dpo.jsonl"))
	})

	It("does not start DPO training below the minimum example threshold", func() {
		datasetID := uuid.New()
		starter := &recordingTrainingWorkflowStarter{}
		listener := trainingmessaging.NewPreferenceDatasetReadyEventListener(starter, trainingProfileCatalog(), "dpo-default@v1", "dpo-eval@v1")

		err := listener.Handle(context.Background(), datasetID, &inferencepb.PreferenceDatasetReadyEvent{
			PreferenceDatasetId: uuid.NewString(),
			UserId:              uuid.NewString(),
			OrgId:               uuid.NewString(),
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
		listener := trainingmessaging.NewPreferenceDatasetReadyEventListener(starter, trainingProfileCatalog(), "dpo-default@v1", "dpo-eval@v1")

		err := listener.Handle(context.Background(), datasetID, &inferencepb.PreferenceDatasetReadyEvent{
			PreferenceDatasetId: uuid.NewString(),
			UserId:              uuid.NewString(),
			OrgId:               uuid.NewString(),
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

	It("treats profile catalog failures as non-retryable", func() {
		datasetID := uuid.New()
		starter := &recordingTrainingWorkflowStarter{}
		profileCatalog := trainingProfileCatalog()
		profileCatalog.err = domain.ErrValidationFailed.Extend("unknown training profile")
		listener := trainingmessaging.NewPreferenceDatasetReadyEventListener(starter, profileCatalog, "dpo-default", "dpo-eval@v1")

		err := listener.Handle(context.Background(), datasetID, &inferencepb.PreferenceDatasetReadyEvent{
			PreferenceDatasetId: uuid.NewString(),
			UserId:              uuid.NewString(),
			OrgId:               uuid.NewString(),
			DatasetId:           datasetID.String(),
			ModelId:             uuid.NewString(),
			SourceRequestId:     uuid.NewString(),
			OutputUri:           "s3://local-dev-bucket/preferences/dpo.jsonl",
			ExampleCount:        12,
			Format:              "DPO_JSONL",
			MinExamples:         10,
			Limit:               1000,
			ParentAdapterUri:    "s3://models/parent-adapter",
			ParentBaseModel:     "mistral-7b",
			ParentModelVersion:  7,
		})

		Expect(err).To(HaveOccurred())
		Expect(shared.IsNonRetryable(err)).To(BeTrue())
		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
		Expect(starter.calls).To(Equal(0))
	})
})

func trainingProfileCatalog() *recordingTrainingProfileCatalog {
	return &recordingTrainingProfileCatalog{
		trainingProfile: model.TrainingProfile{
			Name:                      "dpo-default@v1",
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
		},
		evaluationProfile: "ragas",
	}
}
