package app_test

import (
	"context"
	"errors"

	"training_service/pkg/app"
	"training_service/pkg/domain"
	"training_service/pkg/domain/model"

	"lib/shared_lib/ctxutil"
	sharedDomain "lib/shared_lib/domain"
	"lib/shared_lib/modelstatus"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type commandWorkflowStarterStub struct {
	requests []model.TrainingRunRequest
	status   *model.TrainingRunStatusResult
	err      error
	readErr  error
}

func (s *commandWorkflowStarterStub) StartTrainingWorkflow(_ context.Context, request model.TrainingRunRequest) error {
	s.requests = append(s.requests, request)
	return s.err
}

func (s *commandWorkflowStarterStub) ReadTrainingWorkflowStatus(_ context.Context, trainingRunID string) (*model.TrainingRunStatusResult, error) {
	if s.readErr != nil {
		return nil, s.readErr
	}
	if s.status != nil {
		return s.status, nil
	}
	return &model.TrainingRunStatusResult{TrainingRunID: trainingRunID, Status: "RUNNING"}, nil
}

type datasetResolverStub struct {
	userID    uuid.UUID
	orgID     uuid.UUID
	datasetID uuid.UUID
	ref       model.MaterializedDatasetRef
	err       error
}

func (s *datasetResolverStub) ResolveMaterializedDataset(_ context.Context, userID uuid.UUID, orgID uuid.UUID, datasetID uuid.UUID) (model.MaterializedDatasetRef, error) {
	s.userID = userID
	s.orgID = orgID
	s.datasetID = datasetID
	return s.ref, s.err
}

type modelResolverStub struct {
	userID  uuid.UUID
	orgID   uuid.UUID
	modelID uuid.UUID
	ref     model.SourceModelRef
	err     error
}

func (s *modelResolverStub) ResolveTrainableModel(_ context.Context, userID uuid.UUID, orgID uuid.UUID, modelID uuid.UUID) (model.SourceModelRef, error) {
	s.userID = userID
	s.orgID = orgID
	s.modelID = modelID
	return s.ref, s.err
}

type preferenceDatasetResolverStub struct {
	userID              uuid.UUID
	orgID               uuid.UUID
	preferenceDatasetID uuid.UUID
	ref                 model.PreferenceDatasetRef
	err                 error
}

func (s *preferenceDatasetResolverStub) ResolvePreferenceDataset(_ context.Context, userID uuid.UUID, orgID uuid.UUID, preferenceDatasetID uuid.UUID) (model.PreferenceDatasetRef, error) {
	s.userID = userID
	s.orgID = orgID
	s.preferenceDatasetID = preferenceDatasetID
	return s.ref, s.err
}

var _ = Describe("TrainingCommandUsecase", func() {
	var (
		userID             uuid.UUID
		orgID              uuid.UUID
		datasetID          uuid.UUID
		sourceModelID      uuid.UUID
		preferenceID       uuid.UUID
		starter            *commandWorkflowStarterStub
		datasetResolver    *datasetResolverStub
		modelResolver      *modelResolverStub
		preferenceResolver *preferenceDatasetResolverStub
		usecase            app.TrainingCommandUsecase
	)

	BeforeEach(func() {
		userID = uuid.New()
		orgID = uuid.New()
		datasetID = uuid.New()
		sourceModelID = uuid.New()
		preferenceID = uuid.New()
		starter = &commandWorkflowStarterStub{}
		datasetResolver = &datasetResolverStub{ref: materializedDatasetRef(datasetID, userID, orgID)}
		modelResolver = &modelResolverStub{ref: baseModelRef(sourceModelID)}
		preferenceResolver = &preferenceDatasetResolverStub{ref: preferenceDatasetRef(preferenceID, datasetID, sourceModelID)}
		usecase = app.NewTrainingCommandUsecase(
			starter,
			starter,
			datasetResolver,
			modelResolver,
			trainingProfileCatalog(),
			app.WithPreferenceDatasetResolver(preferenceResolver),
		)
	})

	It("resolves inputs and starts a base-model SFT workflow", func() {
		result, err := usecase.StartTrainingRun(ctxutil.WithActorOrg(context.Background(), userID, orgID), model.StartTrainingRunCommand{
			IdempotencyKey:    uuid.New(),
			DatasetID:         datasetID,
			SourceModelID:     sourceModelID,
			TrainingProfile:   "sft-default@v1",
			EvaluationProfile: "ragas-default@v2",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(result.TrainingRunID).NotTo(BeEmpty())
		Expect(result.StatusURL).To(Equal("/v1/private/training-runs/" + result.TrainingRunID))
		Expect(datasetResolver.userID).To(Equal(userID))
		Expect(datasetResolver.orgID).To(Equal(orgID))
		Expect(datasetResolver.datasetID).To(Equal(datasetID))
		Expect(modelResolver.userID).To(Equal(userID))
		Expect(modelResolver.orgID).To(Equal(orgID))
		Expect(modelResolver.modelID).To(Equal(sourceModelID))
		Expect(starter.requests).To(HaveLen(1))
		request := starter.requests[0]
		Expect(request.UserID).To(Equal(userID.String()))
		Expect(request.OrgID).To(Equal(orgID.String()))
		Expect(request.DatasetID).To(Equal(datasetID.String()))
		Expect(request.DatasetURI).To(Equal("s3://lakehouse/features/movies.parquet"))
		Expect(request.SourceModelID).To(Equal(sourceModelID.String()))
		Expect(request.SourceArtifactURI).To(Equal("s3://models/base"))
		Expect(request.SourceModelKind).To(Equal(sharedDomain.ModelKindBase.String()))
		Expect(request.ParentModelID).To(BeEmpty())
		Expect(request.BaseModel).To(Equal("llama-3"))
		Expect(request.ModelVersion).To(Equal("1"))
		Expect(request.TrainingProfile.Name).To(Equal("sft-default@v1"))
		Expect(request.EvaluationProfile).To(Equal(`{"name":"ragas-default","version":"v2"}`))
	})

	It("populates parent fields for fine-tuned source models", func() {
		modelResolver.ref = fineTunedModelRef(sourceModelID)

		_, err := usecase.StartTrainingRun(ctxutil.WithActorOrg(context.Background(), userID, orgID), model.StartTrainingRunCommand{
			IdempotencyKey: uuid.New(),
			DatasetID:      datasetID,
			SourceModelID:  sourceModelID,
		})

		Expect(err).NotTo(HaveOccurred())
		request := starter.requests[0]
		Expect(request.ParentModelID).To(Equal(sourceModelID.String()))
		Expect(request.ParentModelVersion).To(Equal("7"))
		Expect(request.ParentAdapterURI).To(Equal("s3://models/fine-tuned/adapter"))
		Expect(request.SourceArtifactURI).To(Equal("s3://models/fine-tuned"))
		Expect(request.ModelVersion).To(Equal("8"))
	})

	It("is idempotent for the same request id", func() {
		requestID := uuid.New()
		first, err := usecase.StartTrainingRun(ctxutil.WithActorOrg(context.Background(), userID, orgID), model.StartTrainingRunCommand{
			IdempotencyKey: requestID,
			DatasetID:      datasetID,
			SourceModelID:  sourceModelID,
		})
		Expect(err).NotTo(HaveOccurred())

		second, err := usecase.StartTrainingRun(ctxutil.WithActorOrg(context.Background(), userID, orgID), model.StartTrainingRunCommand{
			IdempotencyKey: requestID,
			DatasetID:      datasetID,
			SourceModelID:  sourceModelID,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(second.TrainingRunID).To(Equal(first.TrainingRunID))
	})

	It("propagates dataset resolver failures", func() {
		datasetResolver.err = domain.ErrValidationFailed.Extend("dataset is not materialized")

		_, err := usecase.StartTrainingRun(ctxutil.WithActorOrg(context.Background(), userID, orgID), model.StartTrainingRunCommand{
			IdempotencyKey: uuid.New(),
			DatasetID:      datasetID,
			SourceModelID:  sourceModelID,
		})

		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
		Expect(starter.requests).To(BeEmpty())
	})

	It("accepts ready source models that are not currently loaded", func() {
		modelResolver.ref.ServingLoadStatus = modelstatus.ModelLoadStatusNotLoaded.String()

		_, err := usecase.StartTrainingRun(ctxutil.WithActorOrg(context.Background(), userID, orgID), model.StartTrainingRunCommand{
			IdempotencyKey: uuid.New(),
			DatasetID:      datasetID,
			SourceModelID:  sourceModelID,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(starter.requests).To(HaveLen(1))
	})

	It("propagates model resolver failures", func() {
		modelResolver.err = domain.ErrValidationFailed.Extend("source model is not ready")

		_, err := usecase.StartTrainingRun(ctxutil.WithActorOrg(context.Background(), userID, orgID), model.StartTrainingRunCommand{
			IdempotencyKey: uuid.New(),
			DatasetID:      datasetID,
			SourceModelID:  sourceModelID,
		})

		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
		Expect(starter.requests).To(BeEmpty())
	})

	It("rejects unknown profile names", func() {
		_, err := usecase.StartTrainingRun(ctxutil.WithActorOrg(context.Background(), userID, orgID), model.StartTrainingRunCommand{
			IdempotencyKey:    uuid.New(),
			DatasetID:         datasetID,
			SourceModelID:     sourceModelID,
			TrainingProfile:   "unknown@v1",
			EvaluationProfile: "ragas-default@v1",
		})

		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
		Expect(starter.requests).To(BeEmpty())
	})

	It("reads training run workflow status", func() {
		trainingRunID := uuid.New()
		starter.status = &model.TrainingRunStatusResult{TrainingRunID: trainingRunID.String(), Status: "RUNNING"}

		result, err := usecase.ReadTrainingRun(context.Background(), trainingRunID)

		Expect(err).NotTo(HaveOccurred())
		Expect(result.TrainingRunID).To(Equal(trainingRunID.String()))
		Expect(result.Status).To(Equal("RUNNING"))
	})

	It("resolves a preference dataset and starts a DPO workflow", func() {
		result, err := usecase.StartDPOTrainingRun(ctxutil.WithActorOrg(context.Background(), userID, orgID), model.StartDPOTrainingRunCommand{
			IdempotencyKey:      uuid.New(),
			PreferenceDatasetID: preferenceID,
			TrainingProfile:     "dpo-default@v1",
			EvaluationProfile:   "dpo-eval@v1",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(result.TrainingRunID).NotTo(BeEmpty())
		Expect(preferenceResolver.userID).To(Equal(userID))
		Expect(preferenceResolver.orgID).To(Equal(orgID))
		Expect(preferenceResolver.preferenceDatasetID).To(Equal(preferenceID))
		Expect(starter.requests).To(HaveLen(1))
		request := starter.requests[0]
		Expect(request.UserID).To(Equal(userID.String()))
		Expect(request.OrgID).To(Equal(orgID.String()))
		Expect(request.DatasetID).To(Equal(datasetID.String()))
		Expect(request.PreferenceDatasetID).To(Equal(preferenceID.String()))
		Expect(request.PreferenceDatasetURI).To(Equal("s3://preferences/train.jsonl"))
		Expect(request.SourceModelID).To(Equal(sourceModelID.String()))
		Expect(request.SourceArtifactURI).To(Equal("s3://models/parent"))
		Expect(request.SourceArtifactChecksum).To(Equal("sha256:parent"))
		Expect(request.ParentModelID).To(Equal(sourceModelID.String()))
		Expect(request.ParentModelVersion).To(Equal("3"))
		Expect(request.ParentAdapterURI).To(Equal(""))
		Expect(request.ModelName).To(Equal("dpo-" + sourceModelID.String()))
		Expect(request.LineageName).To(Equal("citadel-lineage"))
		Expect(request.ModelVersion).To(Equal("4"))
		Expect(request.BaseModel).To(Equal("llama-3"))
		Expect(request.TrainingProfile.Trainer).To(Equal("dpo"))
		Expect(request.TrainingProfile.PreferenceDatasetURI).To(Equal("s3://preferences/train.jsonl"))
		Expect(request.EvaluationProfile).To(ContainSubstring(`"dataset_uri":"s3://preferences/eval.jsonl"`))
	})
})

func materializedDatasetRef(datasetID uuid.UUID, userID uuid.UUID, orgID uuid.UUID) model.MaterializedDatasetRef {
	return model.MaterializedDatasetRef{
		DatasetID:         datasetID.String(),
		UserID:            userID.String(),
		OrgID:             orgID.String(),
		DatasetVersion:    "4",
		FeatureSnapshotID: uuid.NewString(),
		DatasetURI:        "s3://lakehouse/features/movies.parquet",
		TableName:         "movies",
		TableFormat:       "PARQUET",
		ProcessingState:   "FEATURE_MATERIALIZED",
	}
}

func baseModelRef(modelID uuid.UUID) model.SourceModelRef {
	return model.SourceModelRef{
		ModelID:           modelID.String(),
		ModelKind:         sharedDomain.ModelKindBase.String(),
		Name:              "llama-3",
		ModelVersion:      1,
		BaseModel:         "llama-3",
		ArtifactLocation:  "s3://models/base",
		ArtifactChecksum:  "sha256:base",
		ServingLoadStatus: modelstatus.ModelLoadStatusLoaded.String(),
		Status:            "READY",
	}
}

func fineTunedModelRef(modelID uuid.UUID) model.SourceModelRef {
	ref := baseModelRef(modelID)
	ref.ModelKind = sharedDomain.ModelKindFineTuned.String()
	ref.Name = "movies-ranker"
	ref.ModelVersion = 7
	ref.BaseModel = "llama-3"
	ref.ArtifactLocation = "s3://models/fine-tuned"
	ref.AdapterURI = "s3://models/fine-tuned/adapter"
	return ref
}

func preferenceDatasetRef(preferenceID uuid.UUID, datasetID uuid.UUID, modelID uuid.UUID) model.PreferenceDatasetRef {
	return model.PreferenceDatasetRef{
		PreferenceDatasetID:    preferenceID.String(),
		DatasetID:              datasetID.String(),
		DatasetIDs:             []string{datasetID.String()},
		ModelID:                modelID.String(),
		ParentModelKind:        sharedDomain.ModelKindBase.String(),
		ParentArtifactURI:      "s3://models/parent",
		ParentArtifactChecksum: "sha256:parent",
		ParentBaseModel:        "llama-3",
		ParentModelName:        "citadel-rag",
		ParentLineageName:      "citadel-lineage",
		ParentModelVersion:     3,
		OutputURI:              "s3://preferences/train.jsonl",
		EvaluationOutputURI:    "s3://preferences/eval.jsonl",
		ExampleCount:           4,
		IntegrityKey:           "sha256:pref",
	}
}

func trainingCommandProfile() model.TrainingProfile {
	return model.TrainingProfile{
		Name:                      "sft-default@v1",
		Trainer:                   "sft",
		Adapter:                   "qlora",
		Quantization:              "4bit",
		SequenceLength:            2048,
		LearningRate:              0.0002,
		Epochs:                    3,
		MicroBatchSize:            1,
		GradientAccumulationSteps: 4,
		LoRAR:                     16,
		LoRAAlpha:                 32,
	}
}

func trainingProfileCatalog() app.TrainingProfileCatalog {
	return app.NewStaticTrainingProfileCatalog(
		[]model.TrainingProfile{trainingCommandProfile(), dpoTrainingProfile()},
		"sft-default@v1",
		map[string]string{
			"ragas-default@v1": `{"name":"ragas-default","version":"v1"}`,
			"ragas-default@v2": `{"name":"ragas-default","version":"v2"}`,
			"dpo-eval@v1":      `{"name":"dpo-eval","version":"v1"}`,
		},
		"ragas-default@v1",
	)
}

func dpoTrainingProfile() model.TrainingProfile {
	profile := trainingCommandProfile()
	profile.Name = "dpo-default@v1"
	profile.Trainer = "dpo"
	profile.PreferenceDatasetURI = ""
	return profile
}
