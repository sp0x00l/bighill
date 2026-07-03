package test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	inferenceapp "inference_service/pkg/app"
	inferencemodel "inference_service/pkg/domain/model"
	inferencemessaging "inference_service/pkg/infra/network/messaging"
	registryapp "model_registry_service/pkg/app"
	registrydomain "model_registry_service/pkg/domain"
	registrymodel "model_registry_service/pkg/domain/model"
	registryk8s "model_registry_service/pkg/infra/network/k8s"
	registrymessaging "model_registry_service/pkg/infra/network/messaging"
	servingapp "model_serving_service/pkg/app"
	servingk8s "model_serving_service/pkg/infra/network/k8s"
	trainingapp "training_service/pkg/app"
	trainingmodel "training_service/pkg/domain/model"
	trainingmessaging "training_service/pkg/infra/network/messaging"
	"training_service/pkg/infra/temporalworker"

	modelregistrypb "lib/data_contracts_lib/model_registry"
	trainingpb "lib/data_contracts_lib/training"
	sharedmessaging "lib/shared_lib/messaging"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"google.golang.org/protobuf/proto"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	k8sfake "k8s.io/client-go/dynamic/fake"
)

const (
	e2eNamespace       = "default"
	e2eServedModelsGVR = "servedmodels"
)

var (
	servedModelGVR = schema.GroupVersionResource{Group: "serving.bighill.io", Version: "v1alpha1", Resource: e2eServedModelsGVR}
	deploymentGVR  = schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
)

type testFailureReporter interface {
	Helper()
	Fatal(args ...any)
	Fatalf(format string, args ...any)
}

var _ = Describe("Full ML loop", func() {
	It("runs training, serving reconciliation, inference, and feedback through service seams", func() {
		t := GinkgoT()
		ctx := context.Background()

		datasetID := uuid.New()
		trainingRunID := uuid.New()
		featureSnapshotID := uuid.New()
		embeddingSnapshotID := uuid.New()
		userID := uuid.New()
		servingModel := "rag-adapter-v1"

		trainingEvent := runTrainingWorkflow(t, trainingRunID, datasetID, featureSnapshotID, servingModel)

		k8sClient := newE2EK8sClient()
		registryRepo := newRegistryMemoryRepository()
		servedModelAdapter, err := registryk8s.NewServedModelAdapterWithClient(servedModelConfig(), k8sClient)
		Expect(err).NotTo(HaveOccurred())
		registryUsecase := registryapp.NewModelRegistryUsecase(
			registryRepo,
			registryapp.WithModelServingDeployer(servedModelAdapter),
		)

		trainingListener := registrymessaging.NewModelTrainingCompletedEventListener(registryUsecase)
		Expect(trainingListener.Handle(ctx, datasetID, trainingEvent)).To(Succeed())

		modelID := trainingRunID
		registeredModel, err := registryRepo.ReadByID(ctx, modelID)
		Expect(err).NotTo(HaveOccurred())
		Expect(registeredModel.Status).To(Equal(registrymodel.ModelStatusEvaluated))
		Expect(registeredModel.ServingLoadStatus).To(Equal(registrymodel.ModelLoadStatusNotLoaded))

		vllmTarget := "http://served-model.local"
		vllmHTTP := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.Method != http.MethodGet || req.URL.String() != vllmTarget+"/v1/models" {
				return nil, fmt.Errorf("unexpected vllm request %s %s", req.Method, req.URL.String())
			}
			return httpJSON(http.StatusOK, map[string]any{
				"data": []map[string]string{{"id": servingModel}},
			}), nil
		})}
		store, err := servingk8s.NewServedModelStore(servingk8s.ServedModelStoreConfig{
			Namespace: e2eNamespace,
			Group:     servedModelGVR.Group,
			Version:   servedModelGVR.Version,
			Resource:  servedModelGVR.Resource,
		}, k8sClient)
		Expect(err).NotTo(HaveOccurred())
		runtime, err := servingk8s.NewVLLMRuntime(servingk8s.VLLMRuntimeConfig{
			Namespace:  e2eNamespace,
			Image:      "vllm:test",
			Replicas:   1,
			Port:       8000,
			HTTPClient: vllmHTTP,
		}, k8sClient)
		Expect(err).NotTo(HaveOccurred())
		controller := servingk8s.NewServedModelController(
			store,
			servingapp.NewServedModelReconciler(runtime, store),
			time.Second,
		)

		Expect(controller.ProcessOnce(ctx)).To(Succeed())
		servedModels, err := store.List(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(servedModels).To(HaveLen(1))
		workloadName := servingk8s.WorkloadName(servedModels[0])
		markDeploymentReady(t, ctx, k8sClient, workloadName)

		Expect(controller.ProcessOnce(ctx)).To(Succeed())
		assertServedModelStatus(t, ctx, k8sClient, registrymodel.ModelLoadStatusLoaded.String(), servingModel)

		statusObserver, err := registryk8s.NewServedModelStatusObserver(servedModelAdapter, registryUsecase, time.Second)
		Expect(err).NotTo(HaveOccurred())
		Expect(statusObserver.ProcessOnce(ctx)).To(Succeed())
		readyModel, err := registryRepo.ReadByID(ctx, modelID)
		Expect(err).NotTo(HaveOccurred())
		Expect(readyModel.Status).To(Equal(registrymodel.ModelStatusReady))
		Expect(readyModel.ServingLoadStatus).To(Equal(registrymodel.ModelLoadStatusLoaded))
		Expect(readyModel.ServingModel).To(Equal(servingModel))

		inferenceUsecase, inferenceRequests, feedbacks := newInferenceUsecase(datasetID, userID, embeddingSnapshotID)
		modelEvent := modelUpdatedEvent(readyModel)
		modelListener := inferencemessaging.NewModelUpdatedEventListener(inferenceUsecase)
		Expect(modelListener.Handle(ctx, readyModel.ModelID, modelEvent)).To(Succeed())

		requestID := uuid.New()
		response, err := inferenceUsecase.Generate(ctx, inferencemodel.GenerateRequest{
			RequestID: requestID,
			DatasetID: datasetID,
			ModelID:   readyModel.ModelID,
			QueryText: "What changed in the dataset?",
			TopK:      2,
			MetadataFilters: map[string]string{
				"source": "e2e",
			},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(response.ModelID).To(Equal(readyModel.ModelID))
		Expect(response.Answer).To(Equal("generated from " + servingModel))
		Expect(response.Contexts).To(HaveLen(2))
		Expect(inferenceRequests.records).To(HaveLen(1))
		Expect(inferenceRequests.records[0].Status).To(Equal(inferencemodel.InferenceRequestStatusCompleted))

		feedbackID := uuid.New()
		_, err = inferenceUsecase.RecordFeedback(ctx, &inferencemodel.InferenceFeedback{
			FeedbackID: feedbackID,
			RequestID:  requestID,
			UserID:     userID,
			Accepted:   true,
			Rating:     5,
			Comment:    "good context",
		}, feedbackID)
		Expect(err).NotTo(HaveOccurred())
		Expect(feedbacks.records).To(HaveLen(1))
	})
})

func runTrainingWorkflow(t testFailureReporter, trainingRunID uuid.UUID, datasetID uuid.UUID, featureSnapshotID uuid.UUID, servingModel string) *trainingpb.ModelTrainingCompletedEvent {
	t.Helper()

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	publisher := &capturingPublisher{}
	activities := temporalworker.NewTrainingActivities(
		trainingmessaging.NewTrainingEventPublisher(publisher, trainingmessaging.TrainingTopics{Training: "training"}),
		temporalworker.WithExecutor(&fakeTrainingExecutor{}),
		temporalworker.WithModelURIPrefix("s3://models"),
		temporalworker.WithEvaluationURIPrefix("s3://evaluations"),
		temporalworker.WithServingConfig("http://served-model.local", servingModel, registrymodel.ModelLoadStatusNotLoaded.String()),
	)
	env.RegisterActivityWithOptions(activities.PrepareTrainingDataset, activity.RegisterOptions{Name: trainingapp.PrepareTrainingDatasetActivity})
	env.RegisterActivityWithOptions(activities.RunTrainingJob, activity.RegisterOptions{Name: trainingapp.RunTrainingJobActivity})
	env.RegisterActivityWithOptions(activities.EvaluateTrainedModel, activity.RegisterOptions{Name: trainingapp.EvaluateTrainedModelActivity})
	env.RegisterActivityWithOptions(activities.PublishModelTrainingCompleted, activity.RegisterOptions{Name: trainingapp.PublishModelTrainingCompletedActivity})
	env.RegisterActivityWithOptions(activities.PublishModelTrainingFailed, activity.RegisterOptions{Name: trainingapp.PublishModelTrainingFailedActivity})

	request := trainingmodel.TrainingRunRequest{
		TrainingRunID:     trainingRunID.String(),
		DatasetID:         datasetID.String(),
		DatasetVersion:    "1",
		FeatureSnapshotID: featureSnapshotID.String(),
		ModelName:         "rag-adapter",
		ModelVersion:      "1",
		BaseModel:         "mistral-7b",
		EvaluationProfile: "smoke",
		TrainingProfile: trainingmodel.TrainingProfile{
			Name:                      "qlora-smoke",
			Trainer:                   "sft",
			Adapter:                   "qlora",
			Quantization:              "4bit",
			SequenceLength:            2048,
			SamplePacking:             true,
			LearningRate:              0.0002,
			Epochs:                    1,
			MicroBatchSize:            1,
			GradientAccumulationSteps: 4,
			LoRAR:                     16,
			LoRAAlpha:                 32,
		},
	}
	env.ExecuteWorkflow(trainingapp.TrainModelWorkflow, request)
	if !env.IsWorkflowCompleted() {
		t.Fatal("training workflow did not complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("training workflow failed: %v", err)
	}
	var result trainingmodel.TrainingRunResult
	if err := env.GetWorkflowResult(&result); err != nil {
		t.Fatalf("read training workflow result: %v", err)
	}
	if result.Status != trainingmodel.TrainingRunStatusCompleted {
		t.Fatalf("training workflow status = %s", result.Status)
	}
	if len(publisher.messages) != 1 {
		t.Fatalf("expected one training fact, got %d", len(publisher.messages))
	}
	if publisher.messages[0].message.MsgType != sharedmessaging.MsgTypeModelTrainingCompleted {
		t.Fatalf("expected model_training_completed, got %s", publisher.messages[0].message.MsgType)
	}
	completed, ok := publisher.messages[0].payload.(*trainingpb.ModelTrainingCompletedEvent)
	if !ok {
		t.Fatalf("unexpected training payload type %T", publisher.messages[0].payload)
	}
	if completed.GetServingLoadStatus() != registrymodel.ModelLoadStatusNotLoaded.String() {
		t.Fatalf("training fact should not assert loaded status, got %s", completed.GetServingLoadStatus())
	}
	return completed
}

type fakeTrainingExecutor struct{}

func (e *fakeTrainingExecutor) RunTrainingJob(_ context.Context, spec trainingmodel.TrainingJobSpec) (*trainingmodel.TrainedModelArtifact, error) {
	return &trainingmodel.TrainedModelArtifact{
		TrainingRunID:     spec.TrainingRunID,
		ModelURI:          spec.ModelURI,
		ModelName:         spec.ModelName,
		ModelVersion:      spec.ModelVersion,
		BaseModel:         spec.BaseModel,
		ArtifactFormat:    "HF_PEFT_ADAPTER",
		ArtifactChecksum:  "sha256:adapter",
		ArtifactSizeBytes: 512,
		AdapterURI:        spec.AdapterURI,
		ServingTarget:     spec.ServingTarget,
		ServingModel:      spec.ServingModel,
		ServingLoadStatus: spec.ServingLoadStatus,
		RecipeHash:        spec.RecipeHash,
	}, nil
}

func (e *fakeTrainingExecutor) EvaluateModel(_ context.Context, spec trainingmodel.EvaluationJobSpec) (*trainingmodel.EvaluationReport, error) {
	return &trainingmodel.EvaluationReport{
		TrainingRunID: spec.TrainingRunID,
		ReportURI:     spec.ReportURI,
		Passed:        true,
		Metrics: map[string]float64{
			"faithfulness":      0.95,
			"answer_relevancy":  0.92,
			"context_precision": 0.9,
		},
		Thresholds: map[string]float64{
			"faithfulness":      0.8,
			"answer_relevancy":  0.8,
			"context_precision": 0.8,
		},
	}, nil
}

type publishedMessage struct {
	topic   string
	message sharedmessaging.Message
	payload proto.Message
}

type capturingPublisher struct {
	messages []publishedMessage
}

func (p *capturingPublisher) Publish(_ context.Context, topic string, message sharedmessaging.Message, payload proto.Message) error {
	p.messages = append(p.messages, publishedMessage{topic: topic, message: message, payload: payload})
	return nil
}

func (p *capturingPublisher) Close() {}

func newE2EK8sClient() dynamic.Interface {
	scheme := kruntime.NewScheme()
	return k8sfake.NewSimpleDynamicClientWithCustomListKinds(scheme, map[schema.GroupVersionResource]string{
		servedModelGVR: "ServedModelList",
	}, &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "Service",
			"metadata": map[string]any{
				"name":      "seed",
				"namespace": e2eNamespace,
			},
			"spec": map[string]any{
				"clusterIP": "10.96.0.1",
			},
		},
	})
}

func servedModelConfig() registryk8s.ServedModelConfig {
	return registryk8s.ServedModelConfig{
		Namespace: e2eNamespace,
		Group:     servedModelGVR.Group,
		Version:   servedModelGVR.Version,
		Resource:  servedModelGVR.Resource,
		Kind:      "ServedModel",
	}
}

func markDeploymentReady(t testFailureReporter, ctx context.Context, client dynamic.Interface, workloadName string) {
	t.Helper()

	resource := client.Resource(deploymentGVR).Namespace(e2eNamespace)
	deployment, err := resource.Get(ctx, workloadName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("read vllm deployment: %v", err)
	}
	_ = unstructured.SetNestedField(deployment.Object, int64(1), "status", "readyReplicas")
	_ = unstructured.SetNestedField(deployment.Object, deployment.GetGeneration(), "status", "observedGeneration")
	_, err = resource.Update(ctx, deployment, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("mark deployment ready: %v", err)
	}
}

func assertServedModelStatus(t testFailureReporter, ctx context.Context, client dynamic.Interface, loadStatus string, servingModel string) {
	t.Helper()

	items, err := client.Resource(servedModelGVR).Namespace(e2eNamespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list served model CRs: %v", err)
	}
	if len(items.Items) != 1 {
		t.Fatalf("expected one served model CR, got %d", len(items.Items))
	}
	gotStatus, _, _ := unstructured.NestedString(items.Items[0].Object, "status", "servingLoadStatus")
	gotServingModel, _, _ := unstructured.NestedString(items.Items[0].Object, "status", "servingModel")
	if gotStatus != loadStatus {
		t.Fatalf("served model load status = %s", gotStatus)
	}
	if gotServingModel != servingModel {
		t.Fatalf("served model name = %s", gotServingModel)
	}
}

type registryMemoryRepository struct {
	mu            sync.Mutex
	byModelID     map[uuid.UUID]*registrymodel.Model
	byTrainingID  map[uuid.UUID]uuid.UUID
	servingStatus map[uuid.UUID]uuid.UUID
}

func newRegistryMemoryRepository() *registryMemoryRepository {
	return &registryMemoryRepository{
		byModelID:     map[uuid.UUID]*registrymodel.Model{},
		byTrainingID:  map[uuid.UUID]uuid.UUID{},
		servingStatus: map[uuid.UUID]uuid.UUID{},
	}
}

func (r *registryMemoryRepository) Close() {}

func (r *registryMemoryRepository) Create(_ context.Context, registeredModel *registrymodel.Model, _ uuid.UUID) (*registrymodel.Model, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if existingID, ok := r.byTrainingID[registeredModel.TrainingRunID]; ok {
		if existing, ok := r.byModelID[existingID]; ok {
			return cloneRegistryModel(existing), registrydomain.ErrModelExists
		}
	}
	record := cloneRegistryModel(registeredModel)
	r.byModelID[record.ModelID] = record
	r.byTrainingID[record.TrainingRunID] = record.ModelID
	return cloneRegistryModel(record), nil
}

func (r *registryMemoryRepository) ReadByID(_ context.Context, modelID uuid.UUID) (*registrymodel.Model, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	record, ok := r.byModelID[modelID]
	if !ok {
		return nil, registrydomain.ErrModelNotFound
	}
	return cloneRegistryModel(record), nil
}

func (r *registryMemoryRepository) ReadByTrainingRunID(_ context.Context, trainingRunID uuid.UUID) (*registrymodel.Model, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	modelID, ok := r.byTrainingID[trainingRunID]
	if !ok {
		return nil, registrydomain.ErrModelNotFound
	}
	return cloneRegistryModel(r.byModelID[modelID]), nil
}

func (r *registryMemoryRepository) UpdateStatus(_ context.Context, modelID uuid.UUID, status registrymodel.ModelStatus, artifactLocation string, failureReason string) (*registrymodel.Model, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	record, ok := r.byModelID[modelID]
	if !ok {
		return nil, registrydomain.ErrModelNotFound
	}
	record.Status = status
	record.ArtifactLocation = artifactLocation
	record.FailureReason = failureReason
	return cloneRegistryModel(record), nil
}

func (r *registryMemoryRepository) UpdateServingStatus(_ context.Context, modelID uuid.UUID, status registrymodel.ModelStatus, servingLoadStatus registrymodel.ModelLoadStatus, servingTarget string, servingModel string, failureReason string, idempotencyKey uuid.UUID) (*registrymodel.Model, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.servingStatus[modelID] == idempotencyKey {
		return cloneRegistryModel(r.byModelID[modelID]), nil
	}
	record, ok := r.byModelID[modelID]
	if !ok {
		return nil, registrydomain.ErrModelNotFound
	}
	record.Status = status
	record.ServingLoadStatus = servingLoadStatus
	record.ServingTarget = servingTarget
	record.ServingModel = servingModel
	record.FailureReason = failureReason
	r.servingStatus[modelID] = idempotencyKey
	return cloneRegistryModel(record), nil
}

func cloneRegistryModel(in *registrymodel.Model) *registrymodel.Model {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func modelUpdatedEvent(model *registrymodel.Model) *modelregistrypb.ModelUpdatedEvent {
	return &modelregistrypb.ModelUpdatedEvent{
		ModelId:           model.ModelID.String(),
		TrainingRunId:     model.TrainingRunID.String(),
		DatasetId:         model.DatasetID.String(),
		Name:              model.Name,
		ModelVersion:      int32(model.ModelVersion),
		BaseModel:         model.BaseModel,
		ArtifactLocation:  model.ArtifactLocation,
		ArtifactFormat:    model.ArtifactFormat,
		ArtifactChecksum:  model.ArtifactChecksum,
		ArtifactSizeBytes: model.ArtifactSizeBytes,
		MetricsMetadata:   model.MetricsMetadata,
		Status:            model.Status.String(),
		FailureReason:     model.FailureReason,
		AdapterUri:        model.AdapterURI,
		ServingTarget:     model.ServingTarget,
		ServingModel:      model.ServingModel,
		ServingLoadStatus: model.ServingLoadStatus.String(),
	}
}

func newInferenceUsecase(datasetID uuid.UUID, userID uuid.UUID, embeddingSnapshotID uuid.UUID) (inferenceapp.InferenceUsecase, *inferenceRequestMemoryRepository, *inferenceFeedbackMemoryRepository) {
	modelRepo := newInferenceModelMemoryRepository()
	datasetRepo := newInferenceDatasetMemoryRepository(&inferencemodel.InferenceDataset{
		DatasetID:                datasetID,
		UserID:                   userID,
		DatasetVersion:           1,
		ProcessingState:          inferencemodel.DatasetProcessingEmbeddingsMaterialized,
		StorageLocation:          "s3://features/dataset.parquet",
		TableNamespace:           "default",
		TableName:                "dataset_features",
		TableFormat:              "PARQUET",
		CatalogProvider:          "LOCAL",
		ProcessingProfile:        "rag",
		SchemaVersion:            1,
		SchemaMetadata:           `{"columns":[]}`,
		EmbeddingSnapshotID:      embeddingSnapshotID,
		VectorStore:              "pgvector",
		CollectionName:           "dataset_embeddings",
		EmbeddingDimensions:      384,
		EmbeddingCount:           3,
		EmbeddingStrategyVersion: "v1",
		EmbeddingChunkerName:     "go-token-window",
		EmbeddingChunkerVersion:  "v1",
		EmbeddingChunkSize:       384,
		EmbeddingChunkOverlap:    64,
		EmbeddingProvider:        "tei",
		EmbeddingModel:           "bge-small-en-v1.5",
	})
	requestRepo := &inferenceRequestMemoryRepository{}
	feedbackRepo := &inferenceFeedbackMemoryRepository{}
	usecase := inferenceapp.NewInferenceUsecase(
		modelRepo,
		inferenceapp.WithInferenceDatasetRepository(datasetRepo),
		inferenceapp.WithInferenceRequestRepository(requestRepo),
		inferenceapp.WithInferenceFeedbackRepository(feedbackRepo),
		inferenceapp.WithRetrievalClient(&fakeRetrievalClient{embeddingSnapshotID: embeddingSnapshotID}),
		inferenceapp.WithReranker(&fakeReranker{}),
		inferenceapp.WithRerankCandidateMultiplier(3),
		inferenceapp.WithContextPacker(&fakeContextPacker{}),
		inferenceapp.WithPromptStrategy(inferencemodel.PromptStrategy{Version: "rag-v1", SystemPrompt: "answer from context", MaxContextTokens: 1024, MaxContextChunks: 2}),
		inferenceapp.WithPromptBuilder(&fakePromptBuilder{}),
		inferenceapp.WithGenerationAdapter(&fakeGenerator{}),
	)
	return usecase, requestRepo, feedbackRepo
}

type inferenceModelMemoryRepository struct {
	mu      sync.Mutex
	byModel map[uuid.UUID]*inferencemodel.InferenceModel
}

func newInferenceModelMemoryRepository() *inferenceModelMemoryRepository {
	return &inferenceModelMemoryRepository{byModel: map[uuid.UUID]*inferencemodel.InferenceModel{}}
}

func (r *inferenceModelMemoryRepository) UpsertModel(_ context.Context, inferenceModel *inferencemodel.InferenceModel, _ uuid.UUID) (*inferencemodel.InferenceModel, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	record := *inferenceModel
	r.byModel[record.ModelID] = &record
	return &record, nil
}

func (r *inferenceModelMemoryRepository) ReadByID(_ context.Context, modelID uuid.UUID) (*inferencemodel.InferenceModel, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	record, ok := r.byModel[modelID]
	if !ok {
		return nil, errors.New("inference model not found")
	}
	out := *record
	return &out, nil
}

type inferenceDatasetMemoryRepository struct {
	dataset *inferencemodel.InferenceDataset
}

func newInferenceDatasetMemoryRepository(dataset *inferencemodel.InferenceDataset) *inferenceDatasetMemoryRepository {
	return &inferenceDatasetMemoryRepository{dataset: dataset}
}

func (r *inferenceDatasetMemoryRepository) UpsertDataset(_ context.Context, dataset *inferencemodel.InferenceDataset, _ uuid.UUID) (*inferencemodel.InferenceDataset, error) {
	record := *dataset
	r.dataset = &record
	return &record, nil
}

func (r *inferenceDatasetMemoryRepository) ReadDataset(_ context.Context, datasetID uuid.UUID) (*inferencemodel.InferenceDataset, error) {
	if r.dataset == nil || r.dataset.DatasetID != datasetID {
		return nil, errors.New("inference dataset not found")
	}
	out := *r.dataset
	return &out, nil
}

type inferenceRequestMemoryRepository struct {
	records []*inferencemodel.InferenceRequest
}

func (r *inferenceRequestMemoryRepository) RecordInferenceRequest(_ context.Context, request *inferencemodel.InferenceRequest) error {
	record := *request
	r.records = append(r.records, &record)
	return nil
}

type inferenceFeedbackMemoryRepository struct {
	records   []*inferencemodel.InferenceFeedback
	snapshots []*inferencemodel.PreferenceDataset
}

func (r *inferenceFeedbackMemoryRepository) RecordFeedback(_ context.Context, feedback *inferencemodel.InferenceFeedback, _ uuid.UUID) (*inferencemodel.InferenceFeedback, error) {
	record := *feedback
	r.records = append(r.records, &record)
	return &record, nil
}

func (r *inferenceFeedbackMemoryRepository) ReadPreferenceDataset(_ context.Context, request inferencemodel.PreferenceDatasetExportRequest) (*inferencemodel.PreferenceDataset, error) {
	return &inferencemodel.PreferenceDataset{
		RequestID: request.RequestID,
		DatasetID: request.DatasetID,
		ModelID:   request.ModelID,
		OutputURI: request.OutputURI,
	}, nil
}

func (r *inferenceFeedbackMemoryRepository) RecordPreferenceDatasetSnapshot(_ context.Context, dataset *inferencemodel.PreferenceDataset, _ inferencemodel.PreferenceDatasetExportRequest) (*inferencemodel.PreferenceDataset, error) {
	record := *dataset
	r.snapshots = append(r.snapshots, &record)
	return &record, nil
}

type fakeRetrievalClient struct {
	embeddingSnapshotID uuid.UUID
}

func (c *fakeRetrievalClient) SearchEmbeddings(_ context.Context, _ uuid.UUID, _ string, topK int, _ map[string]string) ([]inferencemodel.RetrievedContext, error) {
	contexts := make([]inferencemodel.RetrievedContext, 0, topK)
	for i := 0; i < topK; i++ {
		contexts = append(contexts, inferencemodel.RetrievedContext{
			EmbeddingRecordID:   uuid.NewSHA1(uuid.NameSpaceURL, []byte(fmt.Sprintf("record-%d", i))),
			EmbeddingSnapshotID: c.embeddingSnapshotID,
			ChunkIndex:          i,
			SourceText:          fmt.Sprintf("retrieved context %d", i),
			Distance:            float64(i) / 10,
			Similarity:          1 - float64(i)/10,
		})
	}
	return contexts, nil
}

func (c *fakeRetrievalClient) Close() error { return nil }

type fakeReranker struct{}

func (r *fakeReranker) Rerank(_ context.Context, _ string, candidates []inferencemodel.RetrievedContext, topK int) ([]inferencemodel.RetrievedContext, error) {
	if len(candidates) < topK {
		return nil, fmt.Errorf("rerank requires at least topK candidates")
	}
	out := make([]inferencemodel.RetrievedContext, 0, topK)
	for i := 0; i < topK; i++ {
		candidate := candidates[len(candidates)-1-i]
		candidate.RerankScore = 0.99 - float64(i)/100
		out = append(out, candidate)
	}
	return out, nil
}

type fakeContextPacker struct{}

func (p *fakeContextPacker) Pack(_ context.Context, request inferencemodel.ContextPackRequest) ([]inferencemodel.RetrievedContext, error) {
	return request.Contexts, nil
}

type fakePromptBuilder struct{}

func (b *fakePromptBuilder) BuildPrompt(_ context.Context, request inferencemodel.PromptBuildRequest) (*inferencemodel.PromptPackage, error) {
	return &inferencemodel.PromptPackage{
		Prompt:   "question: " + request.Query + "\nmodel: " + request.Model.ServingModel,
		Strategy: inferencemodel.PromptStrategy{Version: "rag-v1"},
		Contexts: request.Contexts,
	}, nil
}

type fakeGenerator struct{}

func (g *fakeGenerator) Generate(_ context.Context, request inferencemodel.GenerationRequest) (string, error) {
	return "generated from " + request.Model.ServingModel, nil
}

func (g *fakeGenerator) Provider() string { return "vllm" }

func (g *fakeGenerator) Model() string { return "mistral-7b" }

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func httpJSON(status int, payload any) *http.Response {
	raw, _ := json.Marshal(payload)
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(bytes.NewReader(raw)),
		Header:     make(http.Header),
	}
}

var _ registryapp.ModelRepository = (*registryMemoryRepository)(nil)
var _ inferenceapp.InferenceModelRepository = (*inferenceModelMemoryRepository)(nil)
var _ inferenceapp.InferenceDatasetRepository = (*inferenceDatasetMemoryRepository)(nil)
var _ inferenceapp.InferenceRequestRepository = (*inferenceRequestMemoryRepository)(nil)
var _ inferenceapp.InferenceFeedbackRepository = (*inferenceFeedbackMemoryRepository)(nil)
var _ inferenceapp.RetrievalClient = (*fakeRetrievalClient)(nil)
var _ inferenceapp.Reranker = (*fakeReranker)(nil)
var _ inferenceapp.ContextPacker = (*fakeContextPacker)(nil)
var _ inferenceapp.PromptBuilder = (*fakePromptBuilder)(nil)
var _ inferenceapp.GenerationAdapter = (*fakeGenerator)(nil)

var _ = appsv1.SchemeGroupVersion
var _ = corev1.SchemeGroupVersion
