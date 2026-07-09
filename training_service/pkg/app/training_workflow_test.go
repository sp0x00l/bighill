package app_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"training_service/pkg/app"
	"training_service/pkg/domain/model"
	"training_service/pkg/infra/executor"
	"training_service/pkg/infra/temporalworker"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

func TestApp(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Training service app unit test suite")
}

var _ = Describe("TrainModelWorkflow", func() {
	var suite testsuite.WorkflowTestSuite

	It("runs the training workflow through all activities", func() {
		env := suite.NewTestWorkflowEnvironment()
		request := model.TrainingRunRequest{
			TrainingRunID:     "training-run-1",
			DatasetID:         "dataset-1",
			DatasetVersion:    "3",
			FeatureSnapshotID: "feature-snapshot-1",
			ModelName:         "sentence-transformer",
			ModelVersion:      "local-dev",
			BaseModel:         "mistral-7b",
			EvaluationProfile: "smoke",
		}
		prepared := model.PreparedTrainingDataset{
			TrainingRunID:     request.TrainingRunID,
			FeatureSnapshotID: request.FeatureSnapshotID,
			DatasetURI:        "s3://local-dev-bucket/features/feature-snapshot-1.parquet",
		}
		artifact := model.TrainedModelArtifact{
			TrainingRunID:     request.TrainingRunID,
			ModelURI:          "s3://local-dev-bucket/models/training-run-1",
			ModelName:         request.ModelName,
			ModelVersion:      request.ModelVersion,
			BaseModel:         request.BaseModel,
			ArtifactFormat:    "HF_PEFT_ADAPTER",
			ArtifactChecksum:  "sha256:abc",
			ArtifactSizeBytes: 128,
		}
		report := model.EvaluationReport{
			TrainingRunID:        request.TrainingRunID,
			ReportURI:            "s3://local-dev-bucket/evaluations/training-run-1.json",
			Passed:               true,
			Metrics:              map[string]float64{"faithfulness": 0.91},
			Thresholds:           map[string]float64{"faithfulness": 0.8},
			EvaluatorName:        "ragas",
			EvaluatorVersion:     "ragas-v1",
			MetricSuite:          "rag",
			EvalDatasetURI:       "s3://evals/run-1.jsonl",
			EvalDatasetMode:      "labeled",
			JudgeProvider:        "openai",
			JudgeModel:           "local-judge",
			JudgeTemplateVersion: "judge-v1",
			DeepchecksPassed:     true,
			DeepchecksReportURI:  "s3://local-dev-bucket/evaluations/deepchecks.html",
			EvidentlyPassed:      true,
			EvidentlyReportURI:   "s3://local-dev-bucket/evaluations/evidently.html",
		}

		env.RegisterActivityWithOptions(func(model.TrainingRunRequest) (*model.PreparedTrainingDataset, error) {
			return nil, nil
		}, activity.RegisterOptions{Name: app.PrepareTrainingDatasetActivity})
		env.RegisterActivityWithOptions(func(model.PreparedTrainingDataset, model.TrainingRunRequest) (*model.TrainedModelArtifact, error) {
			return nil, nil
		}, activity.RegisterOptions{Name: app.RunTrainingJobActivity})
		var evaluationInfo activity.Info
		env.RegisterActivityWithOptions(func(ctx context.Context, _ model.TrainedModelArtifact, _ model.TrainingRunRequest) (*model.EvaluationReport, error) {
			evaluationInfo = activity.GetInfo(ctx)
			return &report, nil
		}, activity.RegisterOptions{Name: app.EvaluateTrainedModelActivity})
		env.RegisterActivityWithOptions(func(model.TrainingRunResult) error {
			return nil
		}, activity.RegisterOptions{Name: app.PublishModelTrainingCompletedActivity})
		env.RegisterActivityWithOptions(func(model.TrainingRunResult) error {
			return nil
		}, activity.RegisterOptions{Name: app.PublishModelTrainingFailedActivity})

		env.OnActivity(app.PrepareTrainingDatasetActivity, request).Return(&prepared, nil)
		env.OnActivity(app.RunTrainingJobActivity, prepared, request).Return(&artifact, nil)
		env.OnActivity(app.PublishModelTrainingCompletedActivity, mock.Anything).Return(nil)

		env.ExecuteWorkflow(app.TrainModelWorkflow, request)

		Expect(env.IsWorkflowCompleted()).To(BeTrue())
		Expect(env.GetWorkflowError()).NotTo(HaveOccurred())

		var result model.TrainingRunResult
		Expect(env.GetWorkflowResult(&result)).To(Succeed())
		Expect(result.Status).To(Equal(model.TrainingRunStatusCompleted))
		Expect(result.DatasetVersion).To(Equal("3"))
		Expect(result.ModelURI).To(Equal(artifact.ModelURI))
		Expect(result.ReportURI).To(Equal(report.ReportURI))
		Expect(result.MetricsMetadata).To(MatchJSON(`{"passed":true,"metrics":{"faithfulness":0.91},"thresholds":{"faithfulness":0.8},"report_uri":"s3://local-dev-bucket/evaluations/training-run-1.json","evaluator_name":"ragas","evaluator_version":"ragas-v1","metric_suite":"rag","eval_dataset_uri":"s3://evals/run-1.jsonl","eval_dataset_mode":"labeled","judge_provider":"openai","judge_model":"local-judge","judge_template_version":"judge-v1","deepchecks_passed":true,"deepchecks_report_uri":"s3://local-dev-bucket/evaluations/deepchecks.html","evidently_passed":true,"evidently_report_uri":"s3://local-dev-bucket/evaluations/evidently.html"}`))
		Expect(evaluationInfo.ActivityID).To(Equal("evaluate:training-run-1"))
		Expect(evaluationInfo.StartToCloseTimeout).To(Equal(app.DefaultEvaluateTrainingActivityTimeout))
		Expect(evaluationInfo.ScheduleToCloseTimeout).To(Equal(app.DefaultEvaluateTrainingActivityTimeout))
		Expect(evaluationInfo.HeartbeatTimeout).To(Equal(app.DefaultTrainingActivityHeartbeat))
	})

	It("publishes a failed training fact when evaluation does not pass", func() {
		env := suite.NewTestWorkflowEnvironment()
		request := model.TrainingRunRequest{
			TrainingRunID:     "training-run-2",
			DatasetID:         "dataset-2",
			DatasetVersion:    "5",
			FeatureSnapshotID: "feature-snapshot-2",
			ModelName:         "sentence-transformer",
			ModelVersion:      "local-dev",
			BaseModel:         "mistral-7b",
		}
		prepared := model.PreparedTrainingDataset{
			TrainingRunID:     request.TrainingRunID,
			FeatureSnapshotID: request.FeatureSnapshotID,
			DatasetURI:        "s3://local-dev-bucket/features/feature-snapshot-2.parquet",
		}
		artifact := model.TrainedModelArtifact{
			TrainingRunID:     request.TrainingRunID,
			ModelURI:          "s3://local-dev-bucket/models/training-run-2",
			ModelName:         request.ModelName,
			ModelVersion:      request.ModelVersion,
			BaseModel:         request.BaseModel,
			ArtifactFormat:    "HF_PEFT_ADAPTER",
			ArtifactChecksum:  "sha256:def",
			ArtifactSizeBytes: 128,
		}
		report := model.EvaluationReport{
			TrainingRunID: request.TrainingRunID,
			ReportURI:     "s3://local-dev-bucket/evaluations/training-run-2.json",
			Passed:        false,
			FailureReason: "model evaluation failed",
		}

		env.RegisterActivityWithOptions(func(model.TrainingRunRequest) (*model.PreparedTrainingDataset, error) {
			return nil, nil
		}, activity.RegisterOptions{Name: app.PrepareTrainingDatasetActivity})
		env.RegisterActivityWithOptions(func(model.PreparedTrainingDataset, model.TrainingRunRequest) (*model.TrainedModelArtifact, error) {
			return nil, nil
		}, activity.RegisterOptions{Name: app.RunTrainingJobActivity})
		env.RegisterActivityWithOptions(func(model.TrainedModelArtifact, model.TrainingRunRequest) (*model.EvaluationReport, error) {
			return nil, nil
		}, activity.RegisterOptions{Name: app.EvaluateTrainedModelActivity})
		env.RegisterActivityWithOptions(func(model.TrainingRunResult) error {
			return nil
		}, activity.RegisterOptions{Name: app.PublishModelTrainingCompletedActivity})
		env.RegisterActivityWithOptions(func(model.TrainingRunResult) error {
			return nil
		}, activity.RegisterOptions{Name: app.PublishModelTrainingFailedActivity})

		env.OnActivity(app.PrepareTrainingDatasetActivity, request).Return(&prepared, nil)
		env.OnActivity(app.RunTrainingJobActivity, prepared, request).Return(&artifact, nil)
		env.OnActivity(app.EvaluateTrainedModelActivity, artifact, request).Return(&report, nil)
		env.OnActivity(app.PublishModelTrainingFailedActivity, mock.Anything).Return(nil)

		env.ExecuteWorkflow(app.TrainModelWorkflow, request)

		Expect(env.IsWorkflowCompleted()).To(BeTrue())
		Expect(env.GetWorkflowError()).NotTo(HaveOccurred())

		var result model.TrainingRunResult
		Expect(env.GetWorkflowResult(&result)).To(Succeed())
		Expect(result.Status).To(Equal(model.TrainingRunStatusFailed))
		Expect(result.FailureReason).To(Equal("model evaluation failed"))
		Expect(result.MetricsMetadata).To(MatchJSON(`{"passed":false,"report_uri":"s3://local-dev-bucket/evaluations/training-run-2.json"}`))
	})

	It("publishes a failed training fact when an activity fails", func() {
		env := suite.NewTestWorkflowEnvironment()
		request := model.TrainingRunRequest{
			TrainingRunID:     "6d7adfd9-d11f-53d9-bc8d-567c73ff4307",
			UserID:            "d3b800cb-f988-46e3-b6c7-2f5a8bf42ce6",
			DatasetID:         "d7799a97-a188-4a9c-b73f-1546c66bbcdf",
			DatasetVersion:    "5",
			FeatureSnapshotID: "feature-snapshot-activity-failure",
			ModelName:         "sentence-transformer",
			ModelVersion:      "local-dev",
			BaseModel:         "mistral-7b",
		}
		activityErr := errors.New("prepare failed")
		var failedResult model.TrainingRunResult

		env.RegisterActivityWithOptions(func(model.TrainingRunRequest) (*model.PreparedTrainingDataset, error) {
			return nil, nil
		}, activity.RegisterOptions{Name: app.PrepareTrainingDatasetActivity})
		env.RegisterActivityWithOptions(func(model.TrainingRunResult) error {
			return nil
		}, activity.RegisterOptions{Name: app.PublishModelTrainingFailedActivity})
		env.OnActivity(app.PrepareTrainingDatasetActivity, request).Return(nil, activityErr)
		env.OnActivity(app.PublishModelTrainingFailedActivity, mock.MatchedBy(func(result model.TrainingRunResult) bool {
			failedResult = result
			return true
		})).Return(nil)

		env.ExecuteWorkflow(app.TrainModelWorkflow, request)

		Expect(env.IsWorkflowCompleted()).To(BeTrue())
		Expect(env.GetWorkflowError()).To(HaveOccurred())
		Expect(failedResult.Status).To(Equal(model.TrainingRunStatusFailed))
		Expect(failedResult.TrainingRunID).To(Equal(request.TrainingRunID))
		Expect(failedResult.ModelID).To(Equal(request.TrainingRunID))
		Expect(failedResult.UserID).To(Equal(request.UserID))
		Expect(failedResult.DatasetID).To(Equal(request.DatasetID))
		Expect(failedResult.FailureReason).To(ContainSubstring("prepare training dataset failed"))
		Expect(failedResult.FailureReason).To(ContainSubstring("prepare failed"))
	})

	It("runs end to end through fake Ray submit, poll, manifests, and model event publishing", func() {
		env := suite.NewTestWorkflowEnvironment()
		request := model.TrainingRunRequest{
			TrainingRunID:     "training-run-ray",
			DatasetID:         "dataset-ray",
			DatasetVersion:    "7",
			FeatureSnapshotID: "feature-snapshot-ray",
			DatasetURI:        "s3://features/dataset-ray.parquet",
			ModelName:         "rag-adapter",
			ModelVersion:      "v1",
			BaseModel:         "mistral-7b",
			EvaluationProfile: "smoke",
			TrainingProfile: model.TrainingProfile{
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
		reader := &workflowManifestReader{
			payloads: map[string]string{
				"s3://models/training-run-ray/artifact.json": `{"training_run_id":"training-run-ray","model_uri":"s3://models/training-run-ray","model_name":"rag-adapter","model_version":"v1","base_model":"mistral-7b","artifact_format":"HF_PEFT_ADAPTER","artifact_checksum":"sha256:adapter","artifact_size_bytes":512}`,
				"s3://evaluations/training-run-ray.json":     `{"training_run_id":"training-run-ray","report_uri":"s3://evaluations/training-run-ray.json","passed":true}`,
			},
			stats: map[string]model.ObjectInfo{
				"s3://models/training-run-ray":           {Location: "s3://models/training-run-ray", SizeBytes: 512, Checksum: "sha256:adapter"},
				"s3://evaluations/training-run-ray.json": {Location: "s3://evaluations/training-run-ray.json", SizeBytes: 64},
			},
		}
		posts := make([]string, 0, 2)
		trainStatusCalls := 0
		evalStatusCalls := 0
		ray, err := executor.NewRayExecutorWithClient(executor.RayExecutorConfig{
			URL:                  "http://ray.local",
			TrainingEntrypoint:   "python -m training_jobs.train",
			EvaluationEntrypoint: "python -m training_jobs.evaluate",
			PromotionEntrypoint:  "python -m training_jobs.promotion_report",
			RequestTimeout:       time.Second,
			PollInterval:         time.Millisecond,
		}, reader, &http.Client{Transport: workflowRoundTripFunc(func(req *http.Request) (*http.Response, error) {
			switch {
			case req.Method == http.MethodGet && strings.HasPrefix(req.URL.Path, "/api/jobs/train-training-run-ray-"):
				trainStatusCalls++
				if trainStatusCalls == 1 {
					return workflowHTTPResponse(http.StatusNotFound, ""), nil
				}
				return workflowHTTPResponse(http.StatusOK, `{"status":"SUCCEEDED"}`), nil
			case req.Method == http.MethodGet && strings.HasPrefix(req.URL.Path, "/api/jobs/eval-training-run-ray-"):
				evalStatusCalls++
				if evalStatusCalls == 1 {
					return workflowHTTPResponse(http.StatusNotFound, ""), nil
				}
				return workflowHTTPResponse(http.StatusOK, `{"status":"SUCCEEDED"}`), nil
			case req.Method == http.MethodPost && req.URL.Path == "/api/jobs/":
				raw, err := io.ReadAll(req.Body)
				Expect(err).NotTo(HaveOccurred())
				posts = append(posts, string(raw))
				return workflowHTTPResponse(http.StatusOK, `{"job_id":"accepted"}`), nil
			default:
				Fail("unexpected Ray request " + req.Method + " " + req.URL.Path)
				return nil, nil
			}
		})})
		Expect(err).NotTo(HaveOccurred())
		publisher := &workflowTrainingEventPublisher{}
		activities := temporalworker.NewTrainingActivities(
			publisher,
			temporalworker.WithExecutor(ray),
			temporalworker.WithModelURIPrefix("s3://models"),
			temporalworker.WithEvaluationURIPrefix("s3://evaluations"),
		)

		env.RegisterActivityWithOptions(activities.PrepareTrainingDataset, activity.RegisterOptions{Name: app.PrepareTrainingDatasetActivity})
		env.RegisterActivityWithOptions(activities.RunTrainingJob, activity.RegisterOptions{Name: app.RunTrainingJobActivity})
		env.RegisterActivityWithOptions(activities.EvaluateTrainedModel, activity.RegisterOptions{Name: app.EvaluateTrainedModelActivity})
		env.RegisterActivityWithOptions(activities.PublishModelTrainingCompleted, activity.RegisterOptions{Name: app.PublishModelTrainingCompletedActivity})
		env.RegisterActivityWithOptions(activities.PublishModelTrainingFailed, activity.RegisterOptions{Name: app.PublishModelTrainingFailedActivity})

		env.ExecuteWorkflow(app.TrainModelWorkflow, request)

		Expect(env.IsWorkflowCompleted()).To(BeTrue())
		Expect(env.GetWorkflowError()).NotTo(HaveOccurred())
		Expect(posts).To(HaveLen(2))
		Expect(posts[0]).To(ContainSubstring(`"submission_id":"train-training-run-ray-`))
		Expect(posts[0]).To(ContainSubstring(`"TRAINING_RECIPE_YAML"`))
		Expect(posts[0]).To(ContainSubstring(`"TRAINING_AXOLOTL_COMMAND"`))
		Expect(posts[0]).To(ContainSubstring(`"TRAINING_ARTIFACT_BUCKET_REGION"`))
		Expect(posts[1]).To(ContainSubstring(`"submission_id":"eval-training-run-ray-`))
		Expect(posts[1]).To(ContainSubstring(`"TRAINING_ARTIFACT_BUCKET_REGION"`))
		Expect(reader.readLocations).To(Equal([]string{
			"s3://models/training-run-ray/artifact.json",
			"s3://evaluations/training-run-ray.json",
		}))
		Expect(reader.statLocations).To(Equal([]string{
			"s3://models/training-run-ray",
			"s3://evaluations/training-run-ray.json",
		}))
		Expect(publisher.completedResult).NotTo(BeNil())
		Expect(publisher.completedResult.ModelURI).To(Equal("s3://models/training-run-ray"))
		Expect(publisher.completedResult.ReportURI).To(Equal("s3://evaluations/training-run-ray.json"))
		Expect(publisher.completedResult.ServingLoadStatus).To(Equal("NOT_LOADED"))
	})
})

type workflowRoundTripFunc func(*http.Request) (*http.Response, error)

func (f workflowRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type workflowManifestReader struct {
	payloads      map[string]string
	stats         map[string]model.ObjectInfo
	readLocations []string
	statLocations []string
}

func (r *workflowManifestReader) Read(_ context.Context, location string) ([]byte, error) {
	r.readLocations = append(r.readLocations, location)
	return []byte(r.payloads[location]), nil
}

func (r *workflowManifestReader) Stat(_ context.Context, location string) (model.ObjectInfo, error) {
	r.statLocations = append(r.statLocations, location)
	return r.stats[location], nil
}

type workflowTrainingEventPublisher struct {
	completedResult *model.TrainingRunResult
	failedResult    *model.TrainingRunResult
}

func (p *workflowTrainingEventPublisher) PublishModelTrainingCompleted(_ context.Context, result *model.TrainingRunResult) error {
	p.completedResult = result
	return nil
}

func (p *workflowTrainingEventPublisher) PublishModelTrainingFailed(_ context.Context, result *model.TrainingRunResult) error {
	p.failedResult = result
	return nil
}

func (p *workflowTrainingEventPublisher) PublishPromotionReportReady(context.Context, *model.PromotionReport) error {
	return nil
}

func workflowHTTPResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(bytes.NewBufferString(body)),
		Header:     make(http.Header),
	}
}
