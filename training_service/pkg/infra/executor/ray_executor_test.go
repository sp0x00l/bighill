package executor_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"training_service/pkg/domain/model"
	"training_service/pkg/infra/executor"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestExecutor(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Training executor unit test suite")
}

type executorRoundTripFunc func(*http.Request) (*http.Response, error)

func (f executorRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type manifestReaderStub struct {
	locations []string
	payloads  map[string]string
}

func (r *manifestReaderStub) Read(_ context.Context, location string) ([]byte, error) {
	r.locations = append(r.locations, location)
	return []byte(r.payloads[location]), nil
}

var _ = Describe("RayExecutor", func() {
	It("submits a deterministic Ray training job and reads the artifact manifest on success", func() {
		var postBody []byte
		statusCalls := 0
		reader := &manifestReaderStub{payloads: map[string]string{
			"s3://models/run-1/artifact.json": `{"training_run_id":"run-1","model_uri":"s3://models/run-1","model_name":"ranker","model_version":"v1","base_model":"mistral","artifact_format":"HF_PEFT_ADAPTER","artifact_checksum":"sha256:abc","artifact_size_bytes":123}`,
		}}
		ray, err := executor.NewRayExecutorWithClient(rayConfig(), reader, &http.Client{
			Transport: executorRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				switch req.Method + " " + req.URL.Path {
				case "GET /api/jobs/train-run-1-hash":
					statusCalls++
					if statusCalls == 1 {
						return response(http.StatusNotFound, ""), nil
					}
					return response(http.StatusOK, `{"status":"SUCCEEDED"}`), nil
				case "POST /api/jobs/":
					var err error
					postBody, err = io.ReadAll(req.Body)
					Expect(err).NotTo(HaveOccurred())
					return response(http.StatusOK, `{"job_id":"train-run-1-hash"}`), nil
				default:
					Fail("unexpected request " + req.Method + " " + req.URL.Path)
					return nil, nil
				}
			}),
		})
		Expect(err).NotTo(HaveOccurred())

		artifact, err := ray.RunTrainingJob(context.Background(), model.TrainingJobSpec{
			TrainingRunID:       "run-1",
			DatasetURI:          "s3://features/run-1.parquet",
			ModelName:           "ranker",
			ModelVersion:        "v1",
			BaseModel:           "mistral",
			ModelURI:            "s3://models/run-1",
			ArtifactManifestURI: "s3://models/run-1/artifact.json",
			RecipeYAML:          "base_model: mistral",
			RecipeHash:          "hash",
			SubmissionID:        "train-run-1-hash",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(artifact.ModelURI).To(Equal("s3://models/run-1"))
		Expect(artifact.ArtifactChecksum).To(Equal("sha256:abc"))
		Expect(reader.locations).To(Equal([]string{"s3://models/run-1/artifact.json"}))
		Expect(string(postBody)).To(MatchJSON(`{
			"submission_id":"train-run-1-hash",
			"entrypoint":"python -m train",
			"runtime_env":{"env_vars":{
				"TRAINING_RUN_ID":"run-1",
				"TRAINING_DATASET_URI":"s3://features/run-1.parquet",
				"TRAINING_MODEL_NAME":"ranker",
				"TRAINING_MODEL_VERSION":"v1",
				"TRAINING_BASE_MODEL":"mistral",
				"TRAINING_MODEL_URI":"s3://models/run-1",
				"TRAINING_ARTIFACT_MANIFEST_URI":"s3://models/run-1/artifact.json",
				"TRAINING_RECIPE_YAML":"base_model: mistral",
				"TRAINING_RECIPE_HASH":"hash"
			}},
			"metadata":{"submission_id":"train-run-1-hash"}
		}`))
	})

	It("reattaches to an already succeeded Ray job without submitting again", func() {
		reader := &manifestReaderStub{payloads: map[string]string{
			"s3://models/run-2/artifact.json": `{"training_run_id":"run-2","model_uri":"s3://models/run-2","model_name":"ranker","model_version":"v1","base_model":"mistral","artifact_format":"HF_PEFT_ADAPTER","artifact_checksum":"sha256:def","artifact_size_bytes":456}`,
		}}
		ray, err := executor.NewRayExecutorWithClient(rayConfig(), reader, &http.Client{
			Transport: executorRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				Expect(req.Method).To(Equal(http.MethodGet))
				return response(http.StatusOK, `{"status":"SUCCEEDED"}`), nil
			}),
		})
		Expect(err).NotTo(HaveOccurred())

		artifact, err := ray.RunTrainingJob(context.Background(), model.TrainingJobSpec{
			TrainingRunID:       "run-2",
			ArtifactManifestURI: "s3://models/run-2/artifact.json",
			SubmissionID:        "train-run-2-hash",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(artifact.ArtifactChecksum).To(Equal("sha256:def"))
	})

	It("returns Ray failed jobs as training failures", func() {
		ray, err := executor.NewRayExecutorWithClient(rayConfig(), &manifestReaderStub{}, &http.Client{
			Transport: executorRoundTripFunc(func(_ *http.Request) (*http.Response, error) {
				return response(http.StatusOK, `{"status":"FAILED","message":"container exited"}`), nil
			}),
		})
		Expect(err).NotTo(HaveOccurred())

		artifact, err := ray.RunTrainingJob(context.Background(), model.TrainingJobSpec{
			TrainingRunID:       "run-3",
			ArtifactManifestURI: "s3://models/run-3/artifact.json",
			SubmissionID:        "train-run-3-hash",
		})

		Expect(artifact).To(BeNil())
		Expect(err).To(MatchError(ContainSubstring("ray job failed: container exited")))
	})

	It("runs evaluation jobs and reads the report manifest", func() {
		reader := &manifestReaderStub{payloads: map[string]string{
			"s3://evals/run-4.json": `{"training_run_id":"run-4","report_uri":"s3://evals/run-4.json","passed":true}`,
		}}
		ray, err := executor.NewRayExecutorWithClient(rayConfig(), reader, &http.Client{
			Transport: executorRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				switch req.Method + " " + req.URL.Path {
				case "GET /api/jobs/eval-run-4-hash":
					return response(http.StatusOK, `{"status":"SUCCEEDED"}`), nil
				default:
					Fail("unexpected request " + req.Method + " " + req.URL.Path)
					return nil, nil
				}
			}),
		})
		Expect(err).NotTo(HaveOccurred())

		report, err := ray.EvaluateModel(context.Background(), model.EvaluationJobSpec{
			TrainingRunID:     "run-4",
			ReportManifestURI: "s3://evals/run-4.json",
			SubmissionID:      "eval-run-4-hash",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(report.Passed).To(BeTrue())
		Expect(report.ReportURI).To(Equal("s3://evals/run-4.json"))
	})
})

func rayConfig() executor.RayExecutorConfig {
	return executor.RayExecutorConfig{
		URL:                  "http://ray.local",
		TrainingEntrypoint:   "python -m train",
		EvaluationEntrypoint: "python -m eval",
		RequestTimeout:       time.Second,
		PollInterval:         time.Millisecond,
	}
}

func response(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(bytes.NewBufferString(body)),
		Header:     make(http.Header),
	}
}
