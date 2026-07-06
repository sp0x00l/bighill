package executor_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"strings"
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
	locations     []string
	statLocations []string
	payloads      map[string]string
	stats         map[string]executor.ObjectInfo
}

func (r *manifestReaderStub) Read(_ context.Context, location string) ([]byte, error) {
	r.locations = append(r.locations, location)
	return []byte(r.payloads[location]), nil
}

func (r *manifestReaderStub) Stat(_ context.Context, location string) (executor.ObjectInfo, error) {
	r.statLocations = append(r.statLocations, location)
	return r.stats[location], nil
}

var _ = Describe("RayExecutor", func() {
	It("submits a deterministic Ray training job and reads the artifact manifest on success", func() {
		var postBody []byte
		statusCalls := 0
		reader := &manifestReaderStub{payloads: map[string]string{
			"s3://models/run-1/artifact.json": `{"training_run_id":"run-1","model_uri":"s3://models/run-1","model_name":"ranker","model_version":"v1","base_model":"mistral","artifact_format":"HF_PEFT_ADAPTER","artifact_checksum":"sha256:abc","artifact_size_bytes":123,"recipe_hash":"hash"}`,
		}, stats: map[string]executor.ObjectInfo{
			"s3://models/run-1": {Location: "s3://models/run-1", SizeBytes: 123},
		}}
		ray, err := executor.NewRayExecutorWithClient(rayConfig(), reader, &http.Client{
			Transport: executorRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				switch req.Method + " " + req.URL.Path {
				case "GET /api/jobs/train-run-1-hash":
					statusCalls++
					if statusCalls == 1 {
						return response(http.StatusNotFound, ""), nil
					}
					if statusCalls == 2 {
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
			TrainingRunID:        "run-1",
			DatasetURI:           "s3://features/run-1.parquet",
			ModelName:            "ranker",
			ModelVersion:         "v1",
			BaseModel:            "mistral",
			ModelURI:             "s3://models/run-1",
			AdapterURI:           "s3://models/run-1",
			ServingTarget:        "vllm-local",
			ServingModel:         "ranker-v1",
			ServingLoadStatus:    "LOADED",
			ArtifactFormat:       "HF_PEFT_ADAPTER",
			ArtifactManifestURI:  "s3://models/run-1/artifact.json",
			ArtifactBucketRegion: "eu-west-1",
			AxolotlCommand:       "axolotl train",
			RecipeYAML:           "base_model: mistral",
			RecipeHash:           "hash",
			SubmissionID:         "train-run-1-hash",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(artifact.ModelURI).To(Equal("s3://models/run-1"))
		Expect(artifact.ArtifactChecksum).To(Equal("sha256:abc"))
		Expect(artifact.RecipeHash).To(Equal("hash"))
		Expect(reader.locations).To(Equal([]string{"s3://models/run-1/artifact.json"}))
		Expect(reader.statLocations).To(Equal([]string{"s3://models/run-1"}))
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
				"TRAINING_ADAPTER_URI":"s3://models/run-1",
				"TRAINING_SERVING_TARGET":"vllm-local",
				"TRAINING_SERVING_MODEL":"ranker-v1",
				"TRAINING_SERVING_LOAD_STATUS":"LOADED",
				"TRAINING_ARTIFACT_FORMAT":"HF_PEFT_ADAPTER",
				"TRAINING_ARTIFACT_MANIFEST_URI":"s3://models/run-1/artifact.json",
				"TRAINING_ARTIFACT_BUCKET_REGION":"eu-west-1",
				"TRAINING_AXOLOTL_COMMAND":"axolotl train",
				"TRAINING_RECIPE_YAML":"base_model: mistral",
				"TRAINING_RECIPE_HASH":"hash"
			}},
			"metadata":{"job_submission_id":"train-run-1-hash","submission_id":"train-run-1-hash"}
		}`))
	})

	It("reattaches to an already succeeded Ray job without submitting again", func() {
		reader := &manifestReaderStub{payloads: map[string]string{
			"s3://models/run-2/artifact.json": `{"training_run_id":"run-2","model_uri":"s3://models/run-2","model_name":"ranker","model_version":"v1","base_model":"mistral","artifact_format":"HF_PEFT_ADAPTER","artifact_checksum":"sha256:def","artifact_size_bytes":456}`,
		}, stats: map[string]executor.ObjectInfo{
			"s3://models/run-2": {Location: "s3://models/run-2", SizeBytes: 456},
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

	It("rejects artifact manifests whose artifact size does not match storage", func() {
		reader := &manifestReaderStub{payloads: map[string]string{
			"s3://models/run-size/artifact.json": `{"training_run_id":"run-size","model_uri":"s3://models/run-size","model_name":"ranker","model_version":"v1","base_model":"mistral","artifact_format":"HF_PEFT_ADAPTER","artifact_checksum":"sha256:def","artifact_size_bytes":456}`,
		}, stats: map[string]executor.ObjectInfo{
			"s3://models/run-size": {Location: "s3://models/run-size", SizeBytes: 455},
		}}
		ray, err := executor.NewRayExecutorWithClient(rayConfig(), reader, &http.Client{
			Transport: executorRoundTripFunc(func(_ *http.Request) (*http.Response, error) {
				return response(http.StatusOK, `{"status":"SUCCEEDED"}`), nil
			}),
		})
		Expect(err).NotTo(HaveOccurred())

		artifact, err := ray.RunTrainingJob(context.Background(), model.TrainingJobSpec{
			TrainingRunID:       "run-size",
			ArtifactManifestURI: "s3://models/run-size/artifact.json",
			SubmissionID:        "train-run-size-hash",
		})

		Expect(artifact).To(BeNil())
		Expect(err).To(MatchError(ContainSubstring("training artifact size mismatch")))
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

	It("stops the Ray job when the activity context is canceled", func() {
		stopCalled := false
		statusCalls := 0
		ctx, cancel := context.WithCancel(context.Background())
		ray, err := executor.NewRayExecutorWithClient(rayConfig(), &manifestReaderStub{}, &http.Client{
			Transport: executorRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				switch req.Method + " " + req.URL.Path {
				case "GET /api/jobs/train-cancel-hash":
					statusCalls++
					if statusCalls == 1 {
						return response(http.StatusNotFound, ""), nil
					}
					cancel()
					return response(http.StatusOK, `{"status":"RUNNING"}`), nil
				case "POST /api/jobs/":
					return response(http.StatusOK, `{"job_id":"train-cancel-hash"}`), nil
				case "POST /api/jobs/train-cancel-hash/stop":
					stopCalled = true
					return response(http.StatusOK, `{}`), nil
				default:
					Fail("unexpected request " + req.Method + " " + req.URL.Path)
					return nil, nil
				}
			}),
		})
		Expect(err).NotTo(HaveOccurred())

		artifact, err := ray.RunTrainingJob(ctx, model.TrainingJobSpec{
			TrainingRunID:       "run-cancel",
			ArtifactManifestURI: "s3://models/run-cancel/artifact.json",
			SubmissionID:        "train-cancel-hash",
		})

		Expect(artifact).To(BeNil())
		Expect(err).To(HaveOccurred())
		Expect(stopCalled).To(BeTrue())
	})

	It("runs evaluation jobs and reads the report manifest", func() {
		reader := &manifestReaderStub{payloads: map[string]string{
			"s3://evals/run-4.json": `{"training_run_id":"run-4","report_uri":"s3://evals/run-4.json","passed":true}`,
		}, stats: map[string]executor.ObjectInfo{
			"s3://evals/run-4.json": {Location: "s3://evals/run-4.json", SizeBytes: 32},
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

	It("submits promotion report jobs with an encoded job spec argument", func() {
		var postBody []byte
		statusCalls := 0
		reader := &manifestReaderStub{payloads: map[string]string{
			"s3://promotion/model-1.json": `{"user_id":"user-1","model_id":"model-1","training_run_id":"run-1","promotion_report_uri":"s3://promotion/model-1.json","deltas":{"faithfulness":0.1}}`,
		}, stats: map[string]executor.ObjectInfo{
			"s3://promotion/model-1.json": {Location: "s3://promotion/model-1.json", SizeBytes: 128},
		}}
		ray, err := executor.NewRayExecutorWithClient(rayConfig(), reader, &http.Client{
			Transport: executorRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				switch req.Method + " " + req.URL.Path {
				case "GET /api/jobs/promotion-model-1":
					statusCalls++
					if statusCalls == 1 {
						return response(http.StatusNotFound, ""), nil
					}
					return response(http.StatusOK, `{"status":"SUCCEEDED"}`), nil
				case "POST /api/jobs/":
					var err error
					postBody, err = io.ReadAll(req.Body)
					Expect(err).NotTo(HaveOccurred())
					return response(http.StatusOK, `{"job_id":"promotion-model-1"}`), nil
				default:
					Fail("unexpected request " + req.Method + " " + req.URL.Path)
					return nil, nil
				}
			}),
		})
		Expect(err).NotTo(HaveOccurred())

		report, err := ray.RunPromotionReport(context.Background(), model.PromotionReportJobSpec{
			UserID:                   "user-1",
			ModelID:                  "model-1",
			TrainingRunID:            "run-1",
			CandidateReportURI:       "s3://evals/candidate.json",
			CandidateMetricsMetadata: `{"metrics":{"faithfulness":0.9}}`,
			ChampionModelID:          "champion-1",
			ChampionReportURI:        "s3://evals/champion.json",
			ChampionMetricsMetadata:  `{"metrics":{"faithfulness":0.8}}`,
			PromotionProfile:         `{"promotion":{"require_deepchecks":true}}`,
			ReportURI:                "s3://promotion/model-1.json",
			ReportManifestURI:        "s3://promotion/model-1.json",
			ArtifactBucketRegion:     "eu-west-1",
			SubmissionID:             "promotion-model-1",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(report.ModelID).To(Equal("model-1"))
		var submitted struct {
			SubmissionID string `json:"submission_id"`
			Entrypoint   string `json:"entrypoint"`
			RuntimeEnv   struct {
				EnvVars map[string]string `json:"env_vars"`
			} `json:"runtime_env"`
		}
		Expect(json.Unmarshal(postBody, &submitted)).To(Succeed())
		Expect(submitted.SubmissionID).To(Equal("promotion-model-1"))
		Expect(submitted.RuntimeEnv.EnvVars).To(Equal(map[string]string{
			"TRAINING_ARTIFACT_BUCKET_REGION": "eu-west-1",
		}))
		Expect(submitted.Entrypoint).To(HavePrefix("python -m promote --job-spec-b64 "))
		encoded := strings.TrimPrefix(submitted.Entrypoint, "python -m promote --job-spec-b64 ")
		raw, err := base64.RawURLEncoding.DecodeString(encoded)
		Expect(err).NotTo(HaveOccurred())
		var spec map[string]string
		Expect(json.Unmarshal(raw, &spec)).To(Succeed())
		Expect(spec).To(Equal(map[string]string{
			"user_id":                    "user-1",
			"model_id":                   "model-1",
			"training_run_id":            "run-1",
			"candidate_report_uri":       "s3://evals/candidate.json",
			"candidate_metrics_metadata": `{"metrics":{"faithfulness":0.9}}`,
			"champion_model_id":          "champion-1",
			"champion_report_uri":        "s3://evals/champion.json",
			"champion_metrics_metadata":  `{"metrics":{"faithfulness":0.8}}`,
			"promotion_profile":          `{"promotion":{"require_deepchecks":true}}`,
			"report_uri":                 "s3://promotion/model-1.json",
			"report_manifest_uri":        "s3://promotion/model-1.json",
		}))
	})
})

func rayConfig() executor.RayExecutorConfig {
	return executor.RayExecutorConfig{
		URL:                  "http://ray.local",
		TrainingEntrypoint:   "python -m train",
		EvaluationEntrypoint: "python -m eval",
		PromotionEntrypoint:  "python -m promote",
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
