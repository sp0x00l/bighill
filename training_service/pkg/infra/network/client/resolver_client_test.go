package client

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"training_service/pkg/domain"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestClient(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Training service client unit test suite")
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

var _ = Describe("DatasetResolver", func() {
	It("resolves materialized datasets and forwards the user and org ids", func() {
		userID := uuid.New()
		orgID := uuid.New()
		datasetID := uuid.New()
		httpClient := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			Expect(req.Method).To(Equal(http.MethodGet))
			Expect(req.URL.Path).To(Equal("/v1/data/registry/" + datasetID.String() + "/materialization"))
			Expect(req.Header.Get(userIDHeader)).To(Equal(userID.String()))
			Expect(req.Header.Get(orgIDHeader)).To(Equal(orgID.String()))
			return jsonResponse(http.StatusOK, `{
				"id":"`+datasetID.String()+`",
				"userId":"`+userID.String()+`",
				"orgId":"`+orgID.String()+`",
				"storageLocation":"s3://lakehouse/features/movies.parquet",
				"tableName":"movies",
				"tableFormat":"PARQUET",
				"processingState":"FEATURE_MATERIALIZED",
				"datasetVersion":4,
				"featureSnapshotId":"`+uuid.NewString()+`"
			}`), nil
		})}

		ref, err := NewDatasetResolver("http://data-registry", httpClient).ResolveMaterializedDataset(context.Background(), userID, orgID, datasetID)

		Expect(err).NotTo(HaveOccurred())
		Expect(ref.DatasetID).To(Equal(datasetID.String()))
		Expect(ref.UserID).To(Equal(userID.String()))
		Expect(ref.OrgID).To(Equal(orgID.String()))
		Expect(ref.DatasetVersion).To(Equal("4"))
		Expect(ref.DatasetURI).To(Equal("s3://lakehouse/features/movies.parquet"))
	})

	It("maps missing datasets to validation errors", func() {
		httpClient := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return jsonResponse(http.StatusNotFound, `{"message":"missing"}`), nil
		})}

		_, err := NewDatasetResolver("http://data-registry", httpClient).ResolveMaterializedDataset(context.Background(), uuid.New(), uuid.New(), uuid.New())

		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
	})

	It("maps unmaterialized datasets to validation errors", func() {
		httpClient := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return jsonResponse(http.StatusBadRequest, `{"message":"not materialized"}`), nil
		})}

		_, err := NewDatasetResolver("http://data-registry", httpClient).ResolveMaterializedDataset(context.Background(), uuid.New(), uuid.New(), uuid.New())

		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
	})

	It("rejects resolver responses that are not trainable materialized parquet datasets", func() {
		datasetID := uuid.New()
		orgID := uuid.New()
		httpClient := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return jsonResponse(http.StatusOK, `{
				"id":"`+datasetID.String()+`",
				"userId":"`+uuid.NewString()+`",
				"orgId":"`+orgID.String()+`",
				"storageLocation":"s3://lakehouse/raw/movies.parquet",
				"tableName":"movies",
				"tableFormat":"PARQUET",
				"processingState":"RAW_MATERIALIZED",
				"datasetVersion":4,
				"featureSnapshotId":"`+uuid.NewString()+`"
			}`), nil
		})}

		_, err := NewDatasetResolver("http://data-registry", httpClient).ResolveMaterializedDataset(context.Background(), uuid.New(), orgID, datasetID)

		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
		Expect(err.Error()).To(ContainSubstring("dataset is not materialized"))
	})

	It("rejects dataset resolver responses from another org", func() {
		datasetID := uuid.New()
		orgID := uuid.New()
		httpClient := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return jsonResponse(http.StatusOK, `{
				"id":"`+datasetID.String()+`",
				"userId":"`+uuid.NewString()+`",
				"orgId":"`+uuid.NewString()+`",
				"storageLocation":"s3://lakehouse/features/movies.parquet",
				"tableName":"movies",
				"tableFormat":"PARQUET",
				"processingState":"FEATURE_MATERIALIZED",
				"datasetVersion":4,
				"featureSnapshotId":"`+uuid.NewString()+`"
			}`), nil
		})}

		_, err := NewDatasetResolver("http://data-registry", httpClient).ResolveMaterializedDataset(context.Background(), uuid.New(), orgID, datasetID)

		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
		Expect(err.Error()).To(ContainSubstring("active org"))
	})

	It("maps dataset resolver outages to dependency errors", func() {
		httpClient := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return jsonResponse(http.StatusInternalServerError, `{"message":"downstream failed"}`), nil
		})}

		_, err := NewDatasetResolver("http://data-registry", httpClient).ResolveMaterializedDataset(context.Background(), uuid.New(), uuid.New(), uuid.New())

		Expect(errors.Is(err, domain.ErrDependencyFailed)).To(BeTrue())
	})

	It("maps dataset resolver transport failures to dependency errors", func() {
		httpClient := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, errors.New("connection refused")
		})}

		_, err := NewDatasetResolver("http://data-registry", httpClient).ResolveMaterializedDataset(context.Background(), uuid.New(), uuid.New(), uuid.New())

		Expect(errors.Is(err, domain.ErrDependencyFailed)).To(BeTrue())
	})
})

var _ = Describe("ModelResolver", func() {
	It("resolves trainable models and forwards the user and org ids", func() {
		userID := uuid.New()
		orgID := uuid.New()
		modelID := uuid.New()
		httpClient := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			Expect(req.Method).To(Equal(http.MethodGet))
			Expect(req.URL.Path).To(Equal("/v1/models/" + modelID.String()))
			Expect(req.Header.Get(userIDHeader)).To(Equal(userID.String()))
			Expect(req.Header.Get(orgIDHeader)).To(Equal(orgID.String()))
			return jsonResponse(http.StatusOK, `{
				"id":"`+modelID.String()+`",
				"user_id":"`+userID.String()+`",
				"org_id":"`+orgID.String()+`",
				"model_kind":"BASE",
				"name":"llama-3",
				"model_version":1,
				"base_model":"llama-3",
				"artifact_location":"s3://models/base",
				"artifact_checksum":"sha256:base",
				"serving_load_status":"LOADED",
				"status":"READY"
			}`), nil
		})}

		ref, err := NewModelResolver("http://model-registry", httpClient).ResolveTrainableModel(context.Background(), userID, orgID, modelID)

		Expect(err).NotTo(HaveOccurred())
		Expect(ref.ModelID).To(Equal(modelID.String()))
		Expect(ref.UserID).To(Equal(userID.String()))
		Expect(ref.OrgID).To(Equal(orgID.String()))
		Expect(ref.ModelKind).To(Equal("BASE"))
		Expect(ref.ArtifactLocation).To(Equal("s3://models/base"))
	})

	It("maps missing models to validation errors", func() {
		httpClient := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return jsonResponse(http.StatusNotFound, `not found`), nil
		})}

		_, err := NewModelResolver("http://model-registry", httpClient).ResolveTrainableModel(context.Background(), uuid.New(), uuid.New(), uuid.New())

		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
	})

	It("rejects source models that are not ready", func() {
		modelID := uuid.New()
		orgID := uuid.New()
		httpClient := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return jsonResponse(http.StatusOK, `{
				"id":"`+modelID.String()+`",
				"user_id":"`+uuid.NewString()+`",
				"org_id":"`+orgID.String()+`",
				"model_kind":"BASE",
				"name":"llama-3",
				"model_version":1,
				"base_model":"llama-3",
				"artifact_location":"s3://models/base",
				"artifact_checksum":"sha256:base",
				"status":"PENDING"
			}`), nil
		})}

		_, err := NewModelResolver("http://model-registry", httpClient).ResolveTrainableModel(context.Background(), uuid.New(), orgID, modelID)

		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
		Expect(err.Error()).To(ContainSubstring("source model is not ready"))
	})

	It("rejects fine-tuned source models without an adapter uri", func() {
		modelID := uuid.New()
		orgID := uuid.New()
		httpClient := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return jsonResponse(http.StatusOK, `{
				"id":"`+modelID.String()+`",
				"user_id":"`+uuid.NewString()+`",
				"org_id":"`+orgID.String()+`",
				"model_kind":"FINE_TUNED",
				"name":"ranker",
				"model_version":2,
				"base_model":"llama-3",
				"artifact_location":"s3://models/ranker",
				"artifact_checksum":"sha256:ranker",
				"status":"READY"
			}`), nil
		})}

		_, err := NewModelResolver("http://model-registry", httpClient).ResolveTrainableModel(context.Background(), uuid.New(), orgID, modelID)

		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
		Expect(err.Error()).To(ContainSubstring("adapter uri"))
	})

	It("maps model resolver outages to dependency errors", func() {
		httpClient := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return jsonResponse(http.StatusInternalServerError, `registry failed`), nil
		})}

		_, err := NewModelResolver("http://model-registry", httpClient).ResolveTrainableModel(context.Background(), uuid.New(), uuid.New(), uuid.New())

		Expect(errors.Is(err, domain.ErrDependencyFailed)).To(BeTrue())
	})

	It("maps malformed model resolver responses to dependency errors", func() {
		httpClient := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return jsonResponse(http.StatusOK, `{`), nil
		})}

		_, err := NewModelResolver("http://model-registry", httpClient).ResolveTrainableModel(context.Background(), uuid.New(), uuid.New(), uuid.New())

		Expect(errors.Is(err, domain.ErrDependencyFailed)).To(BeTrue())
	})
})

func jsonResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}
