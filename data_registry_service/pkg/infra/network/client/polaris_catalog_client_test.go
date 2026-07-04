package client

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	domainErrors "data_registry_service/pkg/domain"
	"data_registry_service/pkg/domain/model"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("PolarisCatalogClient", func() {
	var (
		ctx        context.Context
		client     *PolarisCatalogClient
		requestLog []string
	)

	BeforeEach(func() {
		ctx = context.Background()
		requestLog = nil
		httpClient := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			requestLog = append(requestLog, r.Method+" "+r.URL.Path)
			Expect(r.Context()).NotTo(BeNil())

			switch {
			case r.Method == http.MethodPost && r.URL.Path == "/api/catalog/v1/oauth/tokens":
				Expect(r.ParseForm()).To(Succeed())
				Expect(r.Form.Get("client_id")).To(Equal("root"))
				Expect(r.Form.Get("client_secret")).To(Equal("s3cr3t"))
				Expect(r.Form.Get("scope")).To(Equal("PRINCIPAL_ROLE:ALL"))
				return jsonResponse(http.StatusOK, map[string]any{"access_token": "token"}), nil
			case r.Method == http.MethodGet && r.URL.Path == "/api/catalog/v1/config":
				Expect(r.URL.Query().Get("warehouse")).To(Equal("s3://warehouse/"))
				return jsonResponse(http.StatusOK, map[string]any{"defaults": map[string]string{}, "overrides": map[string]string{}}), nil
			case r.Method == http.MethodHead && strings.HasPrefix(r.URL.Path, "/api/catalog/v1/bighill/namespaces/source_connector_"):
				return emptyResponse(http.StatusNotFound), nil
			case r.Method == http.MethodPost && r.URL.Path == "/api/catalog/v1/bighill/namespaces":
				var payload map[string]any
				Expect(json.NewDecoder(r.Body).Decode(&payload)).To(Succeed())
				Expect(payload["namespace"]).NotTo(BeEmpty())
				return jsonResponse(http.StatusOK, map[string]any{}), nil
			case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/api/catalog/v1/bighill/namespaces/source_connector_"):
				return emptyResponse(http.StatusNoContent), nil
			case r.Method == http.MethodHead && r.URL.Path == "/api/catalog/v1/bighill/namespaces/features":
				return emptyResponse(http.StatusOK), nil
			case r.Method == http.MethodHead && r.URL.Path == "/api/catalog/v1/bighill/namespaces/features/tables/movies":
				return emptyResponse(http.StatusOK), nil
			default:
				return jsonResponse(http.StatusNotFound, map[string]any{"error": r.Method + " " + r.URL.Path}), nil
			}
		})}
		client = NewPolarisCatalogClient(PolarisCatalogConfig{
			BaseURL:             "http://polaris.test",
			ClientID:            "root",
			ClientSecret:        "s3cr3t",
			Scope:               "PRINCIPAL_ROLE:ALL",
			Catalog:             "bighill",
			DefaultBaseLocation: "s3://warehouse/",
			StorageRegion:       "eu-west-1",
			StorageEndpoint:     "http://object-store:9000",
			StoragePathStyle:    true,
		}, httpClient)
	})

	It("ensures the Polaris catalog before creating source catalog resources", func() {
		resourceID := uuid.New()

		catalogID, err := client.CreateResource(ctx, resourceID.String(), &model.PostgresDBConnCfg{})

		Expect(err).NotTo(HaveOccurred())
		Expect(catalogID).To(Equal(resourceID))
		Expect(requestLog).To(ContainElement("GET /api/catalog/v1/config"))
		Expect(requestLog).To(ContainElement(MatchRegexp(`HEAD /api/catalog/v1/bighill/namespaces/source_connector_`)))
		Expect(requestLog).To(ContainElement("POST /api/catalog/v1/bighill/namespaces"))
	})

	It("deletes the Polaris namespace for a source catalog resource", func() {
		resourceID := uuid.New()

		err := client.DeleteResource(ctx, resourceID)

		Expect(err).NotTo(HaveOccurred())
		Expect(requestLog).To(ContainElement(MatchRegexp(`DELETE /api/catalog/v1/bighill/namespaces/source_connector_`)))
	})

	It("validates a materialized Polaris Iceberg table through the REST catalog", func() {
		dataset := &model.Dataset{
			ID:              uuid.New(),
			UserID:          uuid.New(),
			TableNamespace:  "features",
			TableName:       "movies",
			TableFormat:     model.Iceberg,
			CatalogProvider: model.PolarisCatalog,
		}

		Expect(client.ValidateDatasetTable(ctx, dataset)).To(Succeed())

		Expect(requestLog).To(ContainElement("HEAD /api/catalog/v1/bighill/namespaces/features"))
		Expect(requestLog).To(ContainElement("HEAD /api/catalog/v1/bighill/namespaces/features/tables/movies"))
	})

	It("fails validation when a Polaris Iceberg table is not registered", func() {
		missingClient := NewPolarisCatalogClient(PolarisCatalogConfig{
			BaseURL:             "http://polaris.test",
			ClientID:            "root",
			ClientSecret:        "s3cr3t",
			Catalog:             "bighill",
			DefaultBaseLocation: "s3://warehouse/",
		}, &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			switch {
			case r.Method == http.MethodPost && r.URL.Path == "/api/catalog/v1/oauth/tokens":
				return jsonResponse(http.StatusOK, map[string]any{"access_token": "token"}), nil
			case r.Method == http.MethodGet && r.URL.Path == "/api/catalog/v1/config":
				return jsonResponse(http.StatusOK, map[string]any{"defaults": map[string]string{}, "overrides": map[string]string{}}), nil
			case r.Method == http.MethodHead && r.URL.Path == "/api/catalog/v1/bighill/namespaces/features":
				return emptyResponse(http.StatusOK), nil
			case r.Method == http.MethodHead && strings.Contains(r.URL.Path, "/tables/missing"):
				return emptyResponse(http.StatusNotFound), nil
			default:
				return jsonResponse(http.StatusNotFound, map[string]any{"error": r.URL.Path}), nil
			}
		})})

		err := missingClient.ValidateDatasetTable(ctx, &model.Dataset{
			TableNamespace:  "features",
			TableName:       "missing",
			TableFormat:     model.Iceberg,
			CatalogProvider: model.PolarisCatalog,
		})

		Expect(err).To(MatchError(ContainSubstring("polaris iceberg table features.missing is not registered")))
		Expect(domainErrors.IsServiceError(err, domainErrors.ErrValidationFailed)).To(BeTrue())
	})

	It("skips catalog calls for local Parquet datasets", func() {
		Expect(client.ValidateDatasetTable(ctx, &model.Dataset{
			TableNamespace:  "features",
			TableName:       "movies",
			TableFormat:     model.Parquet,
			CatalogProvider: model.LocalCatalog,
		})).To(Succeed())

		Expect(requestLog).To(BeEmpty())
	})

	It("rejects inconsistent catalog and table-format metadata", func() {
		Expect(client.ValidateDatasetTable(ctx, &model.Dataset{
			TableNamespace:  "features",
			TableName:       "movies",
			TableFormat:     model.Parquet,
			CatalogProvider: model.PolarisCatalog,
		})).To(MatchError(ContainSubstring("polaris catalog requires iceberg table format")))

		Expect(client.ValidateDatasetTable(ctx, &model.Dataset{
			TableNamespace:  "features",
			TableName:       "movies",
			TableFormat:     model.Iceberg,
			CatalogProvider: model.LocalCatalog,
		})).To(MatchError(ContainSubstring("iceberg table format requires polaris catalog")))
		Expect(requestLog).To(BeEmpty())
	})
})

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func jsonResponse(status int, payload any) *http.Response {
	body, err := json.Marshal(payload)
	Expect(err).NotTo(HaveOccurred())
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(string(body))),
	}
}

func emptyResponse(status int) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader("")),
	}
}
