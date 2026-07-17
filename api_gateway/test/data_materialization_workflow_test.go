package test

import (
	"encoding/json"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Data materialization workflow", Ordered, func() {
	var user profileTestUser

	BeforeAll(func() {
		user = createVerifiedProfileAndLogin()
	})

	It("materializes an uploaded dataset through service-owned Kafka topics", func() {
		createPayload := map[string]any{
			"title":             "Movie Metrics Upload",
			"description":       "CSV uploaded through the gateway and materialized by the feature pipeline",
			"category":          "movies",
			"tableNamespace":    "features",
			"tableName":         "movie_metrics_upload",
			"tableFormat":       "PARQUET",
			"catalogProvider":   "LOCAL",
			"processingProfile": "TEXT_RAG_PROCESSING_PROFILE",
		}

		created := createDataRegistryDataset(user, createPayload)
		datasetID := stringField(created, "id")

		csv := []byte("title,views\nIntro,10\nNext,20\n")
		Eventually(func() bool {
			status, _ := doMultipartFile(http.MethodPost, "/v1/private/data/store/"+datasetID, "file", "movies.csv", csv, user.Token, uuid.New())
			return status == http.StatusCreated
		}, 30*time.Second, 1*time.Second).Should(BeTrue())

		Eventually(func() bool {
			status, body := doJSON(http.MethodGet, "/v1/private/data/registry/"+datasetID, nil, user.Token, uuid.Nil)
			if status != http.StatusOK {
				return false
			}

			read := decodeObject(body)
			metadata, ok := schemaMetadataObjectOK(read)
			return ok &&
				isRAGDatasetProcessingStateReady(read["processingState"]) &&
				isFeatureParquetLocation(read["storageLocation"]) &&
				read["tableFormat"] == "PARQUET" &&
				read["catalogProvider"] == "LOCAL" &&
				numericAtLeast(read["schemaVersion"], 1) &&
				metadata["source_format"] == "csv" &&
				numericEqual(metadata["rows"], 2) &&
				dataMaterializationSchemaMetadataHasField(metadata, "title") &&
				dataMaterializationSchemaMetadataHasField(metadata, "views")
		}, 45*time.Second, 1*time.Second).Should(BeTrue())
	})

	It("uploads a downloaded open dataset and streams the materialized table through Data Stream Flight", func() {
		createPayload := map[string]any{
			"title":             "Open Iris Measurements",
			"description":       "Small open CSV fixture uploaded and queried through the lakehouse stream path",
			"category":          "flowers",
			"tableNamespace":    "features",
			"tableName":         "open_iris_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:8],
			"tableFormat":       "ICEBERG",
			"catalogProvider":   "POLARIS",
			"processingProfile": "GENERIC_PARQUET_PROCESSING_PROFILE",
		}

		created := createDataRegistryDataset(user, createPayload)
		datasetID := uuid.MustParse(stringField(created, "id"))

		openDataset, err := os.ReadFile("data/open_iris.csv")
		Expect(err).NotTo(HaveOccurred())
		Expect(openDataset).NotTo(BeEmpty())

		Eventually(func() bool {
			status, _ := doMultipartFile(http.MethodPost, "/v1/private/data/store/"+datasetID.String(), "file", "open_iris.csv", openDataset, user.Token, uuid.New())
			return status == http.StatusCreated
		}, 30*time.Second, 1*time.Second).Should(BeTrue())

		Eventually(func() bool {
			status, body := doJSON(http.MethodGet, "/v1/private/data/registry/"+datasetID.String(), nil, user.Token, uuid.Nil)
			if status != http.StatusOK {
				return false
			}

			read := decodeObject(body)
			metadata, ok := schemaMetadataObjectOK(read)
			return ok &&
				read["processingState"] == "FEATURE_MATERIALIZED" &&
				read["tableFormat"] == "ICEBERG" &&
				read["catalogProvider"] == "POLARIS" &&
				isFeatureParquetLocation(read["storageLocation"]) &&
				metadata["source_format"] == "csv" &&
				numericEqual(metadata["rows"], 150) &&
				dataMaterializationSchemaMetadataHasField(metadata, "sepal_length") &&
				dataMaterializationSchemaMetadataHasField(metadata, "species")
		}, 45*time.Second, 1*time.Second).Should(BeTrue())

		commandBytes, err := json.Marshal(map[string]string{
			"userId":    user.ID.String(),
			"orgId":     user.OrgID.String(),
			"datasetId": datasetID.String(),
			"sql":       "SELECT species, sepal_length FROM dataset ORDER BY sepal_length DESC LIMIT 2",
		})
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() bool {
			result := queryFlight(commandBytes)
			return result.RowCount == int64(2) &&
				len(result.Columns) == 2 &&
				result.Columns[0] == "species" &&
				result.Columns[1] == "sepal_length" &&
				result.FirstRow["species"] != "" &&
				numericApprox(result.FirstRow["sepal_length"], 7.9)
		}, 20*time.Second, 1*time.Second).Should(BeTrue())
	})

	It("materializes a presigned upload session through staging validation and promotion", func() {
		createPayload := map[string]any{
			"title":             "Presigned Movie Metrics Upload",
			"description":       "CSV uploaded through a presigned staging session and materialized by the feature pipeline",
			"category":          "movies",
			"tableNamespace":    "features",
			"tableName":         "presigned_movie_metrics_upload",
			"tableFormat":       "PARQUET",
			"catalogProvider":   "LOCAL",
			"processingProfile": "TEXT_RAG_PROCESSING_PROFILE",
		}

		created := createDataRegistryDataset(user, createPayload)
		datasetID := stringField(created, "id")

		csv := []byte("title,views\nPresigned Intro,30\nPresigned Next,40\n")
		var uploadID string
		var fields map[string]any
		Eventually(func() bool {
			initiatePayload := map[string]any{
				"file_name":           "presigned-movies.csv",
				"declared_format":     "csv",
				"content_type":        "text/csv",
				"declared_size_bytes": len(csv),
				"client_nonce":        "presigned-" + datasetID,
			}
			status, body := doJSON(http.MethodPost, "/v1/private/data/uploads/"+datasetID, initiatePayload, user.Token, uuid.New())
			if status != http.StatusCreated {
				return false
			}
			initiated := decodeObject(body)
			uploadID = stringField(initiated, "upload_id")
			var ok bool
			fields, ok = initiated["fields"].(map[string]any)
			return ok &&
				stringField(initiated, "url") == "local-s3://local-dev-bucket" &&
				strings.HasPrefix(stringField(fields, "key"), "staging/") &&
				fields["Content-Type"] == "text/csv"
		}, 30*time.Second, 1*time.Second).Should(BeTrue())

		writeLocalS3Object("local-dev-bucket", fields["key"].(string), "text/csv", csv)

		status, body := doJSON(http.MethodPost, "/v1/private/data/uploads/"+uploadID+"/complete", nil, user.Token, uuid.New())
		Expect(status).To(Equal(http.StatusCreated), "body: %s", string(body))
		completed := decodeObject(body)
		Expect(completed["status"]).To(Equal("PROMOTED"))
		Expect(completed["storage_location"]).To(MatchRegexp(`^s3://local-dev-bucket/raw/`))
		Expect(completed["actual_size_bytes"]).To(BeNumerically("==", len(csv)))

		Eventually(func() bool {
			status, body := doJSON(http.MethodGet, "/v1/private/data/registry/"+datasetID, nil, user.Token, uuid.Nil)
			if status != http.StatusOK {
				return false
			}

			read := decodeObject(body)
			metadata, ok := schemaMetadataObjectOK(read)
			return ok &&
				isRAGDatasetProcessingStateReady(read["processingState"]) &&
				isFeatureParquetLocation(read["storageLocation"]) &&
				read["tableFormat"] == "PARQUET" &&
				read["catalogProvider"] == "LOCAL" &&
				numericAtLeast(read["schemaVersion"], 1) &&
				metadata["source_format"] == "csv" &&
				numericEqual(metadata["rows"], 2) &&
				dataMaterializationSchemaMetadataHasField(metadata, "title") &&
				dataMaterializationSchemaMetadataHasField(metadata, "views")
		}, 45*time.Second, 1*time.Second).Should(BeTrue())
	})

	It("materializes an uploaded PDF dataset through service-owned Kafka topics", func() {
		createPayload := map[string]any{
			"title":             "PDF Knowledge Upload",
			"description":       "PDF uploaded through the gateway and materialized by the RAG feature pipeline",
			"category":          "documents",
			"tableNamespace":    "features",
			"tableName":         "pdf_knowledge_upload",
			"tableFormat":       "PARQUET",
			"catalogProvider":   "LOCAL",
			"processingProfile": "TEXT_RAG_PROCESSING_PROFILE",
		}

		created := createDataRegistryDataset(user, createPayload)
		datasetID := stringField(created, "id")

		pdf, err := os.ReadFile("data/example_PDF_1MB.pdf")
		Expect(err).NotTo(HaveOccurred())
		Expect(pdf).NotTo(BeEmpty())
		Eventually(func() bool {
			status, _ := doMultipartFile(http.MethodPost, "/v1/private/data/store/"+datasetID, "file", "example_PDF_1MB.pdf", pdf, user.Token, uuid.New())
			return status == http.StatusCreated
		}, 30*time.Second, 1*time.Second).Should(BeTrue())

		Eventually(func() bool {
			status, body := doJSON(http.MethodGet, "/v1/private/data/registry/"+datasetID, nil, user.Token, uuid.Nil)
			if status != http.StatusOK {
				return false
			}

			read := decodeObject(body)
			metadata, ok := schemaMetadataObjectOK(read)
			return ok &&
				isRAGDatasetProcessingStateReady(read["processingState"]) &&
				isFeatureParquetLocation(read["storageLocation"]) &&
				read["tableFormat"] == "PARQUET" &&
				read["catalogProvider"] == "LOCAL" &&
				read["processingProfile"] == "TEXT_RAG_PROCESSING_PROFILE" &&
				read["tableNamespace"] == "features" &&
				read["tableName"] == "pdf_knowledge_upload" &&
				numericAtLeast(read["schemaVersion"], 1) &&
				metadata["source_format"] == "pdf" &&
				numericGreater(metadata["source_page_count"], 0) &&
				metadata["extractor_name"] == "poppler-cpp-pdf-extractor" &&
				metadata["extractor_version"] == "v1" &&
				metadata["cleaner_name"] == "go-basic-text-cleaner" &&
				metadata["cleaner_version"] == "v1" &&
				numericAtLeast(metadata["rows"], 1) &&
				dataMaterializationSchemaMetadataHasField(metadata, "source_text")
		}, 45*time.Second, 1*time.Second).Should(BeTrue())
	})

	It("materializes an uploaded HTML dataset through service-owned Kafka topics", func() {
		createPayload := map[string]any{
			"title":             "HTML Knowledge Upload",
			"description":       "HTML uploaded through the gateway and materialized by the RAG feature pipeline",
			"category":          "documents",
			"tableNamespace":    "features",
			"tableName":         "html_knowledge_upload",
			"tableFormat":       "PARQUET",
			"catalogProvider":   "LOCAL",
			"processingProfile": "TEXT_RAG_PROCESSING_PROFILE",
		}

		created := createDataRegistryDataset(user, createPayload)
		datasetID := stringField(created, "id")

		html := []byte("<!doctype html><html><head><title>Ignored title</title><script>alert('x')</script></head><body><main><h1>Guide</h1><p>HTML knowledge content.</p></main></body></html>")
		Eventually(func() bool {
			status, _ := doMultipartFile(http.MethodPost, "/v1/private/data/store/"+datasetID, "file", "knowledge.html", html, user.Token, uuid.New())
			return status == http.StatusCreated
		}, 30*time.Second, 1*time.Second).Should(BeTrue())

		Eventually(func() bool {
			status, body := doJSON(http.MethodGet, "/v1/private/data/registry/"+datasetID, nil, user.Token, uuid.Nil)
			if status != http.StatusOK {
				return false
			}

			read := decodeObject(body)
			metadata, ok := schemaMetadataObjectOK(read)
			return ok &&
				isRAGDatasetProcessingStateReady(read["processingState"]) &&
				isFeatureParquetLocation(read["storageLocation"]) &&
				read["tableFormat"] == "PARQUET" &&
				read["catalogProvider"] == "LOCAL" &&
				read["processingProfile"] == "TEXT_RAG_PROCESSING_PROFILE" &&
				read["tableName"] == "html_knowledge_upload" &&
				numericAtLeast(read["schemaVersion"], 1) &&
				metadata["source_format"] == "html" &&
				metadata["extractor_name"] == "go-html-text-extractor" &&
				metadata["extractor_version"] == "v1" &&
				metadata["cleaner_name"] == "go-basic-text-cleaner" &&
				metadata["cleaner_version"] == "v1" &&
				numericAtLeast(metadata["rows"], 1) &&
				dataMaterializationSchemaMetadataHasField(metadata, "source_text")
		}, 45*time.Second, 1*time.Second).Should(BeTrue())
	})

	It("rejects uploads for datasets that were not announced by the registry topic", func() {
		csv := []byte("title,views\nIntro,10\n")

		status, body := doMultipartFile(http.MethodPost, "/v1/private/data/store/"+uuid.NewString(), "file", "movies.csv", csv, user.Token, uuid.New())

		Expect(status).To(Equal(http.StatusNotFound), "body: %s", string(body))
	})
})

func schemaMetadataObject(dataset map[string]any) map[string]any {
	metadata, ok := dataset["schemaMetadata"].(map[string]any)
	Expect(ok).To(BeTrue(), "schemaMetadata: %#v", dataset["schemaMetadata"])
	return metadata
}

func schemaMetadataObjectOK(dataset map[string]any) (map[string]any, bool) {
	metadata, ok := dataset["schemaMetadata"].(map[string]any)
	return metadata, ok
}

func expectSchemaField(metadata map[string]any, fieldName string) {
	Expect(dataMaterializationSchemaMetadataHasField(metadata, fieldName)).To(BeTrue(), "fields: %#v", metadata["fields"])
}

func dataMaterializationSchemaMetadataHasField(metadata map[string]any, fieldName string) bool {
	fields, ok := metadata["fields"].([]any)
	if !ok {
		return false
	}
	for _, field := range fields {
		fieldMap, ok := field.(map[string]any)
		if ok && fieldMap["name"] == fieldName {
			return true
		}
	}
	return false
}

func isRAGDatasetProcessingStateReady(value any) bool {
	switch value {
	case "EMBEDDINGS_MATERIALIZED", "GRAPH_MATERIALIZED":
		return true
	default:
		return false
	}
}

func isFeatureParquetLocation(value any) bool {
	location, ok := value.(string)
	return ok &&
		strings.HasPrefix(location, "s3://local-dev-bucket/lakehouse/features/") &&
		strings.HasSuffix(location, ".parquet")
}

func numericEqual(value any, expected int64) bool {
	return numericCompare(value, func(actual float64) bool {
		return actual == float64(expected)
	})
}

func numericAtLeast(value any, expected int64) bool {
	return numericCompare(value, func(actual float64) bool {
		return actual >= float64(expected)
	})
}

func numericGreater(value any, expected int64) bool {
	return numericCompare(value, func(actual float64) bool {
		return actual > float64(expected)
	})
}

func numericApprox(value any, expected float64) bool {
	return numericCompare(value, func(actual float64) bool {
		return math.Abs(actual-expected) < 0.000001
	})
}

func numericCompare(value any, compare func(float64) bool) bool {
	switch v := value.(type) {
	case int:
		return compare(float64(v))
	case int64:
		return compare(float64(v))
	case float64:
		return compare(v)
	case json.Number:
		parsed, err := v.Float64()
		return err == nil && compare(parsed)
	default:
		return false
	}
}

func writeLocalS3Object(bucket, key, contentType string, content []byte) {
	root := localS3RepoRoot()
	objectPath := filepath.Join(root, "tmp", "local_s3_storage", bucket, filepath.FromSlash(key))
	Expect(os.MkdirAll(filepath.Dir(objectPath), 0755)).To(Succeed())
	Expect(os.WriteFile(objectPath, content, 0600)).To(Succeed())
	metadata, err := json.Marshal(map[string]string{"content_type": contentType})
	Expect(err).NotTo(HaveOccurred())
	Expect(os.WriteFile(objectPath+".metadata.json", metadata, 0600)).To(Succeed())
}

func readLocalS3ObjectURI(uri string) []byte {
	trimmed := strings.TrimPrefix(strings.TrimSpace(uri), "s3://")
	parts := strings.SplitN(trimmed, "/", 2)
	Expect(parts).To(HaveLen(2), "invalid local s3 uri: %s", uri)
	root := localS3RepoRoot()
	objectPath := filepath.Join(root, "tmp", "local_s3_storage", parts[0], filepath.FromSlash(parts[1]))
	content, err := os.ReadFile(objectPath)
	Expect(err).NotTo(HaveOccurred())
	return content
}

func readLocalS3ObjectURIIfExists(uri string) []byte {
	trimmed := strings.TrimPrefix(strings.TrimSpace(uri), "s3://")
	parts := strings.SplitN(trimmed, "/", 2)
	Expect(parts).To(HaveLen(2), "invalid local s3 uri: %s", uri)
	root := localS3RepoRoot()
	objectPath := filepath.Join(root, "tmp", "local_s3_storage", parts[0], filepath.FromSlash(parts[1]))
	content, err := os.ReadFile(objectPath)
	if os.IsNotExist(err) {
		return nil
	}
	Expect(err).NotTo(HaveOccurred())
	return content
}

func localS3RepoRoot() string {
	dir, err := os.Getwd()
	Expect(err).NotTo(HaveOccurred())
	for {
		if _, err := os.Stat(filepath.Join(dir, "shared_lib")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			Fail("failed to find repository root for local S3 storage")
		}
		dir = parent
	}
}
