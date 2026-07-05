package test

import (
	"encoding/json"
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

		status, body := doJSON(http.MethodPost, "/v1/data/registry", createPayload, user.Token, uuid.New())
		Expect(status).To(Equal(http.StatusCreated), "body: %s", string(body))
		created := decodeObject(body)
		datasetID := stringField(created, "id")

		csv := []byte("title,views\nIntro,10\nNext,20\n")
		Eventually(func(g Gomega) {
			status, body := doMultipartFile(http.MethodPost, "/v1/data/store/"+datasetID, "file", "movies.csv", csv, user.Token, uuid.New())
			g.Expect(status).To(Equal(http.StatusCreated), "body: %s", string(body))
		}, 30*time.Second, 1*time.Second).Should(Succeed())

		Eventually(func(g Gomega) {
			status, body := doJSON(http.MethodGet, "/v1/data/registry/"+datasetID, nil, user.Token, uuid.Nil)
			g.Expect(status).To(Equal(http.StatusOK), "body: %s", string(body))

			read := decodeObject(body)
			g.Expect(read["processingState"]).To(Equal("EMBEDDINGS_MATERIALIZED"))
			g.Expect(read["storageLocation"]).To(MatchRegexp(`^s3://local-dev-bucket/lakehouse/features/.+\.parquet$`))
			g.Expect(read["tableFormat"]).To(Equal("PARQUET"))
			g.Expect(read["catalogProvider"]).To(Equal("LOCAL"))
			g.Expect(read["schemaVersion"]).To(BeNumerically(">=", 1))
			metadata := schemaMetadataObject(g, read)
			g.Expect(metadata["source_format"]).To(Equal("csv"))
			g.Expect(metadata["rows"]).To(BeNumerically("==", 2))
			expectSchemaField(g, metadata, "title")
			expectSchemaField(g, metadata, "views")
		}, 45*time.Second, 1*time.Second).Should(Succeed())
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

		status, body := doJSON(http.MethodPost, "/v1/data/registry", createPayload, user.Token, uuid.New())
		Expect(status).To(Equal(http.StatusCreated), "body: %s", string(body))
		created := decodeObject(body)
		datasetID := uuid.MustParse(stringField(created, "id"))

		openDataset, err := os.ReadFile("data/open_iris.csv")
		Expect(err).NotTo(HaveOccurred())
		Expect(openDataset).NotTo(BeEmpty())

		Eventually(func(g Gomega) {
			status, body := doMultipartFile(http.MethodPost, "/v1/data/store/"+datasetID.String(), "file", "open_iris.csv", openDataset, user.Token, uuid.New())
			g.Expect(status).To(Equal(http.StatusCreated), "body: %s", string(body))
		}, 30*time.Second, 1*time.Second).Should(Succeed())

		Eventually(func(g Gomega) {
			status, body := doJSON(http.MethodGet, "/v1/data/registry/"+datasetID.String(), nil, user.Token, uuid.Nil)
			g.Expect(status).To(Equal(http.StatusOK), "body: %s", string(body))

			read := decodeObject(body)
			g.Expect(read["processingState"]).To(Equal("FEATURE_MATERIALIZED"))
			g.Expect(read["tableFormat"]).To(Equal("ICEBERG"))
			g.Expect(read["catalogProvider"]).To(Equal("POLARIS"))
			g.Expect(read["storageLocation"]).To(MatchRegexp(`^s3://local-dev-bucket/lakehouse/features/.+\.parquet$`))
			metadata := schemaMetadataObject(g, read)
			g.Expect(metadata["source_format"]).To(Equal("csv"))
			g.Expect(metadata["rows"]).To(BeNumerically("==", 150))
			expectSchemaField(g, metadata, "sepal_length")
			expectSchemaField(g, metadata, "species")
		}, 45*time.Second, 1*time.Second).Should(Succeed())

		commandBytes, err := json.Marshal(map[string]string{
			"userId":    user.ID.String(),
			"datasetId": datasetID.String(),
			"sql":       "SELECT species, sepal_length FROM dataset ORDER BY sepal_length DESC LIMIT 2",
		})
		Expect(err).NotTo(HaveOccurred())

		Eventually(func(g Gomega) {
			result := queryFlight(commandBytes)
			g.Expect(result.RowCount).To(Equal(int64(2)))
			g.Expect(result.Columns).To(Equal([]string{"species", "sepal_length"}))
			g.Expect(result.FirstRow["species"]).NotTo(BeEmpty())
			g.Expect(result.FirstRow["sepal_length"]).To(BeNumerically("==", 7.9))
		}, 20*time.Second, 1*time.Second).Should(Succeed())
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

		status, body := doJSON(http.MethodPost, "/v1/data/registry", createPayload, user.Token, uuid.New())
		Expect(status).To(Equal(http.StatusCreated), "body: %s", string(body))
		created := decodeObject(body)
		datasetID := stringField(created, "id")

		csv := []byte("title,views\nPresigned Intro,30\nPresigned Next,40\n")
		var uploadID string
		var fields map[string]any
		Eventually(func(g Gomega) {
			initiatePayload := map[string]any{
				"file_name":           "presigned-movies.csv",
				"declared_format":     "csv",
				"content_type":        "text/csv",
				"declared_size_bytes": len(csv),
				"client_nonce":        "presigned-" + datasetID,
			}
			status, body := doJSON(http.MethodPost, "/v1/data/uploads/"+datasetID, initiatePayload, user.Token, uuid.New())
			g.Expect(status).To(Equal(http.StatusCreated), "body: %s", string(body))
			initiated := decodeObject(body)
			uploadID = stringField(initiated, "upload_id")
			g.Expect(stringField(initiated, "url")).To(Equal("local-s3://local-dev-bucket"))
			var ok bool
			fields, ok = initiated["fields"].(map[string]any)
			g.Expect(ok).To(BeTrue(), "fields: %#v", initiated["fields"])
			g.Expect(fields).To(HaveKeyWithValue("key", MatchRegexp(`^staging/`)))
			g.Expect(fields).To(HaveKeyWithValue("Content-Type", "text/csv"))
		}, 30*time.Second, 1*time.Second).Should(Succeed())

		writeLocalS3Object("local-dev-bucket", fields["key"].(string), "text/csv", csv)

		status, body = doJSON(http.MethodPost, "/v1/data/uploads/"+uploadID+"/complete", nil, user.Token, uuid.New())
		Expect(status).To(Equal(http.StatusCreated), "body: %s", string(body))
		completed := decodeObject(body)
		Expect(completed["status"]).To(Equal("PROMOTED"))
		Expect(completed["storage_location"]).To(MatchRegexp(`^s3://local-dev-bucket/raw/`))
		Expect(completed["actual_size_bytes"]).To(BeNumerically("==", len(csv)))

		Eventually(func(g Gomega) {
			status, body := doJSON(http.MethodGet, "/v1/data/registry/"+datasetID, nil, user.Token, uuid.Nil)
			g.Expect(status).To(Equal(http.StatusOK), "body: %s", string(body))

			read := decodeObject(body)
			g.Expect(read["processingState"]).To(Equal("EMBEDDINGS_MATERIALIZED"))
			g.Expect(read["storageLocation"]).To(MatchRegexp(`^s3://local-dev-bucket/lakehouse/features/.+\.parquet$`))
			g.Expect(read["tableFormat"]).To(Equal("PARQUET"))
			g.Expect(read["catalogProvider"]).To(Equal("LOCAL"))
			g.Expect(read["schemaVersion"]).To(BeNumerically(">=", 1))
			metadata := schemaMetadataObject(g, read)
			g.Expect(metadata["source_format"]).To(Equal("csv"))
			g.Expect(metadata["rows"]).To(BeNumerically("==", 2))
			expectSchemaField(g, metadata, "title")
			expectSchemaField(g, metadata, "views")
		}, 45*time.Second, 1*time.Second).Should(Succeed())
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

		status, body := doJSON(http.MethodPost, "/v1/data/registry", createPayload, user.Token, uuid.New())
		Expect(status).To(Equal(http.StatusCreated), "body: %s", string(body))
		created := decodeObject(body)
		datasetID := stringField(created, "id")

		pdf, err := os.ReadFile("data/example_PDF_1MB.pdf")
		Expect(err).NotTo(HaveOccurred())
		Expect(pdf).NotTo(BeEmpty())
		Eventually(func(g Gomega) {
			status, body := doMultipartFile(http.MethodPost, "/v1/data/store/"+datasetID, "file", "example_PDF_1MB.pdf", pdf, user.Token, uuid.New())
			g.Expect(status).To(Equal(http.StatusCreated), "body: %s", string(body))
		}, 30*time.Second, 1*time.Second).Should(Succeed())

		Eventually(func(g Gomega) {
			status, body := doJSON(http.MethodGet, "/v1/data/registry/"+datasetID, nil, user.Token, uuid.Nil)
			g.Expect(status).To(Equal(http.StatusOK), "body: %s", string(body))

			read := decodeObject(body)
			g.Expect(read["processingState"]).To(Equal("EMBEDDINGS_MATERIALIZED"))
			g.Expect(read["storageLocation"]).To(MatchRegexp(`^s3://local-dev-bucket/lakehouse/features/.+\.parquet$`))
			g.Expect(read["tableFormat"]).To(Equal("PARQUET"))
			g.Expect(read["catalogProvider"]).To(Equal("LOCAL"))
			g.Expect(read["processingProfile"]).To(Equal("TEXT_RAG_PROCESSING_PROFILE"))
			g.Expect(read["tableNamespace"]).To(Equal("features"))
			g.Expect(read["tableName"]).To(Equal("pdf_knowledge_upload"))
			g.Expect(read["schemaVersion"]).To(BeNumerically(">=", 1))
			metadata := schemaMetadataObject(g, read)
			g.Expect(metadata["source_format"]).To(Equal("pdf"))
			g.Expect(metadata["source_page_count"]).To(BeNumerically(">", 0))
			g.Expect(metadata["extractor_name"]).To(Equal("poppler-cpp-pdf-extractor"))
			g.Expect(metadata["extractor_version"]).To(Equal("v1"))
			g.Expect(metadata["cleaner_name"]).To(Equal("go-basic-text-cleaner"))
			g.Expect(metadata["cleaner_version"]).To(Equal("v1"))
			g.Expect(metadata["rows"]).To(BeNumerically(">=", 1))
			expectSchemaField(g, metadata, "source_text")
		}, 45*time.Second, 1*time.Second).Should(Succeed())
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

		status, body := doJSON(http.MethodPost, "/v1/data/registry", createPayload, user.Token, uuid.New())
		Expect(status).To(Equal(http.StatusCreated), "body: %s", string(body))
		created := decodeObject(body)
		datasetID := stringField(created, "id")

		html := []byte("<!doctype html><html><head><title>Ignored title</title><script>alert('x')</script></head><body><main><h1>Guide</h1><p>HTML knowledge content.</p></main></body></html>")
		Eventually(func(g Gomega) {
			status, body := doMultipartFile(http.MethodPost, "/v1/data/store/"+datasetID, "file", "knowledge.html", html, user.Token, uuid.New())
			g.Expect(status).To(Equal(http.StatusCreated), "body: %s", string(body))
		}, 30*time.Second, 1*time.Second).Should(Succeed())

		Eventually(func(g Gomega) {
			status, body := doJSON(http.MethodGet, "/v1/data/registry/"+datasetID, nil, user.Token, uuid.Nil)
			g.Expect(status).To(Equal(http.StatusOK), "body: %s", string(body))

			read := decodeObject(body)
			g.Expect(read["processingState"]).To(Equal("EMBEDDINGS_MATERIALIZED"))
			g.Expect(read["storageLocation"]).To(MatchRegexp(`^s3://local-dev-bucket/lakehouse/features/.+\.parquet$`))
			g.Expect(read["tableFormat"]).To(Equal("PARQUET"))
			g.Expect(read["catalogProvider"]).To(Equal("LOCAL"))
			g.Expect(read["processingProfile"]).To(Equal("TEXT_RAG_PROCESSING_PROFILE"))
			g.Expect(read["tableName"]).To(Equal("html_knowledge_upload"))
			g.Expect(read["schemaVersion"]).To(BeNumerically(">=", 1))
			metadata := schemaMetadataObject(g, read)
			g.Expect(metadata["source_format"]).To(Equal("html"))
			g.Expect(metadata["extractor_name"]).To(Equal("go-html-text-extractor"))
			g.Expect(metadata["extractor_version"]).To(Equal("v1"))
			g.Expect(metadata["cleaner_name"]).To(Equal("go-basic-text-cleaner"))
			g.Expect(metadata["cleaner_version"]).To(Equal("v1"))
			g.Expect(metadata["rows"]).To(BeNumerically(">=", 1))
			expectSchemaField(g, metadata, "source_text")
		}, 45*time.Second, 1*time.Second).Should(Succeed())
	})

	It("rejects uploads for datasets that were not announced by the registry topic", func() {
		csv := []byte("title,views\nIntro,10\n")

		status, body := doMultipartFile(http.MethodPost, "/v1/data/store/"+uuid.NewString(), "file", "movies.csv", csv, user.Token, uuid.New())

		Expect(status).To(Equal(http.StatusNotFound), "body: %s", string(body))
	})
})

func schemaMetadataObject(g Gomega, dataset map[string]any) map[string]any {
	metadata, ok := dataset["schemaMetadata"].(map[string]any)
	g.Expect(ok).To(BeTrue(), "schemaMetadata: %#v", dataset["schemaMetadata"])
	return metadata
}

func expectSchemaField(g Gomega, metadata map[string]any, fieldName string) {
	fields, ok := metadata["fields"].([]any)
	g.Expect(ok).To(BeTrue(), "fields: %#v", metadata["fields"])
	for _, field := range fields {
		fieldMap, ok := field.(map[string]any)
		g.Expect(ok).To(BeTrue(), "field: %#v", field)
		if fieldMap["name"] == fieldName {
			return
		}
	}
	g.Expect(fields).To(ContainElement(HaveKeyWithValue("name", fieldName)))
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
