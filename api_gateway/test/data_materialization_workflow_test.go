package test

import (
	"net/http"
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
			"title":       "Movie Metrics Upload",
			"description": "CSV uploaded through the gateway and materialized by the feature pipeline",
			"category":    "movies",
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
		}, 45*time.Second, 1*time.Second).Should(Succeed())
	})

	It("rejects uploads for datasets that were not announced by the registry topic", func() {
		csv := []byte("title,views\nIntro,10\n")

		status, body := doMultipartFile(http.MethodPost, "/v1/data/store/"+uuid.NewString(), "file", "movies.csv", csv, user.Token, uuid.New())

		Expect(status).To(Equal(http.StatusNotFound), "body: %s", string(body))
	})
})
