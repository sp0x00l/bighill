package test

import (
	"net/http"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Data Registry API", Ordered, func() {
	var (
		user      profileTestUser
		datasetID string
	)

	BeforeAll(func() {
		user = createVerifiedProfileAndLogin()
	})

	It("creates and reads an authenticated user dataset", func() {
		payload := map[string]any{
			"title":       "Customer Churn Training Data",
			"description": "Synthetic customer churn records for model training",
			"location":    "gs://bighill-mlops-fixtures/churn.csv",
			"category":    "training",
		}

		status, body := doJSON(http.MethodPost, "/v1/data/registry", payload, user.Token, uuid.New())
		Expect(status).To(Equal(http.StatusCreated), "body: %s", string(body))

		created := decodeObject(body)
		datasetID = stringField(created, "id")
		Expect(created["userId"]).To(Equal(user.ID.String()))
		Expect(created["title"]).To(Equal("Customer Churn Training Data"))
		Expect(created["status"]).To(Equal("draft"))

		status, body = doJSON(http.MethodGet, "/v1/data/registry/"+datasetID, nil, user.Token, uuid.Nil)
		Expect(status).To(Equal(http.StatusOK), "body: %s", string(body))
		read := decodeObject(body)
		Expect(read["id"]).To(Equal(datasetID))
		Expect(read["userId"]).To(Equal(user.ID.String()))
	})

	It("rejects invalid dataset payloads before persistence", func() {
		status, body := doJSON(http.MethodPost, "/v1/data/registry", map[string]any{}, user.Token, uuid.New())
		Expect(status).To(Equal(http.StatusBadRequest), "body: %s", string(body))
	})

	It("lists, replaces, publishes, and reads the user's published dataset", func() {
		status, body := doJSON(http.MethodGet, "/v1/data/registry?limit=10&page=1", nil, user.Token, uuid.Nil)
		Expect(status).To(Equal(http.StatusOK), "body: %s", string(body))
		list := decodeObject(body)
		Expect(list).To(HaveKey("metadata"))
		Expect(list).To(HaveKey("resources"))

		updatePayload := map[string]any{
			"title":       "Customer Churn Feature Set",
			"description": "Curated churn features for supervised training",
			"origin":      "community",
			"location":    "gs://bighill-mlops-fixtures/churn-features.parquet",
			"category":    "features",
		}
		status, body = doJSON(http.MethodPut, "/v1/data/registry/"+datasetID, updatePayload, user.Token, uuid.Nil)
		Expect(status).To(Equal(http.StatusOK), "body: %s", string(body))
		updated := decodeObject(body)
		Expect(updated["title"]).To(Equal("Customer Churn Feature Set"))
		Expect(updated["origin"]).To(Equal("community"))

		status, body = doJSON(http.MethodPatch, "/v1/data/registry/"+datasetID+"/publish", nil, user.Token, uuid.Nil)
		Expect(status).To(Equal(http.StatusOK), "body: %s", string(body))

		status, body = doJSON(http.MethodGet, "/v1/data/registry/"+datasetID, nil, user.Token, uuid.Nil)
		Expect(status).To(Equal(http.StatusOK), "body: %s", string(body))
		published := decodeObject(body)
		Expect(published["id"]).To(Equal(datasetID))
		Expect(published["status"]).To(Equal("published"))
	})

	It("validates connector payloads without creating an external connector", func() {
		status, body := doJSON(http.MethodPost, "/v1/data/registry/connector/postgres", map[string]any{}, user.Token, uuid.New())
		Expect(status).To(Equal(http.StatusBadRequest), "body: %s", string(body))
	})
})
