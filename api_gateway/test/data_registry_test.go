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

		status, body := doJSON(http.MethodPost, "/v1/private/data/registry", payload, user.Token, uuid.New())
		Expect(status).To(Equal(http.StatusCreated), "body: %s", string(body))

		created := decodeObject(body)
		datasetID = stringField(created, "id")
		Expect(created["userId"]).To(Equal(user.ID.String()))
		Expect(created["title"]).To(Equal("Customer Churn Training Data"))
		Expect(created["status"]).To(Equal("draft"))

		status, body = doJSON(http.MethodGet, "/v1/private/data/registry/"+datasetID, nil, user.Token, uuid.Nil)
		Expect(status).To(Equal(http.StatusOK), "body: %s", string(body))
		read := decodeObject(body)
		Expect(read["id"]).To(Equal(datasetID))
		Expect(read["userId"]).To(Equal(user.ID.String()))
	})

	It("rejects invalid dataset payloads before persistence", func() {
		status, body := doJSON(http.MethodPost, "/v1/private/data/registry", map[string]any{}, user.Token, uuid.New())
		Expect(status).To(Equal(http.StatusBadRequest), "body: %s", string(body))
	})

	It("lists, replaces, publishes, and reads the user's published dataset", func() {
		status, body := doJSON(http.MethodGet, "/v1/private/data/registry?limit=10&page=1", nil, user.Token, uuid.Nil)
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
		status, body = doJSON(http.MethodPut, "/v1/private/data/registry/"+datasetID, updatePayload, user.Token, uuid.Nil)
		Expect(status).To(Equal(http.StatusOK), "body: %s", string(body))
		updated := decodeObject(body)
		Expect(updated["title"]).To(Equal("Customer Churn Feature Set"))
		Expect(updated["origin"]).To(Equal("community"))

		status, body = doJSON(http.MethodPatch, "/v1/private/data/registry/"+datasetID+"/publish", nil, user.Token, uuid.Nil)
		Expect(status).To(Equal(http.StatusOK), "body: %s", string(body))

		status, body = doJSON(http.MethodGet, "/v1/private/data/registry/"+datasetID, nil, user.Token, uuid.Nil)
		Expect(status).To(Equal(http.StatusOK), "body: %s", string(body))
		published := decodeObject(body)
		Expect(published["id"]).To(Equal(datasetID))
		Expect(published["status"]).To(Equal("published"))
	})

	It("does not expose a published dataset to another tenant", func() {
		otherUser := createVerifiedProfileAndLogin()
		otherPayload := map[string]any{
			"title":       "Other Tenant Dataset",
			"description": "Dataset owned by the second tenant",
			"location":    "gs://bighill-mlops-fixtures/other-tenant.csv",
			"category":    "training",
		}
		status, body := doJSON(http.MethodPost, "/v1/private/data/registry", otherPayload, otherUser.Token, uuid.New())
		Expect(status).To(Equal(http.StatusCreated), "body: %s", string(body))
		otherDatasetID := stringField(decodeObject(body), "id")

		status, body = doJSON(http.MethodGet, "/v1/private/data/registry/"+datasetID, nil, otherUser.Token, uuid.Nil)
		Expect(status).To(SatisfyAny(Equal(http.StatusForbidden), Equal(http.StatusNotFound)), "body: %s", string(body))

		status, body = doJSON(http.MethodGet, "/v1/private/data/registry?limit=25&page=1", nil, otherUser.Token, uuid.Nil)
		Expect(status).To(Equal(http.StatusOK), "body: %s", string(body))
		list := decodeObject(body)
		resources, ok := list["resources"].([]any)
		Expect(ok).To(BeTrue(), "resources: %#v", list["resources"])
		Expect(resources).NotTo(BeEmpty())
		seenOtherDataset := false
		for _, resource := range resources {
			item, ok := resource.(map[string]any)
			Expect(ok).To(BeTrue(), "resource: %#v", resource)
			Expect(item["id"]).NotTo(Equal(datasetID))
			if item["id"] == otherDatasetID {
				seenOtherDataset = true
			}
		}
		Expect(seenOtherDataset).To(BeTrue())
	})

	It("validates connector payloads without creating an external connector", func() {
		status, body := doJSON(http.MethodPost, "/v1/private/data/registry/connector/postgres", map[string]any{}, user.Token, uuid.New())
		Expect(status).To(Equal(http.StatusBadRequest), "body: %s", string(body))
	})
})
