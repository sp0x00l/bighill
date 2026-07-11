package test

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	inferencepb "lib/data_contracts_lib/inference"
	ingestionpb "lib/data_contracts_lib/ingestion"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Multi-LoRA serving control plane", Ordered, func() {
	var user profileTestUser
	var datasetID string
	var baseModelID uuid.UUID
	var baseModel map[string]any
	var firstAdapterID uuid.UUID
	var secondAdapterID uuid.UUID

	BeforeAll(func() {
		user = createVerifiedProfileAndLogin()
		datasetID = createRAGInferenceDataset(user)
		materializeRAGInferenceDataset(user, datasetID)
		baseModelID = uploadBaseModelThroughIngestion(user, datasetID)
		baseModel = assertModelSelectable(user, baseModelID, "UPLOAD", "rag-e2e-uploaded-base")
	})

	It("P1 serves a tenant-owned base model before adapter loading", func() {
		client, closeClient := newInferenceClient()
		defer closeClient()

		var response *inferencepb.GenerateResponse
		Eventually(func(g Gomega) {
			ctx, cancel := context.WithTimeout(context.Background(), ragE2EGenerateCallTimeout)
			defer cancel()

			var err error
			response, err = client.Generate(ctx, &inferencepb.GenerateRequest{
				RequestId: uuid.NewString(),
				UserId:    user.ID.String(),
				OrgId:     user.OrgID.String(),
				DatasetId: datasetID,
				ModelId:   baseModelID.String(),
				QueryText: "What phrase identifies the embedded knowledge base?",
				TopK:      3,
			})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.TrimSpace(response.GetAnswer())).NotTo(BeEmpty())
			expectRAGVerificationContext(g, response)
		}, ragE2EGenerateWaitTimeout, 1*time.Second).Should(Succeed())

		Expect(response.GetGenerationModel()).To(Equal(stringField(baseModel, "serving_model")))
		Expect(response.GetGenerationProtocol()).To(Equal(stringField(baseModel, "serving_protocol")))
	})

	It("P2 uploads a LoRA adapter and refuses silent base fallback", func() {
		firstAdapterID = uploadPEFTAdapterThroughIngestion(user, datasetID, ragE2EBaseModel(), "rag-e2e-lora-one", "2")
		adapter := waitForUploadedAdapterModel(user, firstAdapterID, "rag-e2e-lora-one")

		Expect(adapter["model_kind"]).To(Equal("FINE_TUNED"))
		Expect(adapter["adapter_rank"]).To(BeNumerically("==", 16))
		Expect(adapter["base_model"]).To(Equal(ragE2EBaseModel()))
		Expect(adapter["serving_load_status"]).NotTo(Equal("LOADED"))
		Expect(adapter["serving_model"]).NotTo(Equal(ragE2EBaseModel()))
	})

	It("P3 registers a second tenant adapter on the same base without collision", func() {
		secondAdapterID = uploadPEFTAdapterThroughIngestion(user, datasetID, ragE2EBaseModel(), "rag-e2e-lora-two", "3")
		first := waitForUploadedAdapterModel(user, firstAdapterID, "rag-e2e-lora-one")
		second := waitForUploadedAdapterModel(user, secondAdapterID, "rag-e2e-lora-two")

		Expect(first["id"]).NotTo(Equal(second["id"]))
		Expect(first["base_model"]).To(Equal(second["base_model"]))
		Expect(first["adapter_uri"]).NotTo(BeEmpty())
		Expect(second["adapter_uri"]).NotTo(BeEmpty())
		Expect(first["adapter_rank"]).To(BeNumerically("==", 16))
		Expect(second["adapter_rank"]).To(BeNumerically("==", 16))
	})

	It("P4 does not publish ready inference endpoints for unserved adapters", func() {
		assertNoReadyEndpointForModelName(user, "rag-e2e-lora-one")
		assertNoReadyEndpointForModelName(user, "rag-e2e-lora-two")
	})

	It("P5 fails generation against an unserved adapter instead of answering with the base model", func() {
		client, closeClient := newInferenceClient()
		defer closeClient()
		ctx, cancel := context.WithTimeout(context.Background(), ragE2EGenerateCallTimeout)
		defer cancel()

		_, err := client.Generate(ctx, &inferencepb.GenerateRequest{
			RequestId: uuid.NewString(),
			UserId:    user.ID.String(),
			OrgId:     user.OrgID.String(),
			DatasetId: datasetID,
			ModelId:   firstAdapterID.String(),
			QueryText: "What phrase identifies the embedded knowledge base?",
			TopK:      3,
		})

		Expect(err).To(HaveOccurred())
	})

	It("P6 keeps tenant adapter models private to their owner org", func() {
		otherUser := createVerifiedProfileAndLogin()

		status, body := doJSON(http.MethodGet, "/v1/private/models/"+firstAdapterID.String(), nil, otherUser.Token, uuid.Nil)

		Expect(status).To(BeElementOf(http.StatusForbidden, http.StatusNotFound), "body: %s", string(body))
	})
})

func uploadPEFTAdapterThroughIngestion(user profileTestUser, datasetID string, baseModel string, name string, version string) uuid.UUID {
	archive := minimalPEFTAdapterArchive(16)
	modelEvents, stopModelEvents := newModelArtifactIngestedEventCollector()
	defer stopModelEvents()

	initiatePayload := map[string]any{
		"file_name":           name + ".zip",
		"dataset_id":          datasetID,
		"artifact_type":       "LORA_ADAPTER",
		"artifact_format":     "HF_PEFT_ADAPTER",
		"content_type":        "application/zip",
		"declared_size_bytes": len(archive),
		"client_nonce":        name + "-" + uuid.NewString(),
		"model_name":          name,
		"model_version":       version,
		"base_model":          baseModel,
	}

	var uploadID string
	var resourceID uuid.UUID
	var fields map[string]any
	Eventually(func(g Gomega) {
		status, body := doJSON(http.MethodPost, "/v1/private/models/uploads", initiatePayload, user.Token, uuid.New())
		g.Expect(status).To(Equal(http.StatusCreated), "body: %s", string(body))
		initiated := decodeObject(body)
		uploadID = stringField(initiated, "upload_id")
		parsedResourceID, err := uuid.Parse(stringField(initiated, "resource_id"))
		g.Expect(err).NotTo(HaveOccurred())
		resourceID = parsedResourceID
		var ok bool
		fields, ok = initiated["fields"].(map[string]any)
		g.Expect(ok).To(BeTrue(), "fields: %#v", initiated["fields"])
	}, 30*time.Second, 1*time.Second).Should(Succeed())

	writeLocalS3Object("local-dev-bucket", fields["key"].(string), "application/zip", archive)

	status, body := doJSON(http.MethodPost, "/v1/private/models/uploads/"+uploadID+"/complete", nil, user.Token, uuid.New())
	Expect(status).To(Equal(http.StatusCreated), "body: %s", string(body))
	completed := decodeObject(body)
	Expect(completed["resource_id"]).To(Equal(resourceID.String()))
	Expect(completed["dataset_id"]).To(Equal(datasetID))
	Expect(completed["artifact_type"]).To(Equal("LORA_ADAPTER"))
	Expect(completed["artifact_format"]).To(Equal("hf_peft_adapter"))
	Expect(completed["model_name"]).To(Equal(name))
	Expect(completed["model_version"]).To(Equal(version))
	Expect(completed["base_model"]).To(Equal(baseModel))

	modelEvents.waitFor(resourceID, 30*time.Second, func(event *ingestionpb.ModelArtifactIngestedEvent) bool {
		return event.GetArtifactId() == resourceID.String() &&
			event.GetDatasetId() == datasetID &&
			event.GetOrgId() == user.OrgID.String() &&
			event.GetUserId() == user.ID.String() &&
			event.GetAdapterRank() == 16
	})
	return resourceID
}

func waitForUploadedAdapterModel(user profileTestUser, modelID uuid.UUID, name string) map[string]any {
	var read map[string]any
	Eventually(func(g Gomega) {
		status, body := doJSONWithTimeout(http.MethodGet, "/v1/private/models/"+modelID.String(), nil, user.Token, uuid.Nil, ragE2EModelPollTimeout)
		g.Expect(status).To(Equal(http.StatusOK), "body: %s", string(body))
		read = decodeObject(body)
		g.Expect(read).To(SatisfyAll(
			HaveKeyWithValue("id", modelID.String()),
			HaveKeyWithValue("name", name),
			HaveKeyWithValue("source", "UPLOAD"),
			HaveKeyWithValue("model_kind", "FINE_TUNED"),
			HaveKeyWithValue("adapter_rank", BeNumerically("==", 16)),
		))
	}, 75*time.Second, 1*time.Second).Should(Succeed())
	return read
}

func assertNoReadyEndpointForModelName(user profileTestUser, displayName string) {
	Consistently(func(g Gomega) {
		status, body := doJSON(http.MethodGet, "/v1/private/inference/endpoints", nil, user.Token, uuid.Nil)
		g.Expect(status).To(Equal(http.StatusOK), "body: %s", string(body))
		var endpoints []map[string]any
		g.Expect(json.Unmarshal(body, &endpoints)).To(Succeed())
		for _, endpoint := range endpoints {
			if endpoint["display_name"] == displayName {
				g.Expect(endpoint["status"]).NotTo(Equal("ready"))
			}
		}
	}, 5*time.Second, 1*time.Second).Should(Succeed())
}

func minimalPEFTAdapterArchive(rank int) []byte {
	var buffer bytes.Buffer
	writer := zip.NewWriter(&buffer)
	writeZipFile(writer, "adapter_config.json", []byte(fmt.Sprintf(`{"r":%d,"base_model_name_or_path":"bighill-e2e"}`, rank)))
	writeZipFile(writer, "adapter_model.safetensors", minimalSafetensorsObject())
	Expect(writer.Close()).To(Succeed())
	return buffer.Bytes()
}
