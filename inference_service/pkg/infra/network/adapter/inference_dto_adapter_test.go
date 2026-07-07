package adapter

import (
	"context"
	"encoding/json"
	"testing"

	"inference_service/pkg/domain/model"

	serializers "lib/shared_lib/serializer"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestAdapter(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Inference service adapter unit test suite")
}

var _ = Describe("InferenceDTOAdapter", func() {
	var adapter *inferenceDTOAdapter

	BeforeEach(func() {
		adapter = NewInferenceDTOAdapter(serializers.NewJSONSerializer())
	})

	It("maps generation DTOs to domain requests", func() {
		request, err := adapter.FromGenerateDTO(context.Background(), []byte(`{
			"query_text":"What did the report say?",
			"top_k":3,
			"metadata_filters":{"section":"summary"}
		}`))

		Expect(err).NotTo(HaveOccurred())
		Expect(request.QueryText).To(Equal("What did the report say?"))
		Expect(request.TopK).To(Equal(3))
		Expect(request.MetadataFilters).To(HaveKeyWithValue("section", "summary"))
	})

	It("defaults top_k and rejects invalid generation DTOs", func() {
		request, err := adapter.FromGenerateDTO(context.Background(), []byte(`{"query_text":"hello"}`))
		Expect(err).NotTo(HaveOccurred())
		Expect(request.TopK).To(Equal(defaultTopK))

		_, err = adapter.FromGenerateDTO(context.Background(), []byte(`{"top_k":1}`))
		Expect(err).To(HaveOccurred())

		_, err = adapter.FromGenerateDTO(context.Background(), []byte(`{"query_text":"hello","top_k":0}`))
		Expect(err).To(HaveOccurred())
	})

	It("serializes generation responses without leaking raw model or dataset IDs", func() {
		payload, err := adapter.ToGenerateDTO(context.Background(), &model.GenerateResponse{
			RequestID: uuid.New(),
			DatasetID: uuid.New(),
			ModelID:   uuid.New(),
			QueryText: "hello",
			Answer:    "world",
			Contexts: []model.RetrievedContext{{
				ChunkIndex: 1,
				SourceText: "source",
				Similarity: 0.9,
			}},
		})

		Expect(err).NotTo(HaveOccurred())
		var dto map[string]any
		Expect(json.Unmarshal(payload, &dto)).To(Succeed())
		Expect(dto).To(HaveKey("request_id"))
		Expect(dto).To(HaveKeyWithValue("answer", "world"))
		Expect(dto).NotTo(HaveKey("model_id"))
		Expect(dto).NotTo(HaveKey("dataset_id"))
	})

	It("maps feedback DTOs to domain feedback", func() {
		requestID := uuid.New()
		feedback, err := adapter.FromFeedbackDTO(context.Background(), []byte(`{
			"request_id":"`+requestID.String()+`",
			"accepted":true,
			"rating":1,
			"comment":"good",
			"preferred_answer":"better"
		}`))

		Expect(err).NotTo(HaveOccurred())
		Expect(feedback.RequestID).To(Equal(requestID))
		Expect(feedback.Accepted).To(BeTrue())
		Expect(feedback.Rating).To(Equal(1))
		Expect(feedback.Comment).To(Equal("good"))
	})

	It("rejects invalid feedback DTOs", func() {
		_, err := adapter.FromFeedbackDTO(context.Background(), []byte(`{"accepted":true}`))
		Expect(err).To(HaveOccurred())

		_, err = adapter.FromFeedbackDTO(context.Background(), []byte(`{"request_id":"not-a-uuid","rating":1}`))
		Expect(err).To(HaveOccurred())

		_, err = adapter.FromFeedbackDTO(context.Background(), []byte(`{"request_id":"`+uuid.NewString()+`","rating":2}`))
		Expect(err).To(HaveOccurred())
	})

	It("serializes safe endpoint projections", func() {
		endpointID := uuid.New()
		payload, err := adapter.ToEndpointDTOs(context.Background(), []*model.PublishedEndpoint{{
			EndpointID:  endpointID,
			OrgID:       uuid.New(),
			ModelID:     uuid.New(),
			DatasetID:   uuid.New(),
			Status:      model.PublishedEndpointStatusReady,
			DisplayName: "Support bot",
		}})

		Expect(err).NotTo(HaveOccurred())
		var dtos []map[string]any
		Expect(json.Unmarshal(payload, &dtos)).To(Succeed())
		Expect(dtos).To(HaveLen(1))
		Expect(dtos[0]).To(HaveKeyWithValue("endpoint_id", endpointID.String()))
		Expect(dtos[0]).To(HaveKeyWithValue("display_name", "Support bot"))
		Expect(dtos[0]).NotTo(HaveKey("model_id"))
		Expect(dtos[0]).NotTo(HaveKey("dataset_id"))
	})
})
