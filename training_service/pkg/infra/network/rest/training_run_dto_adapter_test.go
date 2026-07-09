package rest

import (
	"context"
	"encoding/json"

	"training_service/pkg/domain/model"

	serializers "lib/shared_lib/serializer"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("TrainingRunDTOAdapter", func() {
	var adapter *trainingRunDTOAdapter

	BeforeEach(func() {
		adapter = NewTrainingRunDTOAdapter(serializers.NewJSONSerializer())
	})

	It("maps start training run DTOs to commands", func() {
		datasetID := uuid.New()
		modelID := uuid.NewSHA1(uuid.NameSpaceURL, []byte("model-registry/base-model"))

		command, err := adapter.FromDTO(context.Background(), []byte(`{
			"dataset_id":"`+datasetID.String()+`",
			"source_model_id":"`+modelID.String()+`",
			"training_profile":"sft-default@v1",
			"evaluation_profile":"ragas-default@v1"
		}`))

		Expect(err).NotTo(HaveOccurred())
		Expect(command.DatasetID).To(Equal(datasetID))
		Expect(command.SourceModelID).To(Equal(modelID))
		Expect(command.TrainingProfile).To(Equal("sft-default@v1"))
		Expect(command.EvaluationProfile).To(Equal("ragas-default@v1"))
	})

	It("rejects missing and malformed identifiers", func() {
		_, err := adapter.FromDTO(context.Background(), []byte(`{"source_model_id":"`+uuid.NewString()+`"}`))
		Expect(err).To(HaveOccurred())

		_, err = adapter.FromDTO(context.Background(), []byte(`{"dataset_id":"not-a-uuid","source_model_id":"`+uuid.NewString()+`"}`))
		Expect(err).To(HaveOccurred())

		_, err = adapter.FromDTO(context.Background(), []byte(`{"dataset_id":"`+uuid.NewString()+`","source_model_id":"not-a-uuid"}`))
		Expect(err).To(HaveOccurred())

		_, err = adapter.FromDTO(context.Background(), []byte(`{"dataset_id":"`+uuid.Nil.String()+`","source_model_id":"`+uuid.NewString()+`"}`))
		Expect(err).To(HaveOccurred())
	})

	It("rejects unpinned explicit profile names", func() {
		_, err := adapter.FromDTO(context.Background(), []byte(`{
			"dataset_id":"`+uuid.NewString()+`",
			"source_model_id":"`+uuid.NewString()+`",
			"training_profile":"sft-default"
		}`))

		Expect(err).To(HaveOccurred())
	})

	It("serializes start training run responses", func() {
		payload, err := adapter.ToStartTrainingRunDTO(context.Background(), &model.TrainingRunStartResult{
			TrainingRunID: uuid.NewString(),
			StatusURL:     "/v1/private/training-runs/run",
		})

		Expect(err).NotTo(HaveOccurred())
		var dto StartTrainingRunResponseDTO
		Expect(json.Unmarshal(payload, &dto)).To(Succeed())
		Expect(dto.TrainingRunID).NotTo(BeEmpty())
		Expect(dto.StatusURL).To(Equal("/v1/private/training-runs/run"))
	})

	It("serializes training run status responses", func() {
		trainingRunID := uuid.NewString()
		payload, err := adapter.ToTrainingRunStatusDTO(context.Background(), &model.TrainingRunStatusResult{
			TrainingRunID: trainingRunID,
			Status:        "RUNNING",
		})

		Expect(err).NotTo(HaveOccurred())
		var dto TrainingRunStatusDTO
		Expect(json.Unmarshal(payload, &dto)).To(Succeed())
		Expect(dto.TrainingRunID).To(Equal(trainingRunID))
		Expect(dto.Status).To(Equal("RUNNING"))
	})
})
