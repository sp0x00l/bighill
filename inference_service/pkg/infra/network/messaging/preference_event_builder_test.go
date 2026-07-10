package messaging_test

import (
	"inference_service/pkg/domain/model"
	inferencemessaging "inference_service/pkg/infra/network/messaging"
	inferencepb "lib/data_contracts_lib/inference"
	shared "lib/shared_lib/messaging"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
)

var _ = Describe("PreferenceDatasetEventBuilder", func() {
	It("builds preference dataset ready events", func() {
		preferenceDatasetID := uuid.New()
		requestID := uuid.New()
		userID := uuid.New()
		orgID := uuid.New()
		datasetID := uuid.New()
		modelID := uuid.New()
		builder := inferencemessaging.NewPreferenceDatasetEventBuilder("inference")

		dataset := &model.PreferenceDataset{
			PreferenceDatasetID:    preferenceDatasetID,
			RequestID:              requestID,
			UserID:                 userID,
			OrgID:                  orgID,
			DatasetID:              datasetID,
			ModelID:                modelID,
			ParentModelKind:        model.ModelKindFineTuned,
			ParentArtifactURI:      " s3://local-dev-bucket/models/model ",
			ParentArtifactChecksum: " sha256:abc ",
			ParentAdapterURI:       " s3://local-dev-bucket/models/adapter ",
			ParentBaseModel:        " llama3 ",
			ParentModelVersion:     7,
			OutputURI:              " s3://local-dev-bucket/preferences/train.jsonl ",
			EvaluationOutputURI:    " s3://local-dev-bucket/preferences/eval.jsonl ",
			Format:                 " jsonl ",
			Examples: []model.PreferenceExample{
				{PreferenceExampleID: uuid.New()},
				{PreferenceExampleID: uuid.New()},
			},
		}
		request := model.PreferenceDatasetExportRequest{
			MinExamples: 2,
			Limit:       50,
		}

		outbound := builder.PreferenceDatasetReadyMessage(dataset, request)

		Expect(outbound.Topic).To(Equal("inference"))
		Expect(outbound.DispatchKey).To(Equal("preference_dataset_ready:" + preferenceDatasetID.String()))
		Expect(outbound.Message.ResourceKey).To(Equal(datasetID))
		Expect(outbound.Message.MsgType).To(Equal(shared.MsgTypePreferenceDatasetReady))

		var event inferencepb.PreferenceDatasetReadyEvent
		Expect(proto.Unmarshal(outbound.Message.Payload, &event)).To(Succeed())
		Expect(event.PreferenceDatasetId).To(Equal(preferenceDatasetID.String()))
		Expect(event.SourceRequestId).To(Equal(requestID.String()))
		Expect(event.UserId).To(Equal(userID.String()))
		Expect(event.OrgId).To(Equal(orgID.String()))
		Expect(event.DatasetId).To(Equal(datasetID.String()))
		Expect(event.ModelId).To(Equal(modelID.String()))
		Expect(event.OutputUri).To(Equal("s3://local-dev-bucket/preferences/train.jsonl"))
		Expect(event.EvaluationOutputUri).To(Equal("s3://local-dev-bucket/preferences/eval.jsonl"))
		Expect(event.ExampleCount).To(Equal(int32(2)))
		Expect(event.Format).To(Equal("jsonl"))
		Expect(event.MinExamples).To(Equal(int32(2)))
		Expect(event.Limit).To(Equal(int32(50)))
		Expect(event.ParentModelKind).To(Equal(model.ModelKindFineTuned.String()))
		Expect(event.ParentArtifactUri).To(Equal("s3://local-dev-bucket/models/model"))
		Expect(event.ParentArtifactChecksum).To(Equal("sha256:abc"))
		Expect(event.ParentAdapterUri).To(Equal("s3://local-dev-bucket/models/adapter"))
		Expect(event.ParentBaseModel).To(Equal("llama3"))
		Expect(event.ParentModelVersion).To(Equal(int32(7)))
	})
})
