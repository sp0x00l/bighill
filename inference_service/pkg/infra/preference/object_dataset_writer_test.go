package preference_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"inference_service/pkg/domain/model"
	"inference_service/pkg/infra/preference"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type bucketStub struct {
	bucket      string
	key         string
	contentType string
	body        string
	uploads     []uploadRecord
	err         error
}

type uploadRecord struct {
	bucket      string
	key         string
	contentType string
	body        string
}

func (b *bucketStub) Upload(_ context.Context, bucket string, key string, contentType string, body io.Reader) error {
	b.bucket = bucket
	b.key = key
	b.contentType = contentType
	raw, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	b.body = string(raw)
	b.uploads = append(b.uploads, uploadRecord{bucket: bucket, key: key, contentType: contentType, body: b.body})
	return b.err
}

func TestPreference(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Inference preference unit test suite")
}

var _ = Describe("ObjectDatasetWriter", func() {
	It("writes DPO JSONL to object storage", func() {
		requestID := uuid.New()
		datasetID := uuid.New()
		modelID := uuid.New()
		feedbackID := uuid.New()
		exampleID := uuid.New()
		bucket := &bucketStub{}
		writer := preference.NewObjectDatasetWriter(bucket)

		dataset, err := writer.WritePreferenceDataset(context.Background(), &model.PreferenceDataset{
			RequestID: requestID,
			DatasetID: datasetID,
			ModelID:   modelID,
			OutputURI: "s3://local-dev-bucket/preferences/dataset.jsonl",
			Examples: []model.PreferenceExample{{
				PreferenceExampleID: exampleID,
				FeedbackID:          feedbackID,
				RequestID:           requestID,
				DatasetID:           datasetID,
				ModelID:             modelID,
				Split:               "TRAIN",
				PromptText:          "Prompt text",
				AcceptedAnswer:      "Chosen answer",
				RejectedAnswer:      "Rejected answer",
				Rating:              -1,
				FeedbackLabel:       "REJECTED",
			}},
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(dataset.Exported).To(BeTrue())
		Expect(bucket.bucket).To(Equal("local-dev-bucket"))
		Expect(bucket.key).To(Equal("preferences/dataset.jsonl"))
		Expect(bucket.contentType).To(Equal("application/x-ndjson"))
		Expect(bucket.body).To(ContainSubstring(`"prompt":"Prompt text"`))
		Expect(bucket.body).To(ContainSubstring(`"chosen":"Chosen answer"`))
		Expect(bucket.body).To(ContainSubstring(`"rejected":"Rejected answer"`))
		Expect(bytes.Count([]byte(bucket.body), []byte("\n"))).To(Equal(1))
	})

	It("writes held-out eval examples to the eval object", func() {
		requestID := uuid.New()
		datasetID := uuid.New()
		modelID := uuid.New()
		bucket := &bucketStub{}
		writer := preference.NewObjectDatasetWriter(bucket)

		dataset, err := writer.WritePreferenceDataset(context.Background(), &model.PreferenceDataset{
			RequestID:           requestID,
			DatasetID:           datasetID,
			ModelID:             modelID,
			OutputURI:           "s3://local-dev-bucket/preferences/train.jsonl",
			EvaluationOutputURI: "s3://local-dev-bucket/preferences/eval.jsonl",
			Examples: []model.PreferenceExample{
				{PreferenceExampleID: uuid.New(), FeedbackID: uuid.New(), RequestID: requestID, DatasetID: datasetID, ModelID: modelID, Split: "TRAIN", PromptText: "Train prompt", AcceptedAnswer: "Chosen", RejectedAnswer: "Rejected"},
				{PreferenceExampleID: uuid.New(), FeedbackID: uuid.New(), RequestID: requestID, DatasetID: datasetID, ModelID: modelID, Split: "EVAL", PromptText: "Eval prompt", AcceptedAnswer: "Chosen eval", RejectedAnswer: "Rejected eval"},
			},
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(dataset.Exported).To(BeTrue())
		Expect(bucket.uploads).To(HaveLen(2))
		Expect(bucket.uploads[0].key).To(Equal("preferences/train.jsonl"))
		Expect(bucket.uploads[0].body).To(ContainSubstring(`"prompt":"Train prompt"`))
		Expect(bucket.uploads[0].body).NotTo(ContainSubstring("Eval prompt"))
		Expect(bucket.uploads[1].key).To(Equal("preferences/eval.jsonl"))
		Expect(bucket.uploads[1].body).To(ContainSubstring(`"prompt":"Eval prompt"`))
		Expect(bucket.uploads[1].body).NotTo(ContainSubstring("Train prompt"))
	})
})
