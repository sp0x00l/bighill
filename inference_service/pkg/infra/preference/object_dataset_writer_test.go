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
	err         error
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
})
