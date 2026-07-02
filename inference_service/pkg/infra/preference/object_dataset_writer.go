package preference

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"strings"

	"inference_service/pkg/domain/model"
	corebucket "lib/shared_lib/bucket"

	log "github.com/sirupsen/logrus"
)

type Bucket interface {
	Upload(ctx context.Context, bucket, key, contentType string, body io.Reader) error
}

type ObjectDatasetWriter struct {
	bucket Bucket
}

func NewObjectDatasetWriter(bucket Bucket) *ObjectDatasetWriter {
	log.Trace("NewObjectDatasetWriter")

	return &ObjectDatasetWriter{
		bucket: bucket,
	}
}

func NewS3ObjectDatasetWriter(ctx context.Context, region string, uploadPartSize int64) *ObjectDatasetWriter {
	log.Trace("NewS3ObjectDatasetWriter")

	bucket := corebucket.NewBucket(ctx, region, uploadPartSize)
	if bucket == nil {
		return nil
	}
	return NewObjectDatasetWriter(bucket)
}

func (w *ObjectDatasetWriter) WritePreferenceDataset(ctx context.Context, dataset *model.PreferenceDataset) (*model.PreferenceDataset, error) {
	log.Trace("ObjectDatasetWriter WritePreferenceDataset")

	payload, err := preferenceDatasetJSONL(dataset)
	if err != nil {
		return nil, err
	}
	bucketName, key, err := parseS3URI(dataset.OutputURI)
	if err != nil {
		return nil, err
	}
	if err := w.bucket.Upload(ctx, bucketName, key, "application/x-ndjson", bytes.NewReader(payload)); err != nil {
		return nil, fmt.Errorf("write preference dataset: %w", err)
	}
	dataset.Exported = true
	return dataset, nil
}

func preferenceDatasetJSONL(dataset *model.PreferenceDataset) ([]byte, error) {
	log.Trace("preferenceDatasetJSONL")

	var out bytes.Buffer
	for _, example := range dataset.Examples {
		row := map[string]any{
			"dataset_id":            example.DatasetID.String(),
			"model_id":              example.ModelID.String(),
			"request_id":            example.RequestID.String(),
			"feedback_id":           example.FeedbackID.String(),
			"preference_example_id": example.PreferenceExampleID.String(),
			"prompt":                example.PromptText,
			"chosen":                example.AcceptedAnswer,
			"rejected":              example.RejectedAnswer,
			"rating":                example.Rating,
			"feedback_label":        example.FeedbackLabel,
		}
		raw, err := json.Marshal(row)
		if err != nil {
			return nil, fmt.Errorf("marshal preference dataset row: %w", err)
		}
		out.Write(raw)
		out.WriteByte('\n')
	}
	return out.Bytes(), nil
}

func parseS3URI(raw string) (string, string, error) {
	log.Trace("parseS3URI")

	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", "", err
	}
	if parsed.Scheme != "s3" || parsed.Host == "" || strings.Trim(parsed.Path, "/") == "" {
		return "", "", fmt.Errorf("preference dataset output uri must be an s3 uri")
	}
	return parsed.Host, strings.TrimPrefix(parsed.Path, "/"), nil
}
