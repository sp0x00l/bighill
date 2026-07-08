package model

import "github.com/google/uuid"

type InferenceFeedback struct {
	FeedbackID      uuid.UUID
	RequestID       uuid.UUID
	UserID          uuid.UUID
	OrgID           uuid.UUID
	Accepted        bool
	Rating          int
	Comment         string
	PreferredAnswer string
}

type PreferenceExample struct {
	PreferenceExampleID uuid.UUID
	FeedbackID          uuid.UUID
	RequestID           uuid.UUID
	UserID              uuid.UUID
	OrgID               uuid.UUID
	DatasetID           uuid.UUID
	ModelID             uuid.UUID
	Split               string
	PromptText          string
	AcceptedAnswer      string
	RejectedAnswer      string
	Rating              int
	FeedbackLabel       string
}

type PreferenceDatasetExportRequest struct {
	RequestID   uuid.UUID
	UserID      uuid.UUID
	OrgID       uuid.UUID
	DatasetID   uuid.UUID
	ModelID     uuid.UUID
	OutputURI   string
	MinExamples int
	Limit       int
}

type PreferenceDataset struct {
	PreferenceDatasetID    uuid.UUID
	RequestID              uuid.UUID
	UserID                 uuid.UUID
	OrgID                  uuid.UUID
	DatasetID              uuid.UUID
	ModelID                uuid.UUID
	ParentModelKind        ModelKind
	ParentArtifactURI      string
	ParentArtifactChecksum string
	ParentAdapterURI       string
	ParentBaseModel        string
	ParentModelVersion     int
	OutputURI              string
	EvaluationOutputURI    string
	Format                 string
	EligibilityPolicy      string
	MinExamples            int
	Limit                  int
	Examples               []PreferenceExample
	Exported               bool
}

func (d PreferenceDataset) ExampleCount() int {
	return len(d.Examples)
}

func (d PreferenceDataset) TrainingExamples() []PreferenceExample {
	out := make([]PreferenceExample, 0, len(d.Examples))
	for _, example := range d.Examples {
		if example.Split == "" || example.Split == "TRAIN" {
			out = append(out, example)
		}
	}
	return out
}

func (d PreferenceDataset) EvaluationExamples() []PreferenceExample {
	out := make([]PreferenceExample, 0, len(d.Examples))
	for _, example := range d.Examples {
		if example.Split == "EVAL" {
			out = append(out, example)
		}
	}
	return out
}

func (d PreferenceDataset) TrainingExampleCount() int {
	return len(d.TrainingExamples())
}

func (d PreferenceDataset) EvaluationExampleCount() int {
	return len(d.EvaluationExamples())
}
