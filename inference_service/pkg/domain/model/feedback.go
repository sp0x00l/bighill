package model

import "github.com/google/uuid"

type InferenceFeedback struct {
	FeedbackID      uuid.UUID
	RequestID       uuid.UUID
	UserID          uuid.UUID
	Accepted        bool
	Rating          int
	Comment         string
	PreferredAnswer string
}

type PreferenceExample struct {
	PreferenceExampleID uuid.UUID
	FeedbackID          uuid.UUID
	RequestID           uuid.UUID
	DatasetID           uuid.UUID
	ModelID             uuid.UUID
	PromptText          string
	AcceptedAnswer      string
	RejectedAnswer      string
	Rating              int
	FeedbackLabel       string
}

type PreferenceDatasetExportRequest struct {
	RequestID   uuid.UUID
	DatasetID   uuid.UUID
	ModelID     uuid.UUID
	OutputURI   string
	MinExamples int
	Limit       int
}

type PreferenceDataset struct {
	RequestID uuid.UUID
	DatasetID uuid.UUID
	ModelID   uuid.UUID
	OutputURI string
	Examples  []PreferenceExample
	Exported  bool
}

func (d PreferenceDataset) ExampleCount() int {
	return len(d.Examples)
}
