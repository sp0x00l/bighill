package model

import "github.com/google/uuid"

type InferenceFeedback struct {
	FeedbackID uuid.UUID
	RequestID  uuid.UUID
	UserID     uuid.UUID
	Accepted   bool
	Rating     int
	Comment    string
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
