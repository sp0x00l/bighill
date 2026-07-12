package model

import (
	"time"

	"github.com/google/uuid"
)

type LineageEvalSetSource string

const (
	LineageEvalSetSourceCurated    LineageEvalSetSource = "CURATED"
	LineageEvalSetSourceFrozenGen0 LineageEvalSetSource = "FROZEN_GEN0"
)

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

type PreferenceDatasetBuildRequest struct {
	UserID      uuid.UUID
	OrgID       uuid.UUID
	EndpointID  uuid.UUID
	DatasetID   uuid.UUID
	DatasetIDs  []uuid.UUID
	ModelID     uuid.UUID
	OutputURI   string
	MinExamples int
	Limit       int
	MaxPerUser  int
}

type PreferenceDatasetFilter struct {
	ModelID    uuid.UUID
	EndpointID uuid.UUID
}

type PreferenceDataset struct {
	PreferenceDatasetID    uuid.UUID
	RequestID              uuid.UUID
	EndpointID             uuid.UUID
	UserID                 uuid.UUID
	OrgID                  uuid.UUID
	DatasetID              uuid.UUID
	DatasetIDs             []uuid.UUID
	ModelID                uuid.UUID
	ParentModelKind        ModelKind
	ParentArtifactURI      string
	ParentArtifactChecksum string
	ParentAdapterURI       string
	ParentBaseModel        string
	ParentModelName        string
	ParentLineageName      string
	ParentModelVersion     int
	OutputURI              string
	EvaluationOutputURI    string
	Format                 string
	EligibilityPolicy      string
	ExampleTotal           int
	MinExamples            int
	Limit                  int
	TrainingCount          int
	EvaluationCount        int
	IntegrityKey           string
	CreatedAt              time.Time
	Examples               []PreferenceExample
	Exported               bool
}

func (d PreferenceDataset) ExampleCount() int {
	if d.ExampleTotal > 0 {
		return d.ExampleTotal
	}
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
	if d.TrainingCount > 0 {
		return d.TrainingCount
	}
	return len(d.TrainingExamples())
}

func (d PreferenceDataset) EvaluationExampleCount() int {
	if d.EvaluationCount > 0 {
		return d.EvaluationCount
	}
	return len(d.EvaluationExamples())
}

type LineageEvalSet struct {
	OrgID          uuid.UUID
	LineageName    string
	Version        int
	EvalDatasetURI string
	Checksum       string
	ExampleCount   int
	Source         LineageEvalSetSource
	Active         bool
	FrozenAt       time.Time
}
