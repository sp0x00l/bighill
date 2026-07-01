package model

import "fmt"

type TrainingRunStatus int

const (
	TrainingRunStatusRequested TrainingRunStatus = iota
	TrainingRunStatusPreparingData
	TrainingRunStatusTraining
	TrainingRunStatusEvaluating
	TrainingRunStatusCompleted
	TrainingRunStatusFailed
)

func (s TrainingRunStatus) String() string {
	if s < TrainingRunStatusRequested || s > TrainingRunStatusFailed {
		return "UNKNOWN"
	}
	return [...]string{"REQUESTED", "PREPARING_DATA", "TRAINING", "EVALUATING", "COMPLETED", "FAILED"}[s]
}

func ToTrainingRunStatus(value string) (TrainingRunStatus, error) {
	switch value {
	case "REQUESTED":
		return TrainingRunStatusRequested, nil
	case "PREPARING_DATA":
		return TrainingRunStatusPreparingData, nil
	case "TRAINING":
		return TrainingRunStatusTraining, nil
	case "EVALUATING":
		return TrainingRunStatusEvaluating, nil
	case "COMPLETED":
		return TrainingRunStatusCompleted, nil
	case "FAILED":
		return TrainingRunStatusFailed, nil
	default:
		return 0, fmt.Errorf("invalid training run status %q", value)
	}
}

type TrainingRunRequest struct {
	TrainingRunID     string
	DatasetID         string
	FeatureSnapshotID string
	ModelName         string
	ModelVersion      string
	EvaluationProfile string
}

type PreparedTrainingDataset struct {
	TrainingRunID     string
	FeatureSnapshotID string
	DatasetURI        string
}

type TrainedModelArtifact struct {
	TrainingRunID string
	ModelURI      string
	ModelName     string
	ModelVersion  string
}

type EvaluationReport struct {
	TrainingRunID string
	ReportURI     string
	Passed        bool
}

type TrainingRunResult struct {
	TrainingRunID string
	ModelURI      string
	ReportURI     string
	Status        TrainingRunStatus
}
