package model

import "fmt"

type ModelStatus int

const (
	ModelStatusPending ModelStatus = iota
	ModelStatusCandidate
	ModelStatusEvaluated
	ModelStatusReady
	ModelStatusFailed
)

func (s ModelStatus) String() string {
	if s < ModelStatusPending || s > ModelStatusFailed {
		return "UNKNOWN"
	}
	return [...]string{"PENDING", "CANDIDATE", "EVALUATED", "READY", "FAILED"}[s]
}

func ToModelStatus(value string) (ModelStatus, error) {
	switch value {
	case "PENDING":
		return ModelStatusPending, nil
	case "CANDIDATE":
		return ModelStatusCandidate, nil
	case "EVALUATED":
		return ModelStatusEvaluated, nil
	case "READY":
		return ModelStatusReady, nil
	case "FAILED":
		return ModelStatusFailed, nil
	default:
		return 0, fmt.Errorf("invalid model status %q", value)
	}
}
