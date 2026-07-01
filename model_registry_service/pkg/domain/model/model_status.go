package model

import "fmt"

type ModelStatus int

const (
	ModelStatusPending ModelStatus = iota
	ModelStatusReady
	ModelStatusFailed
)

func (s ModelStatus) String() string {
	return [...]string{"PENDING", "READY", "FAILED"}[s]
}

func ToModelStatus(value string) (ModelStatus, error) {
	switch value {
	case "PENDING":
		return ModelStatusPending, nil
	case "READY":
		return ModelStatusReady, nil
	case "FAILED":
		return ModelStatusFailed, nil
	default:
		return 0, fmt.Errorf("invalid model status %q", value)
	}
}
