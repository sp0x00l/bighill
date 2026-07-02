package model

import "fmt"

type ModelLoadStatus int

const (
	ModelLoadStatusNotLoaded ModelLoadStatus = iota
	ModelLoadStatusLoaded
	ModelLoadStatusFailed
)

func (s ModelLoadStatus) String() string {
	if s < ModelLoadStatusNotLoaded || s > ModelLoadStatusFailed {
		return "UNKNOWN"
	}
	return [...]string{"NOT_LOADED", "LOADED", "FAILED"}[s]
}

func ToModelLoadStatus(value string) (ModelLoadStatus, error) {
	switch value {
	case "", "NOT_LOADED":
		return ModelLoadStatusNotLoaded, nil
	case "LOADED":
		return ModelLoadStatusLoaded, nil
	case "FAILED":
		return ModelLoadStatusFailed, nil
	default:
		return 0, fmt.Errorf("invalid model load status %q", value)
	}
}
