package modelstatus

import (
	"fmt"
	"strings"
)

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
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "", "NOT_LOADED":
		return ModelLoadStatusNotLoaded, nil
	case "LOADED":
		return ModelLoadStatusLoaded, nil
	case "FAILED":
		return ModelLoadStatusFailed, nil
	default:
		return 0, fmt.Errorf("invalid model load status %q: %w", value, ErrUnknownModelLoadStatus)
	}
}

var ErrUnknownModelLoadStatus = errUnknownModelLoadStatus{}

type errUnknownModelLoadStatus struct{}

func (errUnknownModelLoadStatus) Error() string {
	return "unknown model load status"
}
