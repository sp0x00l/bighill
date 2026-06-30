package model

import "fmt"

type SnapshotStatus int

const (
	SnapshotStatusPending SnapshotStatus = iota
	SnapshotStatusReady
	SnapshotStatusFailed
)

func (s SnapshotStatus) String() string {
	return [...]string{"PENDING", "READY", "FAILED"}[s]
}

func ToSnapshotStatus(s string) (SnapshotStatus, error) {
	switch s {
	case "PENDING":
		return SnapshotStatusPending, nil
	case "READY":
		return SnapshotStatusReady, nil
	case "FAILED":
		return SnapshotStatusFailed, nil
	default:
		return 0, fmt.Errorf("invalid snapshot status %q", s)
	}
}
