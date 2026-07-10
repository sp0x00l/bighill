package model

import (
	"fmt"
	"strings"
)

type SnapshotStatus int

const (
	SnapshotStatusPending SnapshotStatus = iota
	SnapshotStatusReady
	SnapshotStatusFailed
)

func (s SnapshotStatus) String() string {
	switch s {
	case SnapshotStatusPending:
		return "PENDING"
	case SnapshotStatusReady:
		return "READY"
	case SnapshotStatusFailed:
		return "FAILED"
	default:
		return "UNKNOWN"
	}
}

func ToSnapshotStatus(s string) (SnapshotStatus, error) {
	switch strings.ToUpper(strings.TrimSpace(s)) {
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
