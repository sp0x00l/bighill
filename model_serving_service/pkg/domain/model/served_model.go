package model

import (
	"strings"

	"github.com/google/uuid"
)

type ServedModel struct {
	ResourceName     string
	Namespace        string
	Generation       int64
	ModelID          uuid.UUID
	OrgID            uuid.UUID
	TrainingRunID    uuid.UUID
	DatasetID        uuid.UUID
	ModelKind        string
	Name             string
	ModelVersion     int
	BaseModel        string
	ArtifactLocation string
	ArtifactFormat   string
	ArtifactChecksum string
	AdapterURI       string
	AdapterRank      int
	RuntimeIsolation string
	Pinned           bool
	ServingTarget    string
	ServingModel     string
	ServingProtocol  ServingProtocol
	Status           *ServedModelStatus
}

type ServingRuntimeState struct {
	Ready           bool
	Failed          bool
	ServingTarget   string
	ServingModel    string
	ServingProtocol ServingProtocol
	FailureReason   string
	ReadyReplicas   int32
}

type ServedModelStatus struct {
	ServingLoadStatus  ModelLoadStatus
	ServingTarget      string
	ServingModel       string
	ServingProtocol    ServingProtocol
	FailureReason      string
	ObservedGeneration int64
	ReadyReplicas      int32
}

func (m *ServedModel) IsAdapter() bool {
	if m == nil {
		return false
	}
	return strings.EqualFold(m.ModelKind, "FINE_TUNED") && strings.TrimSpace(m.AdapterURI) != ""
}

func (m *ServedModel) UsesDedicatedRuntimePool() bool {
	if m == nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(m.RuntimeIsolation), RuntimeIsolationDedicated)
}

const (
	RuntimeIsolationShared         = "SHARED"
	RuntimeIsolationDedicated      = "DEDICATED"
	NotLoadedReasonCapacityEvicted = "capacity_evicted"
)
