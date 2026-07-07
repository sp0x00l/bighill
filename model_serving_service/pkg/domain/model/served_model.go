package model

import "github.com/google/uuid"

type ServedModel struct {
	ResourceName     string
	Namespace        string
	Generation       int64
	ModelID          uuid.UUID
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
