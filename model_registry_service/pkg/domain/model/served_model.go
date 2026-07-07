package model

import "github.com/google/uuid"

type ServedModelStatus struct {
	ModelID           uuid.UUID
	ServingTarget     string
	ServingModel      string
	ServingProtocol   ServingProtocol
	ServingLoadStatus ModelLoadStatus
	FailureReason     string
}
