package model

import "github.com/google/uuid"

type ServedModelStatus struct {
	ModelID           uuid.UUID
	ServingTarget     string
	ServingModel      string
	ServingLoadStatus ModelLoadStatus
	FailureReason     string
}
