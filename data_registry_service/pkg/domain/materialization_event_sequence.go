package domain

import "errors"

type MaterializationEventSeqStatus string

const (
	MaterializationEventSeqReady     MaterializationEventSeqStatus = "ready"
	MaterializationEventSeqFuture    MaterializationEventSeqStatus = "future"
	MaterializationEventSeqDuplicate MaterializationEventSeqStatus = "duplicate"
)

var (
	ErrMaterializationEventSequenceInvalid  = errors.New("materialization event sequence invalid")
	ErrMaterializationEventSequenceNotReady = errors.New("materialization event sequence not ready")
	ErrMaterializationEventSequenceMismatch = errors.New("materialization event sequence mismatch")
)
