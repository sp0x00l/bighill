package model

import sharedmodelstatus "lib/shared_lib/modelstatus"

type ModelLoadStatus = sharedmodelstatus.ModelLoadStatus

const (
	ModelLoadStatusNotLoaded = sharedmodelstatus.ModelLoadStatusNotLoaded
	ModelLoadStatusLoaded    = sharedmodelstatus.ModelLoadStatusLoaded
	ModelLoadStatusFailed    = sharedmodelstatus.ModelLoadStatusFailed
)

var (
	ToModelLoadStatus         = sharedmodelstatus.ToModelLoadStatus
	ErrUnknownModelLoadStatus = sharedmodelstatus.ErrUnknownModelLoadStatus
)
