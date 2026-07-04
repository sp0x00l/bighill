package model

import sharedDomain "lib/shared_lib/domain"

type ModelSource = sharedDomain.ModelSource

const (
	ModelSourceTraining    = sharedDomain.ModelSourceTraining
	ModelSourceUpload      = sharedDomain.ModelSourceUpload
	ModelSourceHuggingFace = sharedDomain.ModelSourceHuggingFace
)

var ToModelSource = sharedDomain.ToModelSource
var IsKnownModelSource = sharedDomain.IsKnownModelSource
