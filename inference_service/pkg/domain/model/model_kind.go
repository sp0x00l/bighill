package model

import sharedDomain "lib/shared_lib/domain"

type ModelKind = sharedDomain.ModelKind

const (
	ModelKindFineTuned = sharedDomain.ModelKindFineTuned
	ModelKindBase      = sharedDomain.ModelKindBase
)

var ToModelKind = sharedDomain.ToModelKind
var IsKnownModelKind = sharedDomain.IsKnownModelKind
