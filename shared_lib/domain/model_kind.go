package domain

import "strings"

type ModelKind string

const (
	ModelKindFineTuned ModelKind = "FINE_TUNED"
	ModelKindBase      ModelKind = "BASE"
)

func (m ModelKind) String() string {
	return string(m)
}

func ToModelKind(value string) ModelKind {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case string(ModelKindBase):
		return ModelKindBase
	case string(ModelKindFineTuned):
		return ModelKindFineTuned
	default:
		return ModelKind(strings.ToUpper(strings.TrimSpace(value)))
	}
}

func IsKnownModelKind(value ModelKind) bool {
	switch value {
	case ModelKindBase, ModelKindFineTuned:
		return true
	default:
		return false
	}
}
