package domain

import "strings"

type ModelSource string

const (
	ModelSourceTraining    ModelSource = "TRAINING"
	ModelSourceUpload      ModelSource = "UPLOAD"
	ModelSourceHuggingFace ModelSource = "HUGGING_FACE"
)

func (m ModelSource) String() string {
	return string(m)
}

func ToModelSource(value string) ModelSource {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case string(ModelSourceTraining):
		return ModelSourceTraining
	case string(ModelSourceUpload):
		return ModelSourceUpload
	case string(ModelSourceHuggingFace):
		return ModelSourceHuggingFace
	default:
		return ModelSource(strings.ToUpper(strings.TrimSpace(value)))
	}
}

func IsKnownModelSource(value ModelSource) bool {
	switch value {
	case ModelSourceTraining, ModelSourceUpload, ModelSourceHuggingFace:
		return true
	default:
		return false
	}
}
