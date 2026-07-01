package model

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
)

type Model struct {
	ModelID           uuid.UUID
	TrainingRunID     uuid.UUID
	DatasetID         uuid.UUID
	Name              string
	ModelVersion      int
	BaseModel         string
	ArtifactLocation  string
	ArtifactFormat    string
	ArtifactChecksum  string
	ArtifactSizeBytes int64
	MetricsMetadata   string
	Status            ModelStatus
	FailureReason     string
}

func NormalizeModel(model *Model) {
	if model == nil {
		return
	}
	if model.ModelID == uuid.Nil {
		model.ModelID = uuid.New()
	}
	if model.Name == "" {
		model.Name = defaultModelName(model.ModelID)
	}
	if model.ModelVersion <= 0 {
		model.ModelVersion = 1
	}
	if model.MetricsMetadata == "" {
		model.MetricsMetadata = "{}"
	}
	if model.ArtifactFormat == "" {
		model.ArtifactFormat = "UNKNOWN"
	}
}

func defaultModelName(modelID uuid.UUID) string {
	return fmt.Sprintf("model_%s", strings.ReplaceAll(modelID.String(), "-", "_"))
}
