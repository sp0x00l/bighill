package model

import (
	"time"

	"github.com/google/uuid"
)

type EffectiveBaseVersion struct {
	EffectiveBaseID        uuid.UUID
	ModelID                uuid.UUID
	OrgID                  uuid.UUID
	BaseModel              string
	SourceArtifactLocation string
	SourceArtifactFormat   string
	SourceArtifactChecksum string
	ServingTarget          string
	ServingModel           string
	ServingProtocol        ServingProtocol
	CreatedAt              time.Time
	UpdatedAt              time.Time
}
