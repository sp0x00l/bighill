package model

import (
	"time"

	"github.com/google/uuid"
)

type CapabilityReport struct {
	CapabilityReportID   uuid.UUID
	EffectiveBaseID      string
	SupportsChat         bool
	SupportsToolCalls    bool
	SupportsSystemPrompt bool
	CreatedAt            time.Time
}
