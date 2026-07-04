package domain

import (
	"time"

	"github.com/google/uuid"
)

type Tenant struct {
	TenantID                   uuid.UUID
	Email                      string
	HuggingFaceTokenCiphertext string
	Deleted                    bool
	UpdatedAt                  time.Time
}
