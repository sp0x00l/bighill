package uuidutil

import (
	"fmt"

	"github.com/google/uuid"
)

func Parse(field, raw string) (uuid.UUID, error) {
	id, err := uuid.Parse(raw)
	if err != nil {
		return uuid.Nil, fmt.Errorf("invalid %s UUID %q: %w", field, raw, err)
	}
	if id == uuid.Nil {
		return uuid.Nil, fmt.Errorf("invalid %s UUID %q: nil UUID", field, raw)
	}
	return id, nil
}
