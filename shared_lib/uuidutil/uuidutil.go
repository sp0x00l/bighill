package uuidutil

import (
	"fmt"
	"strings"

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

func ParseOptional(field, raw string) (uuid.UUID, error) {
	if strings.TrimSpace(raw) == "" {
		return uuid.Nil, nil
	}
	return Parse(field, strings.TrimSpace(raw))
}

func StringOrEmpty(value uuid.UUID) string {
	if value == uuid.Nil {
		return ""
	}
	return value.String()
}
