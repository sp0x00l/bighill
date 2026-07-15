package adapter

import (
	"encoding/json"
	"strings"

	"inference_service/pkg/domain"
	"inference_service/pkg/domain/model"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

func uuidStrings(values []uuid.UUID) []string {
	log.Trace("uuidStrings")

	out := make([]string, 0, len(values))
	for _, value := range values {
		if value == uuid.Nil {
			continue
		}
		out = append(out, value.String())
	}
	return out
}

func optionalUUIDString(value uuid.UUID) string {
	log.Trace("optionalUUIDString")

	if value == uuid.Nil {
		return ""
	}
	return value.String()
}

func parseRequiredUUID(value string, message string) (uuid.UUID, error) {
	log.Trace("parseRequiredUUID")

	id, err := uuid.Parse(strings.TrimSpace(value))
	if err != nil || id == uuid.Nil {
		return uuid.Nil, domain.ErrValidationFailed.Extend(message)
	}
	return id, nil
}

func parseRequiredUUIDs(values []string, message string) ([]uuid.UUID, error) {
	log.Trace("parseRequiredUUIDs")

	ids := make([]uuid.UUID, 0, len(values))
	for _, value := range values {
		id, err := parseRequiredUUID(value, message)
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func marshalMap(value map[string]any) ([]byte, error) {
	log.Trace("marshalMap")

	if value == nil {
		value = map[string]any{}
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, domain.ErrValidationFailed.Extend(err.Error())
	}
	return encoded, nil
}

func optionalRAGMergeStrategy(value string) (model.RAGMergeStrategy, error) {
	log.Trace("optionalRAGMergeStrategy")

	if value == "" {
		return "", nil
	}
	return model.ToRAGMergeStrategy(value)
}

func ptr(value int) *int {
	log.Trace("ptr")

	return &value
}
