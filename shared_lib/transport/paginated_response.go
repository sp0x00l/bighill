package transport

import (
	"fmt"
	serializers "lib/shared_lib/serializer"

	log "github.com/sirupsen/logrus"
)

type PaginatedResponse struct {
	Resources []any    `json:"resources,omitempty"`
	Metadata  Metadata `json:"metadata"`
}

func (r *PaginatedResponse) ToBytes() ([]byte, error) {
	log.Trace("PaginatedResponse ToBytes")

	data, err := serializers.NewJSONSerializer().Serialize(r)
	if err != nil {
		return nil, fmt.Errorf("failed to encode paginated response: %w", err)
	}
	return append(data, '\n'), nil
}
