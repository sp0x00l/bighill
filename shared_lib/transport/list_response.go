package transport

import (
	"fmt"
	serializers "lib/shared_lib/serializer"

	log "github.com/sirupsen/logrus"
)

type ListResponse struct {
	Resources []any
}

func (r *ListResponse) ToBytes() ([]byte, error) {
	log.Trace("ListResponse ToBytes")

	data, err := serializers.NewJSONSerializer().Serialize(r.Resources)
	if err != nil {
		return nil, fmt.Errorf("failed to encode list response: %w", err)
	}
	return data, nil
}
