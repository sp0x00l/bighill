package transport

import (
	"bytes"
	"encoding/json"
	"fmt"

	log "github.com/sirupsen/logrus"
)

type PaginatedResponse struct {
	Resources []any    `json:"resources,omitempty"`
	Metadata  Metadata `json:"metadata"`
}

func (r *PaginatedResponse) ToBytes() ([]byte, error) {
	log.Trace("PaginatedResponse ToBytes")

	// ensure the special query characters (& and :) are not escaped
	buff := new(bytes.Buffer)
	enc := json.NewEncoder(buff)
	enc.SetEscapeHTML(false)

	if err := enc.Encode(r); err != nil {
		return nil, fmt.Errorf("failed to encode paginated response: %w", err)
	}

	return buff.Bytes(), nil
}
