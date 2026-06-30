package transport

import (
	"bytes"
	"encoding/json"
	"fmt"

	log "github.com/sirupsen/logrus"
)

type ListResponse struct {
	Resources []any
}

func (r *ListResponse) ToBytes() ([]byte, error) {
	log.Trace("ListResponse ToBytes")

	buff := new(bytes.Buffer)
	enc := json.NewEncoder(buff)
	enc.SetEscapeHTML(false)

	if err := enc.Encode(r.Resources); err != nil {
		return nil, fmt.Errorf("failed to encode list response: %w", err)
	}

	return bytes.TrimSuffix(buff.Bytes(), []byte("\n")), nil
}
