package executor

import (
	"encoding/json"

	"tool_execution_service/pkg/domain"

	"github.com/go-playground/validator/v10"
	log "github.com/sirupsen/logrus"
)

type httpGetRequestPayload struct {
	URL string `json:"url" validate:"required,url"`
}

func parseHTTPGetRequestPayload(payload []byte, validate *validator.Validate) (httpGetRequestPayload, error) {
	log.Trace("parseHTTPGetRequestPayload")

	var out httpGetRequestPayload
	if err := json.Unmarshal(payload, &out); err != nil {
		return out, domain.ErrValidationFailed.Extend("arguments_json must be a JSON object")
	}
	if err := validate.Struct(out); err != nil {
		return out, domain.ErrValidationFailed.Extend(err.Error())
	}
	return out, nil
}
