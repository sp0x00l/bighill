package executor

import (
	"encoding/json"

	"tool_service/pkg/domain"

	"github.com/go-playground/validator/v10"
	log "github.com/sirupsen/logrus"
)

type httpGetArgumentsDTO struct {
	URL string `json:"url" validate:"required,url"`
}

type HTTPGetArgumentsDTOAdapter struct {
	validator *validator.Validate
}

func NewHTTPGetArgumentsDTOAdapter(v *validator.Validate) *HTTPGetArgumentsDTOAdapter {
	log.Trace("NewHTTPGetArgumentsDTOAdapter")

	if v == nil {
		log.Fatal("http get arguments validator is required")
	}
	return &HTTPGetArgumentsDTOAdapter{validator: v}
}

func (a *HTTPGetArgumentsDTOAdapter) FromDTO(payload []byte) (httpGetArgumentsDTO, error) {
	log.Trace("HTTPGetArgumentsDTOAdapter FromDTO")

	var dto httpGetArgumentsDTO
	if err := json.Unmarshal(payload, &dto); err != nil {
		return dto, domain.ErrValidationFailed.Extend("arguments_json must be a JSON object")
	}
	if err := a.validator.Struct(dto); err != nil {
		return dto, domain.ErrValidationFailed.Extend(err.Error())
	}
	return dto, nil
}
