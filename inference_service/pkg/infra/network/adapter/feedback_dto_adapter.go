package adapter

import (
	"context"

	"inference_service/pkg/domain"
	"inference_service/pkg/domain/model"
	serializers "lib/shared_lib/serializer"

	"github.com/go-playground/validator/v10"
	log "github.com/sirupsen/logrus"
)

type FeedbackDTOAdapter interface {
	FromDTO(ctx context.Context, body []byte) (*model.InferenceFeedback, error)
	ToDTO(ctx context.Context, feedback *model.InferenceFeedback) ([]byte, error)
}

type feedbackDTOAdapter struct {
	validator *validator.Validate
	encoder   *serializers.Encoder
}

type FeedbackRequestDTO struct {
	RequestID       string `json:"request_id"       validate:"required,uuid,ne=00000000-0000-0000-0000-000000000000"`
	Accepted        bool   `json:"accepted"`
	Rating          int    `json:"rating"           validate:"min=-1,max=1"`
	Comment         string `json:"comment"          validate:"max=2000"`
	PreferredAnswer string `json:"preferred_answer" validate:"max=8000"`
}

type FeedbackResponseDTO struct {
	FeedbackID string `json:"feedback_id"`
	RequestID  string `json:"request_id"`
}

func NewFeedbackDTOAdapter(encoder *serializers.Encoder) *feedbackDTOAdapter {
	log.Trace("NewFeedbackDTOAdapter")

	return &feedbackDTOAdapter{
		validator: validator.New(),
		encoder:   encoder,
	}
}

func (a *feedbackDTOAdapter) FromDTO(ctx context.Context, body []byte) (*model.InferenceFeedback, error) {
	log.Trace("FeedbackDTOAdapter FromDTO")

	var dto FeedbackRequestDTO
	if err := a.encoder.DecodeStringToData(string(body), &dto); err != nil {
		return nil, domain.ErrValidationFailed.Extend(err.Error())
	}
	if err := a.validator.Struct(dto); err != nil {
		log.WithContext(ctx).WithError(err).Error("FeedbackRequestDTO validation failed")
		return nil, domain.ErrValidationFailed.Extend(err.Error())
	}
	requestID, err := parseRequiredUUID(dto.RequestID, "feedback request_id is invalid")
	if err != nil {
		return nil, err
	}
	return &model.InferenceFeedback{
		RequestID:       requestID,
		Accepted:        dto.Accepted,
		Rating:          dto.Rating,
		Comment:         dto.Comment,
		PreferredAnswer: dto.PreferredAnswer,
	}, nil
}

func (a *feedbackDTOAdapter) ToDTO(ctx context.Context, feedback *model.InferenceFeedback) ([]byte, error) {
	log.Trace("FeedbackDTOAdapter ToDTO")

	encoded, err := a.encoder.EncodeDataToString(FeedbackResponseDTO{
		FeedbackID: feedback.FeedbackID.String(),
		RequestID:  feedback.RequestID.String(),
	})
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("FeedbackResponseDTO encoding failed")
		return nil, err
	}
	return []byte(encoded), nil
}
