package app

import (
	"context"
	"strings"

	"inference_service/pkg/domain"
	"inference_service/pkg/domain/model"

	log "github.com/sirupsen/logrus"
)

type InferenceUsecase interface {
	RegisterModel(ctx context.Context, inferenceModel model.InferenceModel) error
}

type inferenceUsecase struct{}

func NewInferenceUsecase() InferenceUsecase {
	log.Trace("NewInferenceUsecase")

	return &inferenceUsecase{}
}

func (u *inferenceUsecase) RegisterModel(_ context.Context, inferenceModel model.InferenceModel) error {
	log.Trace("InferenceUsecase RegisterModel")

	if strings.TrimSpace(inferenceModel.ModelID) == "" {
		return domain.ErrValidationFailed.Extend("model id is required")
	}
	if strings.TrimSpace(inferenceModel.ModelURI) == "" {
		return domain.ErrValidationFailed.Extend("model uri is required")
	}
	return nil
}
