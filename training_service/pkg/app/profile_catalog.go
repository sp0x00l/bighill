package app

import (
	"context"

	"training_service/pkg/domain"
	"training_service/pkg/domain/model"

	log "github.com/sirupsen/logrus"
)

type TrainingProfileCatalog interface {
	ResolveTrainingProfile(ctx context.Context, name string) (model.TrainingProfile, error)
	ResolveEvaluationProfile(ctx context.Context, name string) (string, error)
	DefaultTrainingProfileName() string
	DefaultEvaluationProfileName() string
}

type staticTrainingProfileCatalog struct {
	trainingProfiles             map[string]model.TrainingProfile
	evaluationProfiles           map[string]string
	defaultTrainingProfileName   string
	defaultEvaluationProfileName string
}

func NewStaticTrainingProfileCatalog(trainingProfiles []model.TrainingProfile, defaultTrainingProfileName string, evaluationProfiles map[string]string, defaultEvaluationProfileName string) TrainingProfileCatalog {
	log.Trace("NewStaticTrainingProfileCatalog")

	trainingByName := make(map[string]model.TrainingProfile, len(trainingProfiles))
	for _, profile := range trainingProfiles {
		trainingByName[profile.Name] = profile
	}
	evaluationByName := make(map[string]string, len(evaluationProfiles))
	for name, profile := range evaluationProfiles {
		evaluationByName[name] = profile
	}
	return &staticTrainingProfileCatalog{
		trainingProfiles:             trainingByName,
		evaluationProfiles:           evaluationByName,
		defaultTrainingProfileName:   defaultTrainingProfileName,
		defaultEvaluationProfileName: defaultEvaluationProfileName,
	}
}

func (c *staticTrainingProfileCatalog) ResolveTrainingProfile(_ context.Context, name string) (model.TrainingProfile, error) {
	log.Trace("staticTrainingProfileCatalog ResolveTrainingProfile")

	if name == "" {
		name = c.defaultTrainingProfileName
	}
	profile, ok := c.trainingProfiles[name]
	if !ok {
		return model.TrainingProfile{}, domain.ErrValidationFailed.Extend("unknown training profile")
	}
	return profile, nil
}

func (c *staticTrainingProfileCatalog) ResolveEvaluationProfile(_ context.Context, name string) (string, error) {
	log.Trace("staticTrainingProfileCatalog ResolveEvaluationProfile")

	if name == "" {
		name = c.defaultEvaluationProfileName
	}
	profile, ok := c.evaluationProfiles[name]
	if !ok {
		return "", domain.ErrValidationFailed.Extend("unknown evaluation profile")
	}
	return profile, nil
}

func (c *staticTrainingProfileCatalog) DefaultTrainingProfileName() string {
	log.Trace("staticTrainingProfileCatalog DefaultTrainingProfileName")

	return c.defaultTrainingProfileName
}

func (c *staticTrainingProfileCatalog) DefaultEvaluationProfileName() string {
	log.Trace("staticTrainingProfileCatalog DefaultEvaluationProfileName")

	return c.defaultEvaluationProfileName
}
