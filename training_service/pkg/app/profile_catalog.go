package app

import (
	"context"
	"fmt"
	"strings"

	"training_service/pkg/domain"
	"training_service/pkg/domain/model"

	log "github.com/sirupsen/logrus"
)

const profileVersionSeparator = "@"

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
		trainingByName[strings.TrimSpace(profile.Name)] = profile
	}
	evaluationByName := make(map[string]string, len(evaluationProfiles))
	for name, profile := range evaluationProfiles {
		evaluationByName[strings.TrimSpace(name)] = strings.TrimSpace(profile)
	}
	return &staticTrainingProfileCatalog{
		trainingProfiles:             trainingByName,
		evaluationProfiles:           evaluationByName,
		defaultTrainingProfileName:   strings.TrimSpace(defaultTrainingProfileName),
		defaultEvaluationProfileName: strings.TrimSpace(defaultEvaluationProfileName),
	}
}

func (c *staticTrainingProfileCatalog) ResolveTrainingProfile(_ context.Context, name string) (model.TrainingProfile, error) {
	log.Trace("staticTrainingProfileCatalog ResolveTrainingProfile")

	name = strings.TrimSpace(name)
	if name == "" {
		name = c.defaultTrainingProfileName
	}
	if err := validatePinnedProfileName("training profile", name); err != nil {
		return model.TrainingProfile{}, err
	}
	profile, ok := c.trainingProfiles[name]
	if !ok {
		return model.TrainingProfile{}, domain.ErrValidationFailed.Extend("unknown training profile")
	}
	if err := validatePinnedProfileName("training profile", profile.Name); err != nil {
		return model.TrainingProfile{}, err
	}
	return profile, nil
}

func (c *staticTrainingProfileCatalog) ResolveEvaluationProfile(_ context.Context, name string) (string, error) {
	log.Trace("staticTrainingProfileCatalog ResolveEvaluationProfile")

	name = strings.TrimSpace(name)
	if name == "" {
		name = c.defaultEvaluationProfileName
	}
	if err := validatePinnedProfileName("evaluation profile", name); err != nil {
		return "", err
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

func validatePinnedProfileName(kind string, name string) error {
	log.Trace("validatePinnedProfileName")

	name = strings.TrimSpace(name)
	base, version, ok := strings.Cut(name, profileVersionSeparator)
	if !ok || strings.TrimSpace(base) == "" || strings.TrimSpace(version) == "" {
		return domain.ErrValidationFailed.Extend(fmt.Sprintf("%s must be version pinned", kind))
	}
	return nil
}
