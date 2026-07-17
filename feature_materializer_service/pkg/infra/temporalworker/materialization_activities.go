package temporalworker

import (
	"context"
	"strings"

	usecase "feature_materializer_service/pkg/app"
	"feature_materializer_service/pkg/domain"
	"feature_materializer_service/pkg/domain/model"
	"lib/shared_lib/ctxutil"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type MaterializationActivities struct {
	rawSnapshotUsecase     usecase.RawSnapshotUsecase
	featureSnapshotUsecase usecase.FeatureSnapshotUsecase
	embeddingUsecase       usecase.EmbeddingMaterializationUsecase
	graphUsecase           usecase.GraphMaterializationUsecase
}

func NewMaterializationActivities(
	rawSnapshotUsecase usecase.RawSnapshotUsecase,
	featureSnapshotUsecase usecase.FeatureSnapshotUsecase,
	embeddingUsecase usecase.EmbeddingMaterializationUsecase,
	graphUsecase usecase.GraphMaterializationUsecase,
) *MaterializationActivities {
	log.Trace("NewMaterializationActivities")

	return &MaterializationActivities{
		rawSnapshotUsecase:     rawSnapshotUsecase,
		featureSnapshotUsecase: featureSnapshotUsecase,
		embeddingUsecase:       embeddingUsecase,
		graphUsecase:           graphUsecase,
	}
}

func (a *MaterializationActivities) MaterializeRawSnapshot(ctx context.Context, input usecase.MaterializeRawSnapshotActivityInput) (*model.RawSnapshot, error) {
	log.Trace("MaterializationActivities MaterializeRawSnapshot")

	if err := validateMaterializeRawSnapshotInput(input); err != nil {
		return nil, err
	}
	ctx = ctxutil.WithActorOrg(ctx, input.DatasetFile.UserID, input.DatasetFile.OrgID)
	rawSnapshot, err := a.rawSnapshotUsecase.MaterializeRawSnapshot(ctx, &input.DatasetFile, input.IdempotencyKey)
	if err != nil {
		if existing, ok := domain.IsRawSnapshotAlreadyMaterialized(err); ok {
			return existing, nil
		}
		return nil, err
	}
	return rawSnapshot, nil
}

func (a *MaterializationActivities) BuildFeatureSnapshot(ctx context.Context, input usecase.BuildFeatureSnapshotActivityInput) (*model.FeatureSnapshot, error) {
	log.Trace("MaterializationActivities BuildFeatureSnapshot")

	if err := validateBuildFeatureSnapshotInput(input); err != nil {
		return nil, err
	}
	ctx = ctxutil.WithActorOrg(ctx, input.UserID, input.OrgID)
	featureSnapshot, err := a.featureSnapshotUsecase.BuildFeatureSnapshot(ctx, input.RawSnapshotID, input.IdempotencyKey)
	if err != nil {
		if existing, ok := domain.IsFeatureSnapshotAlreadyBuilt(err); ok {
			return existing, nil
		}
		return nil, err
	}
	return featureSnapshot, nil
}

func (a *MaterializationActivities) MaterializeEmbeddings(ctx context.Context, input usecase.MaterializeEmbeddingsActivityInput) (*model.EmbeddingSnapshot, error) {
	log.Trace("MaterializationActivities MaterializeEmbeddings")

	input.Strategy = model.NormalizeEmbeddingStrategy(input.Strategy)
	if err := validateMaterializeEmbeddingsInput(input); err != nil {
		return nil, err
	}
	ctx = ctxutil.WithActorOrg(ctx, input.UserID, input.OrgID)
	embeddingSnapshot, err := a.embeddingUsecase.MaterializeEmbeddings(ctx, input.FeatureSnapshotID, input.IdempotencyKey, input.Strategy)
	if err != nil {
		if existing, ok := domain.IsEmbeddingsAlreadyMaterialized(err); ok {
			return existing, nil
		}
		return nil, err
	}
	return embeddingSnapshot, nil
}

func (a *MaterializationActivities) MaterializeGraph(ctx context.Context, input usecase.MaterializeGraphActivityInput) (*model.GraphSnapshot, error) {
	log.Trace("MaterializationActivities MaterializeGraph")

	input.Strategy = model.ApplyGraphExtractionStrategyDefaults(input.Strategy)
	if err := validateMaterializeGraphInput(input); err != nil {
		return nil, err
	}
	ctx = ctxutil.WithActorOrg(ctx, input.UserID, input.OrgID)
	graphSnapshot, err := a.graphUsecase.MaterializeGraph(ctx, input.EmbeddingSnapshotID, input.IdempotencyKey, input.Strategy)
	if err != nil {
		if existing, ok := domain.IsGraphAlreadyMaterialized(err); ok {
			return existing, nil
		}
		return nil, err
	}
	return graphSnapshot, nil
}

func validateMaterializeRawSnapshotInput(input usecase.MaterializeRawSnapshotActivityInput) error {
	log.Trace("validateMaterializeRawSnapshotInput")

	if input.IdempotencyKey == uuid.Nil {
		return domain.ErrValidationFailed.Extend("idempotency_key is required")
	}
	return validateDatasetFile(input.DatasetFile)
}

func validateBuildFeatureSnapshotInput(input usecase.BuildFeatureSnapshotActivityInput) error {
	log.Trace("validateBuildFeatureSnapshotInput")

	if input.RawSnapshotID == uuid.Nil {
		return domain.ErrValidationFailed.Extend("raw_snapshot_id is required")
	}
	return validateActivityActor(input.UserID, input.OrgID, input.IdempotencyKey)
}

func validateMaterializeEmbeddingsInput(input usecase.MaterializeEmbeddingsActivityInput) error {
	log.Trace("validateMaterializeEmbeddingsInput")

	if input.FeatureSnapshotID == uuid.Nil {
		return domain.ErrValidationFailed.Extend("feature_snapshot_id is required")
	}
	if err := validateActivityActor(input.UserID, input.OrgID, input.IdempotencyKey); err != nil {
		return err
	}
	if err := model.ValidateEmbeddingStrategy(input.Strategy); err != nil {
		return domain.ErrValidationFailed.Extend(err.Error())
	}
	return nil
}

func validateMaterializeGraphInput(input usecase.MaterializeGraphActivityInput) error {
	log.Trace("validateMaterializeGraphInput")

	if input.EmbeddingSnapshotID == uuid.Nil {
		return domain.ErrValidationFailed.Extend("embedding_snapshot_id is required")
	}
	if err := validateActivityActor(input.UserID, input.OrgID, input.IdempotencyKey); err != nil {
		return err
	}
	if strings.TrimSpace(input.Strategy.ExtractionModel) == "" {
		return domain.ErrValidationFailed.Extend("extraction_model is required")
	}
	if strings.TrimSpace(input.Strategy.ExtractionPromptVersion) == "" {
		return domain.ErrValidationFailed.Extend("extraction_prompt_version is required")
	}
	if strings.TrimSpace(input.Strategy.ExtractionSchemaVersion) == "" {
		return domain.ErrValidationFailed.Extend("extraction_schema_version is required")
	}
	return nil
}

func validateActivityActor(userID uuid.UUID, orgID uuid.UUID, idempotencyKey uuid.UUID) error {
	log.Trace("validateActivityActor")

	if err := validateActor(userID, orgID); err != nil {
		return err
	}
	if idempotencyKey == uuid.Nil {
		return domain.ErrValidationFailed.Extend("idempotency_key is required")
	}
	return nil
}

func validateActor(userID uuid.UUID, orgID uuid.UUID) error {
	log.Trace("validateActor")

	if userID == uuid.Nil {
		return domain.ErrValidationFailed.Extend("user_id is required")
	}
	if orgID == uuid.Nil {
		return domain.ErrValidationFailed.Extend("org_id is required")
	}
	return nil
}

func validateDatasetFile(datasetFile model.DatasetFile) error {
	log.Trace("validateDatasetFile")

	if datasetFile.DatasetID == uuid.Nil {
		return domain.ErrValidationFailed.Extend("dataset_id is required")
	}
	if err := validateActor(datasetFile.UserID, datasetFile.OrgID); err != nil {
		return err
	}
	if strings.TrimSpace(datasetFile.StorageLocation) == "" {
		return domain.ErrValidationFailed.Extend("storage_location is required")
	}
	if strings.TrimSpace(datasetFile.TableName) == "" {
		return domain.ErrValidationFailed.Extend("table_name is required")
	}
	if strings.TrimSpace(datasetFile.TableFormat) == "" {
		return domain.ErrValidationFailed.Extend("table_format is required")
	}
	if strings.TrimSpace(datasetFile.CatalogProvider) == "" {
		return domain.ErrValidationFailed.Extend("catalog_provider is required")
	}
	return nil
}
