package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"training_service/pkg/domain"
	"training_service/pkg/domain/model"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

const (
	userIDHeader = "X-User-ID"
	orgIDHeader  = "X-Org-ID"

	tableFormatParquet          = "PARQUET"
	stateFeatureMaterialized    = "FEATURE_MATERIALIZED"
	stateEmbeddingsMaterialized = "EMBEDDINGS_MATERIALIZED"
	stateGraphMaterialized      = "GRAPH_MATERIALIZED"
)

type datasetDTO struct {
	ID                string `json:"id"                validate:"required,uuid"`
	UserID            string `json:"userId"            validate:"required,uuid"`
	OrgID             string `json:"orgId"             validate:"required,uuid"`
	Location          string `json:"location"`
	StorageLocation   string `json:"storageLocation"`
	TableName         string `json:"tableName"         validate:"required"`
	TableFormat       string `json:"tableFormat"       validate:"required"`
	ProcessingState   string `json:"processingState"   validate:"required"`
	DatasetVersion    int    `json:"datasetVersion"    validate:"gt=0"`
	FeatureSnapshotID string `json:"featureSnapshotId" validate:"required,uuid"`
}

type DatasetResolver struct {
	baseURL string
	client  *http.Client
	adapter *datasetDTOAdapter
}

type datasetDTOAdapter struct {
	validator *validator.Validate
}

func NewDatasetResolver(baseURL string, client *http.Client) *DatasetResolver {
	log.Trace("NewDatasetResolver")

	return &DatasetResolver{
		baseURL: strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		client:  client,
		adapter: newDatasetDTOAdapter(),
	}
}

func (r *DatasetResolver) ResolveMaterializedDataset(ctx context.Context, userID uuid.UUID, orgID uuid.UUID, datasetID uuid.UUID) (model.MaterializedDatasetRef, error) {
	log.Trace("DatasetResolver ResolveMaterializedDataset")

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.baseURL+"/v1/data/registry/"+url.PathEscape(datasetID.String())+"/materialization", nil)
	if err != nil {
		return model.MaterializedDatasetRef{}, fmt.Errorf("%w: build dataset resolver request: %w", domain.ErrDependencyFailed, err)
	}
	req.Header.Set(userIDHeader, userID.String())
	req.Header.Set(orgIDHeader, orgID.String())
	resp, err := r.client.Do(req)
	if err != nil {
		return model.MaterializedDatasetRef{}, fmt.Errorf("%w: resolve dataset materialization: %w", domain.ErrDependencyFailed, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return model.MaterializedDatasetRef{}, domain.ErrValidationFailed.Extend("dataset not found")
	}
	if resp.StatusCode == http.StatusBadRequest {
		return model.MaterializedDatasetRef{}, domain.ErrValidationFailed.Extend("dataset is not materialized")
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return model.MaterializedDatasetRef{}, fmt.Errorf("%w: dataset resolver status %d: %s", domain.ErrDependencyFailed, resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	var dto datasetDTO
	if err := json.NewDecoder(resp.Body).Decode(&dto); err != nil {
		return model.MaterializedDatasetRef{}, fmt.Errorf("%w: decode dataset resolver response: %w", domain.ErrDependencyFailed, err)
	}
	return r.adapter.FromDTO(ctx, dto, datasetID, orgID)
}

func newDatasetDTOAdapter() *datasetDTOAdapter {
	log.Trace("newDatasetDTOAdapter")

	return &datasetDTOAdapter{validator: validator.New()}
}

func (a *datasetDTOAdapter) FromDTO(ctx context.Context, dto datasetDTO, datasetID uuid.UUID, orgID uuid.UUID) (model.MaterializedDatasetRef, error) {
	log.Trace("datasetDTOAdapter FromDTO")

	if err := a.validator.Struct(dto); err != nil {
		log.WithContext(ctx).WithError(err).Error("datasetDTO validation failed")
		return model.MaterializedDatasetRef{}, domain.ErrValidationFailed.Extend(err.Error())
	}

	if strings.TrimSpace(dto.ID) != datasetID.String() {
		return model.MaterializedDatasetRef{}, domain.ErrValidationFailed.Extend("dataset resolver returned a different dataset")
	}
	if strings.TrimSpace(dto.OrgID) != orgID.String() {
		return model.MaterializedDatasetRef{}, domain.ErrValidationFailed.Extend("dataset does not belong to active org")
	}
	state := strings.TrimSpace(dto.ProcessingState)
	if state != stateFeatureMaterialized && state != stateEmbeddingsMaterialized && state != stateGraphMaterialized {
		return model.MaterializedDatasetRef{}, domain.ErrValidationFailed.Extend("dataset is not materialized")
	}
	if strings.TrimSpace(dto.TableFormat) != tableFormatParquet {
		return model.MaterializedDatasetRef{}, domain.ErrValidationFailed.Extend("training requires a parquet dataset")
	}
	datasetURI := firstNonEmpty(dto.StorageLocation, dto.Location)
	if datasetURI == "" {
		return model.MaterializedDatasetRef{}, domain.ErrValidationFailed.Extend("dataset uri is required")
	}

	return model.MaterializedDatasetRef{
		DatasetID:         dto.ID,
		UserID:            dto.UserID,
		OrgID:             dto.OrgID,
		DatasetVersion:    fmt.Sprintf("%d", dto.DatasetVersion),
		FeatureSnapshotID: dto.FeatureSnapshotID,
		DatasetURI:        datasetURI,
		TableName:         dto.TableName,
		TableFormat:       dto.TableFormat,
		ProcessingState:   dto.ProcessingState,
	}, nil
}

func firstNonEmpty(values ...string) string {
	log.Trace("firstNonEmpty")

	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
