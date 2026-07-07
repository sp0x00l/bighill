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

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

const (
	userIDHeader = "X-User-ID"
	orgIDHeader  = "X-Org-ID"
)

type DatasetResolver struct {
	baseURL string
	client  *http.Client
}

func NewDatasetResolver(baseURL string, client *http.Client) *DatasetResolver {
	log.Trace("NewDatasetResolver")

	return &DatasetResolver{
		baseURL: strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		client:  client,
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
	return model.MaterializedDatasetRef{
		DatasetID:         dto.ID,
		UserID:            dto.UserID,
		OrgID:             dto.OrgID,
		DatasetVersion:    fmt.Sprintf("%d", dto.DatasetVersion),
		FeatureSnapshotID: dto.FeatureSnapshotID,
		DatasetURI:        firstNonEmpty(dto.StorageLocation, dto.Location),
		TableName:         dto.TableName,
		TableFormat:       dto.TableFormat,
		ProcessingState:   dto.ProcessingState,
	}, nil
}

type datasetDTO struct {
	ID                string `json:"id"`
	UserID            string `json:"userId"`
	OrgID             string `json:"orgId"`
	Location          string `json:"location"`
	StorageLocation   string `json:"storageLocation"`
	TableName         string `json:"tableName"`
	TableFormat       string `json:"tableFormat"`
	ProcessingState   string `json:"processingState"`
	DatasetVersion    int    `json:"datasetVersion"`
	FeatureSnapshotID string `json:"featureSnapshotId"`
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
