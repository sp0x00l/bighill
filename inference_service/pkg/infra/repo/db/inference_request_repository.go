package db

import (
	"context"
	"fmt"

	"inference_service/pkg/domain"
	"inference_service/pkg/domain/model"
	coreDB "lib/shared_lib/db"

	"github.com/jackc/pgx/v5"
	log "github.com/sirupsen/logrus"
)

type InferenceRequestRepository struct {
	coreDB.Database
}

func NewInferenceRequestRepository(db *coreDB.Database) *InferenceRequestRepository {
	log.Trace("NewInferenceRequestRepository")

	return &InferenceRequestRepository{
		Database: *db,
	}
}

func (r *InferenceRequestRepository) RecordInferenceRequest(ctx context.Context, request *model.InferenceRequest) error {
	log.Trace("InferenceRequestRepository RecordInferenceRequest")

	query := `INSERT INTO ` + r.Name + `.inference_requests (
		request_id, user_id, dataset_id, model_id, embedding_snapshot_id, query_text, top_k,
		metadata_filters, retrieved_context_ids, retrieved_contexts, prompt_text, answer_text,
		prompt_strategy_version, generation_provider, generation_model, latency_ms, status, error_message
	) VALUES (
		@request_id, @user_id, @dataset_id, @model_id, @embedding_snapshot_id, @query_text, @top_k,
		@metadata_filters::jsonb, @retrieved_context_ids::jsonb, @retrieved_contexts::jsonb, @prompt_text, @answer_text,
		@prompt_strategy_version, @generation_provider, @generation_model, @latency_ms, @status, @error_message
	)
	ON CONFLICT (request_id) DO UPDATE SET
		user_id = EXCLUDED.user_id,
		dataset_id = EXCLUDED.dataset_id,
		model_id = EXCLUDED.model_id,
		embedding_snapshot_id = EXCLUDED.embedding_snapshot_id,
		query_text = EXCLUDED.query_text,
		top_k = EXCLUDED.top_k,
		metadata_filters = EXCLUDED.metadata_filters,
		retrieved_context_ids = EXCLUDED.retrieved_context_ids,
		retrieved_contexts = EXCLUDED.retrieved_contexts,
		prompt_text = EXCLUDED.prompt_text,
		answer_text = EXCLUDED.answer_text,
		prompt_strategy_version = EXCLUDED.prompt_strategy_version,
		generation_provider = EXCLUDED.generation_provider,
		generation_model = EXCLUDED.generation_model,
		latency_ms = EXCLUDED.latency_ms,
		status = EXCLUDED.status,
		error_message = EXCLUDED.error_message`

	if _, err := r.Pool.Exec(ctx, query, inferenceRequestArgs(request)); err != nil {
		r.LogPoolStatsOnError(ctx, "record inference request failed", err)
		if coreDB.IsForeignKeyViolation(err) {
			return domain.ErrValidationFailed.Extend("tenant projection is not ready")
		}
		return fmt.Errorf("record inference request: %w", err)
	}
	return nil
}

func inferenceRequestArgs(request *model.InferenceRequest) pgx.NamedArgs {
	log.Trace("inferenceRequestArgs")

	return pgx.NamedArgs{
		"request_id":              nullableUUID(request.RequestID),
		"user_id":                 nullableUUID(request.UserID),
		"dataset_id":              nullableUUID(request.DatasetID),
		"model_id":                nullableUUID(request.ModelID),
		"embedding_snapshot_id":   nullableUUID(request.EmbeddingSnapshotID),
		"query_text":              request.QueryText,
		"top_k":                   request.TopK,
		"metadata_filters":        request.MetadataFilters,
		"retrieved_context_ids":   request.RetrievedContextIDs,
		"retrieved_contexts":      request.RetrievedContexts,
		"prompt_text":             request.PromptText,
		"answer_text":             request.AnswerText,
		"prompt_strategy_version": request.PromptStrategyVersion,
		"generation_provider":     request.GenerationProvider,
		"generation_model":        request.GenerationModel,
		"latency_ms":              request.LatencyMs,
		"status":                  request.Status.String(),
		"error_message":           request.ErrorMessage,
	}
}
