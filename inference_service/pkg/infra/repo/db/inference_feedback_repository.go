package db

import (
	"context"
	"encoding/json"
	"fmt"

	"inference_service/pkg/domain"
	"inference_service/pkg/domain/model"
	coreDB "lib/shared_lib/db"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	log "github.com/sirupsen/logrus"
)

type InferenceFeedbackRepository struct {
	coreDB.Database
	unitOfWork *coreDB.UnitOfWork
}

func NewInferenceFeedbackRepository(db *coreDB.Database) *InferenceFeedbackRepository {
	log.Trace("NewInferenceFeedbackRepository")

	return &InferenceFeedbackRepository{
		Database:   *db,
		unitOfWork: coreDB.NewUnitOfWork(db.Pool),
	}
}

func (r *InferenceFeedbackRepository) RecordFeedback(ctx context.Context, feedback *model.InferenceFeedback, idempotencyKey uuid.UUID) (*model.InferenceFeedback, error) {
	log.Trace("InferenceFeedbackRepository RecordFeedback")

	var record *model.InferenceFeedback
	err := r.unitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx) error {
		var err error
		record, err = scanInferenceFeedback(tx.QueryRow(ctx, r.feedbackQuery(), feedbackArgs(feedback, idempotencyKey)))
		if err != nil {
			return fmt.Errorf("record inference feedback: %w", err)
		}
		return nil
	})
	return record, err
}

func (r *InferenceFeedbackRepository) feedbackQuery() string {
	log.Trace("InferenceFeedbackRepository feedbackQuery")

	return `WITH upserted_feedback AS (
		INSERT INTO ` + r.Name + `.inference_feedback (
			feedback_id, idempotency_key, request_id, user_id, accepted, rating, comment, preferred_answer
		) VALUES (
			@feedback_id, @idempotency_key, @request_id, @user_id, @accepted, @rating, @comment, @preferred_answer
		)
		ON CONFLICT (idempotency_key) DO UPDATE SET
			accepted = EXCLUDED.accepted,
			rating = EXCLUDED.rating,
			comment = EXCLUDED.comment,
			preferred_answer = EXCLUDED.preferred_answer
		RETURNING feedback_id::text, request_id::text, user_id::text, accepted, rating, comment, preferred_answer
	), upserted_preference AS (
		INSERT INTO ` + r.Name + `.preference_examples (
			preference_example_id, feedback_id, request_id, dataset_id, model_id, prompt_text,
			accepted_answer, rejected_answer, rating, feedback_label
		)
		SELECT
			@preference_example_id,
			f.feedback_id::uuid,
			req.request_id,
			req.dataset_id,
			req.model_id,
			req.prompt_text,
			CASE WHEN f.accepted THEN req.answer_text ELSE f.preferred_answer END,
			CASE WHEN f.accepted THEN '' ELSE req.answer_text END,
			f.rating,
			CASE WHEN f.accepted THEN 'ACCEPTED' ELSE 'REJECTED' END
		FROM upserted_feedback f
		JOIN ` + r.Name + `.inference_requests req ON req.request_id = f.request_id::uuid
		WHERE req.model_id IS NOT NULL
		ON CONFLICT (feedback_id) DO UPDATE SET
			accepted_answer = EXCLUDED.accepted_answer,
			rejected_answer = EXCLUDED.rejected_answer,
			rating = EXCLUDED.rating,
			feedback_label = EXCLUDED.feedback_label
		RETURNING preference_example_id
	)
	SELECT feedback_id, request_id, user_id, accepted, rating, comment, preferred_answer
	FROM upserted_feedback`
}

func (r *InferenceFeedbackRepository) ReadPreferenceDataset(ctx context.Context, request model.PreferenceDatasetExportRequest) (*model.PreferenceDataset, error) {
	log.Trace("InferenceFeedbackRepository ReadPreferenceDataset")

	raw := ""
	datasetID := ""
	modelID := ""
	query := `WITH request_scope AS (
		SELECT request_id, dataset_id, model_id
		FROM ` + r.Name + `.inference_requests
		WHERE request_id = @request_id
		  AND (@dataset_id::uuid IS NULL OR dataset_id = @dataset_id)
		  AND (@model_id::uuid IS NULL OR model_id = @model_id)
		  AND model_id IS NOT NULL
	), limited_examples AS (
		SELECT
			p.preference_example_id::text,
			p.feedback_id::text,
			p.request_id::text,
			p.dataset_id::text,
			p.model_id::text,
			p.prompt_text,
			p.accepted_answer,
			p.rejected_answer,
			p.rating,
			p.feedback_label,
			p.created_at
		FROM ` + r.Name + `.preference_examples p
		JOIN request_scope s ON s.dataset_id = p.dataset_id AND s.model_id = p.model_id
		WHERE p.accepted_answer <> ''
		  AND p.rejected_answer <> ''
		ORDER BY p.created_at DESC
		LIMIT @limit
	)
	SELECT
		s.dataset_id::text,
		s.model_id::text,
		COALESCE((
			SELECT jsonb_agg(jsonb_build_object(
				'preference_example_id', e.preference_example_id,
				'feedback_id', e.feedback_id,
				'request_id', e.request_id,
				'dataset_id', e.dataset_id,
				'model_id', e.model_id,
				'prompt_text', e.prompt_text,
				'accepted_answer', e.accepted_answer,
				'rejected_answer', e.rejected_answer,
				'rating', e.rating,
				'feedback_label', e.feedback_label
			) ORDER BY e.created_at)
			FROM limited_examples e
		), '[]'::jsonb)::text
	FROM request_scope s`
	row := r.Pool.QueryRow(ctx, query, preferenceDatasetArgs(request))
	if err := row.Scan(&datasetID, &modelID, &raw); err != nil {
		if err == pgx.ErrNoRows {
			return nil, domain.ErrValidationFailed.Extend("preference dataset request does not match an inference request with a model")
		}
		return nil, fmt.Errorf("read preference dataset: %w", err)
	}
	examples, err := decodePreferenceExamples(raw)
	if err != nil {
		return nil, err
	}
	return &model.PreferenceDataset{
		RequestID: request.RequestID,
		DatasetID: uuid.MustParse(datasetID),
		ModelID:   uuid.MustParse(modelID),
		OutputURI: request.OutputURI,
		Examples:  examples,
	}, nil
}

func feedbackArgs(feedback *model.InferenceFeedback, idempotencyKey uuid.UUID) pgx.NamedArgs {
	log.Trace("feedbackArgs")

	return pgx.NamedArgs{
		"feedback_id":           nullableUUID(feedback.FeedbackID),
		"idempotency_key":       nullableUUID(idempotencyKey),
		"request_id":            nullableUUID(feedback.RequestID),
		"user_id":               nullableUUID(feedback.UserID),
		"accepted":              feedback.Accepted,
		"rating":                feedback.Rating,
		"comment":               feedback.Comment,
		"preferred_answer":      feedback.PreferredAnswer,
		"preference_example_id": nullableUUID(uuid.NewSHA1(uuid.NameSpaceURL, []byte("preference:"+idempotencyKey.String()))),
	}
}

func preferenceDatasetArgs(request model.PreferenceDatasetExportRequest) pgx.NamedArgs {
	log.Trace("preferenceDatasetArgs")

	limit := request.Limit
	if limit <= 0 {
		limit = 1000
	}
	return pgx.NamedArgs{
		"request_id": nullableUUID(request.RequestID),
		"dataset_id": nullableUUID(request.DatasetID),
		"model_id":   nullableUUID(request.ModelID),
		"limit":      limit,
	}
}

func scanInferenceFeedback(row pgx.Row) (*model.InferenceFeedback, error) {
	log.Trace("scanInferenceFeedback")

	var feedbackID, requestID, userID string
	record := &model.InferenceFeedback{}
	if err := row.Scan(
		&feedbackID,
		&requestID,
		&userID,
		&record.Accepted,
		&record.Rating,
		&record.Comment,
		&record.PreferredAnswer,
	); err != nil {
		return nil, err
	}
	record.FeedbackID = uuid.MustParse(feedbackID)
	record.RequestID = uuid.MustParse(requestID)
	record.UserID = uuid.MustParse(userID)
	return record, nil
}

func decodePreferenceExamples(raw string) ([]model.PreferenceExample, error) {
	log.Trace("decodePreferenceExamples")

	var rows []struct {
		PreferenceExampleID string `json:"preference_example_id"`
		FeedbackID          string `json:"feedback_id"`
		RequestID           string `json:"request_id"`
		DatasetID           string `json:"dataset_id"`
		ModelID             string `json:"model_id"`
		PromptText          string `json:"prompt_text"`
		AcceptedAnswer      string `json:"accepted_answer"`
		RejectedAnswer      string `json:"rejected_answer"`
		Rating              int    `json:"rating"`
		FeedbackLabel       string `json:"feedback_label"`
	}
	if err := json.Unmarshal([]byte(raw), &rows); err != nil {
		return nil, fmt.Errorf("decode preference dataset examples: %w", err)
	}
	examples := make([]model.PreferenceExample, 0, len(rows))
	for _, row := range rows {
		examples = append(examples, model.PreferenceExample{
			PreferenceExampleID: uuid.MustParse(row.PreferenceExampleID),
			FeedbackID:          uuid.MustParse(row.FeedbackID),
			RequestID:           uuid.MustParse(row.RequestID),
			DatasetID:           uuid.MustParse(row.DatasetID),
			ModelID:             uuid.MustParse(row.ModelID),
			PromptText:          row.PromptText,
			AcceptedAnswer:      row.AcceptedAnswer,
			RejectedAnswer:      row.RejectedAnswer,
			Rating:              row.Rating,
			FeedbackLabel:       row.FeedbackLabel,
		})
	}
	return examples, nil
}
