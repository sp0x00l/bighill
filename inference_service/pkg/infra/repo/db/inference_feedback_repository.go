package db

import (
	"context"
	"fmt"

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
			feedback_id, idempotency_key, request_id, user_id, accepted, rating, comment
		) VALUES (
			@feedback_id, @idempotency_key, @request_id, @user_id, @accepted, @rating, @comment
		)
		ON CONFLICT (idempotency_key) DO UPDATE SET
			accepted = EXCLUDED.accepted,
			rating = EXCLUDED.rating,
			comment = EXCLUDED.comment
		RETURNING feedback_id::text, request_id::text, user_id::text, accepted, rating, comment
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
			CASE WHEN f.accepted THEN req.answer_text ELSE '' END,
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
	SELECT feedback_id, request_id, user_id, accepted, rating, comment
	FROM upserted_feedback`
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
		"preference_example_id": nullableUUID(uuid.NewSHA1(uuid.NameSpaceURL, []byte("preference:"+idempotencyKey.String()))),
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
	); err != nil {
		return nil, err
	}
	record.FeedbackID = uuid.MustParse(feedbackID)
	record.RequestID = uuid.MustParse(requestID)
	record.UserID = uuid.MustParse(userID)
	return record, nil
}
