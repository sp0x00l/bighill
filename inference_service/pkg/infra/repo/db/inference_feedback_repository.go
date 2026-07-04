package db

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"inference_service/pkg/domain"
	"inference_service/pkg/domain/model"
	inferencepb "lib/data_contracts_lib/inference"
	coreDB "lib/shared_lib/db"
	msgConn "lib/shared_lib/messaging"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

type InferenceFeedbackRepository struct {
	coreDB.Database
	unitOfWork   *coreDB.UnitOfWork
	outbox       msgConn.OrderedOutbox
	topic        string
	outboxSignal func()
}

type InferenceFeedbackRepositoryOption func(*InferenceFeedbackRepository)

func WithPreferenceDatasetOutbox(outbox msgConn.OrderedOutbox, topic string) InferenceFeedbackRepositoryOption {
	log.Trace("WithPreferenceDatasetOutbox")

	return func(r *InferenceFeedbackRepository) {
		r.outbox = outbox
		r.topic = topic
	}
}

func WithOutboxSignal(signal func()) InferenceFeedbackRepositoryOption {
	log.Trace("WithOutboxSignal")

	return func(r *InferenceFeedbackRepository) {
		r.outboxSignal = signal
	}
}

func NewInferenceFeedbackRepository(db *coreDB.Database, opts ...InferenceFeedbackRepositoryOption) *InferenceFeedbackRepository {
	log.Trace("NewInferenceFeedbackRepository")

	repository := &InferenceFeedbackRepository{
		Database:   *db,
		unitOfWork: coreDB.NewUnitOfWork(db.Pool),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(repository)
		}
	}
	return repository
}

func (r *InferenceFeedbackRepository) RecordFeedback(ctx context.Context, feedback *model.InferenceFeedback, idempotencyKey uuid.UUID) (*model.InferenceFeedback, error) {
	log.Trace("InferenceFeedbackRepository RecordFeedback")

	var record *model.InferenceFeedback
	err := r.unitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx) error {
		var err error
		record, err = scanInferenceFeedback(tx.QueryRow(ctx, r.feedbackQuery(), feedbackArgs(feedback, idempotencyKey)))
		if err != nil {
			if coreDB.IsForeignKeyViolation(err) {
				return domain.ErrValidationFailed.Extend("tenant projection is not ready")
			}
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
				preference_example_id, feedback_id, request_id, user_id, dataset_id, model_id, prompt_text,
				accepted_answer, rejected_answer, rating, feedback_label
			)
		SELECT
				@preference_example_id,
				f.feedback_id::uuid,
				req.request_id,
				f.user_id::uuid,
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
		  AND req.user_id = f.user_id::uuid
		ON CONFLICT (feedback_id) DO UPDATE SET
			user_id = EXCLUDED.user_id,
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
	userID := ""
	modelID := ""
	query := `WITH request_scope AS (
		SELECT
			req.request_id,
			req.user_id,
			req.dataset_id,
			req.model_id,
			m.adapter_uri,
			m.base_model,
			m.model_version
		FROM ` + r.Name + `.inference_requests req
		JOIN ` + r.Name + `.inference_models m ON m.model_id = req.model_id
		WHERE req.request_id = @request_id
		  AND req.user_id = @user_id
		  AND (@dataset_id::uuid IS NULL OR req.dataset_id = @dataset_id)
		  AND (@model_id::uuid IS NULL OR req.model_id = @model_id)
		  AND req.model_id IS NOT NULL
		  AND m.adapter_uri <> ''
	), eligible_examples AS (
		SELECT DISTINCT ON (p.prompt_text, p.accepted_answer, p.rejected_answer)
			p.preference_example_id::text,
			p.feedback_id::text,
			p.request_id::text,
			p.user_id::text,
			p.dataset_id::text,
			p.model_id::text,
			CASE WHEN substr(md5(p.preference_example_id::text), 1, 1) IN ('0', '1', '2') THEN 'EVAL' ELSE 'TRAIN' END AS split,
			p.prompt_text,
			p.accepted_answer,
			p.rejected_answer,
			p.rating,
			p.feedback_label,
			p.created_at
		FROM ` + r.Name + `.preference_examples p
		JOIN request_scope s ON s.user_id = p.user_id AND s.dataset_id = p.dataset_id AND s.model_id = p.model_id
		WHERE p.accepted_answer <> ''
		  AND p.rejected_answer <> ''
			  AND p.feedback_label = 'REJECTED'
			  AND p.rating < 0
			ORDER BY p.prompt_text, p.accepted_answer, p.rejected_answer, p.created_at DESC, p.preference_example_id DESC
		), limited_examples AS (
				SELECT
					preference_example_id, feedback_id, request_id, user_id, dataset_id, model_id,
					split, prompt_text, accepted_answer, rejected_answer, rating, feedback_label, created_at
			FROM eligible_examples
			ORDER BY created_at DESC, preference_example_id DESC
			LIMIT @limit
		)
	SELECT
		s.dataset_id::text,
		s.user_id::text,
		s.model_id::text,
		s.adapter_uri,
		s.base_model,
		s.model_version,
		COALESCE((
			SELECT jsonb_agg(jsonb_build_object(
					'preference_example_id', e.preference_example_id,
					'feedback_id', e.feedback_id,
					'request_id', e.request_id,
					'user_id', e.user_id,
					'dataset_id', e.dataset_id,
					'model_id', e.model_id,
					'split', e.split,
					'prompt_text', e.prompt_text,
					'accepted_answer', e.accepted_answer,
					'rejected_answer', e.rejected_answer,
				'rating', e.rating,
				'feedback_label', e.feedback_label
				) ORDER BY e.created_at, e.preference_example_id)
				FROM limited_examples e
			), '[]'::jsonb)::text
	FROM request_scope s`
	row := r.Pool.QueryRow(ctx, query, preferenceDatasetArgs(request))
	parentAdapterURI := ""
	parentBaseModel := ""
	parentModelVersion := 0
	if err := row.Scan(&datasetID, &userID, &modelID, &parentAdapterURI, &parentBaseModel, &parentModelVersion, &raw); err != nil {
		if err == pgx.ErrNoRows {
			return nil, domain.ErrValidationFailed.Extend("preference dataset request does not match an inference request with a model")
		}
		return nil, fmt.Errorf("read preference dataset: %w", err)
	}
	examples, err := decodePreferenceExamples(raw)
	if err != nil {
		return nil, err
	}
	examples = ensurePreferenceTrainingSplit(examples)
	return &model.PreferenceDataset{
		RequestID:          request.RequestID,
		UserID:             uuid.MustParse(userID),
		DatasetID:          uuid.MustParse(datasetID),
		ModelID:            uuid.MustParse(modelID),
		ParentAdapterURI:   parentAdapterURI,
		ParentBaseModel:    parentBaseModel,
		ParentModelVersion: parentModelVersion,
		OutputURI:          request.OutputURI,
		Format:             "DPO_JSONL",
		EligibilityPolicy:  "complete_rejected_pairs_by_source_model_v1",
		MinExamples:        request.MinExamples,
		Limit:              request.Limit,
		Examples:           examples,
	}, nil
}

func (r *InferenceFeedbackRepository) RecordPreferenceDatasetSnapshot(ctx context.Context, dataset *model.PreferenceDataset, request model.PreferenceDatasetExportRequest) (*model.PreferenceDataset, error) {
	log.Trace("InferenceFeedbackRepository RecordPreferenceDatasetSnapshot")

	err := r.unitOfWork.Do(ctx, func(ctx context.Context, tx pgx.Tx) error {
		query := `INSERT INTO ` + r.Name + `.preference_dataset_snapshots (
					preference_dataset_id, user_id, dataset_id, model_id, parent_adapter_uri, parent_base_model,
					parent_model_version, source_request_id, output_uri, evaluation_output_uri,
					format, eligibility_policy, example_count, min_examples, limit_count
				) VALUES (
					@preference_dataset_id, @user_id, @dataset_id, @model_id, @parent_adapter_uri, @parent_base_model,
					@parent_model_version, @source_request_id, @output_uri, @evaluation_output_uri,
				@format, @eligibility_policy, @example_count, @min_examples, @limit_count
			)
		ON CONFLICT (preference_dataset_id) DO NOTHING`
		if _, err := tx.Exec(ctx, query, preferenceDatasetSnapshotArgs(dataset, request)); err != nil {
			if coreDB.IsForeignKeyViolation(err) {
				return domain.ErrValidationFailed.Extend("tenant projection is not ready")
			}
			return fmt.Errorf("record preference dataset snapshot: %w", err)
		}
		if r.outbox != nil {
			if err := r.outbox.EnqueueTx(ctx, tx, preferenceDatasetReadyMessage(r.topic, dataset, request)); err != nil {
				return fmt.Errorf("enqueue preference dataset ready: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if r.outbox != nil && r.outboxSignal != nil {
		r.outboxSignal()
	}
	return dataset, nil
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
		"user_id":    nullableUUID(request.UserID),
		"dataset_id": nullableUUID(request.DatasetID),
		"model_id":   nullableUUID(request.ModelID),
		"limit":      limit,
	}
}

func preferenceDatasetSnapshotArgs(dataset *model.PreferenceDataset, request model.PreferenceDatasetExportRequest) pgx.NamedArgs {
	log.Trace("preferenceDatasetSnapshotArgs")

	return pgx.NamedArgs{
		"preference_dataset_id": nullableUUID(dataset.PreferenceDatasetID),
		"user_id":               nullableUUID(dataset.UserID),
		"dataset_id":            nullableUUID(dataset.DatasetID),
		"model_id":              nullableUUID(dataset.ModelID),
		"parent_adapter_uri":    strings.TrimSpace(dataset.ParentAdapterURI),
		"parent_base_model":     strings.TrimSpace(dataset.ParentBaseModel),
		"parent_model_version":  dataset.ParentModelVersion,
		"source_request_id":     nullableUUID(dataset.RequestID),
		"output_uri":            strings.TrimSpace(dataset.OutputURI),
		"evaluation_output_uri": strings.TrimSpace(dataset.EvaluationOutputURI),
		"format":                strings.TrimSpace(dataset.Format),
		"eligibility_policy":    strings.TrimSpace(dataset.EligibilityPolicy),
		"example_count":         dataset.ExampleCount(),
		"min_examples":          request.MinExamples,
		"limit_count":           request.Limit,
	}
}

func preferenceDatasetReadyMessage(topic string, dataset *model.PreferenceDataset, request model.PreferenceDatasetExportRequest) msgConn.OutboundMessage {
	log.Trace("preferenceDatasetReadyMessage")

	payload := mustMarshal(&inferencepb.PreferenceDatasetReadyEvent{
		PreferenceDatasetId: dataset.PreferenceDatasetID.String(),
		UserId:              dataset.UserID.String(),
		DatasetId:           dataset.DatasetID.String(),
		ModelId:             dataset.ModelID.String(),
		SourceRequestId:     dataset.RequestID.String(),
		OutputUri:           strings.TrimSpace(dataset.OutputURI),
		EvaluationOutputUri: strings.TrimSpace(dataset.EvaluationOutputURI),
		ExampleCount:        int32(dataset.ExampleCount()),
		Format:              strings.TrimSpace(dataset.Format),
		MinExamples:         int32(request.MinExamples),
		Limit:               int32(request.Limit),
		ParentAdapterUri:    strings.TrimSpace(dataset.ParentAdapterURI),
		ParentBaseModel:     strings.TrimSpace(dataset.ParentBaseModel),
		ParentModelVersion:  int32(dataset.ParentModelVersion),
	})
	return msgConn.OutboundMessage{
		Topic: topic,
		Message: msgConn.Message{
			ResourceKey: dataset.DatasetID,
			MsgType:     msgConn.MsgTypePreferenceDatasetReady,
			Payload:     payload,
		},
		DispatchKey: "preference_dataset_ready:" + dataset.PreferenceDatasetID.String(),
	}
}

func ensurePreferenceTrainingSplit(examples []model.PreferenceExample) []model.PreferenceExample {
	log.Trace("ensurePreferenceTrainingSplit")

	if len(examples) == 0 {
		return examples
	}
	for _, example := range examples {
		if example.Split == "" || example.Split == "TRAIN" {
			return examples
		}
	}
	out := make([]model.PreferenceExample, len(examples))
	copy(out, examples)
	out[0].Split = "TRAIN"
	return out
}

func mustMarshal(payload proto.Message) []byte {
	log.Trace("mustMarshal")

	out, err := proto.Marshal(payload)
	if err != nil {
		panic(err)
	}
	return out
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
		UserID              string `json:"user_id"`
		DatasetID           string `json:"dataset_id"`
		ModelID             string `json:"model_id"`
		Split               string `json:"split"`
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
			UserID:              uuid.MustParse(row.UserID),
			DatasetID:           uuid.MustParse(row.DatasetID),
			ModelID:             uuid.MustParse(row.ModelID),
			Split:               strings.TrimSpace(row.Split),
			PromptText:          row.PromptText,
			AcceptedAnswer:      row.AcceptedAnswer,
			RejectedAnswer:      row.RejectedAnswer,
			Rating:              row.Rating,
			FeedbackLabel:       row.FeedbackLabel,
		})
	}
	return examples, nil
}
