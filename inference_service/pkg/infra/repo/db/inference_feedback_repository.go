package db

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"inference_service/pkg/domain"
	"inference_service/pkg/domain/model"
	coreDB "lib/shared_lib/db"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	log "github.com/sirupsen/logrus"
)

type InferenceFeedbackRepository struct {
	coreDB.Database
}

func NewInferenceFeedbackRepository(db *coreDB.Database) *InferenceFeedbackRepository {
	log.Trace("NewInferenceFeedbackRepository")

	return &InferenceFeedbackRepository{Database: *db}
}

func (r *InferenceFeedbackRepository) RecordFeedback(ctx context.Context, tx pgx.Tx, feedback *model.InferenceFeedback, idempotencyKey uuid.UUID) (*model.InferenceFeedback, error) {
	log.Trace("InferenceFeedbackRepository RecordFeedback")

	record, err := scanInferenceFeedback(tx.QueryRow(ctx, r.feedbackQuery(), feedbackArgs(feedback, idempotencyKey)))
	if err != nil {
		if coreDB.IsForeignKeyViolation(err) {
			return nil, domain.ErrValidationFailed.Extend("tenant projection is not ready")
		}
		return nil, fmt.Errorf("record inference feedback: %w", err)
	}
	return record, nil
}

func (r *InferenceFeedbackRepository) feedbackQuery() string {
	log.Trace("InferenceFeedbackRepository feedbackQuery")

	return `WITH upserted_feedback AS (
		INSERT INTO ` + r.Name + `.inference_feedback (
			feedback_id, idempotency_key, request_id, user_id, org_id, accepted, rating, comment, preferred_answer
		) VALUES (
			COALESCE(@feedback_id, uuid_generate_v4()), @idempotency_key, @request_id, @user_id, @org_id, @accepted, @rating, @comment, @preferred_answer
		)
		ON CONFLICT (idempotency_key) DO UPDATE SET
			user_id = EXCLUDED.user_id,
			org_id = EXCLUDED.org_id,
			accepted = EXCLUDED.accepted,
			rating = EXCLUDED.rating,
			comment = EXCLUDED.comment,
			preferred_answer = EXCLUDED.preferred_answer
		RETURNING feedback_id::text, request_id::text, user_id::text, org_id::text, accepted, rating, comment, preferred_answer
		), upserted_preference AS (
			INSERT INTO ` + r.Name + `.preference_examples (
				feedback_id, request_id, user_id, org_id, dataset_id, model_id, prompt_text,
				accepted_answer, rejected_answer, rating, feedback_label
			)
		SELECT
				f.feedback_id::uuid,
				req.request_id,
				f.user_id::uuid,
				f.org_id::uuid,
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
		  AND req.org_id = f.org_id::uuid
		ON CONFLICT (feedback_id) DO UPDATE SET
			user_id = EXCLUDED.user_id,
			org_id = EXCLUDED.org_id,
			accepted_answer = EXCLUDED.accepted_answer,
			rejected_answer = EXCLUDED.rejected_answer,
			rating = EXCLUDED.rating,
			feedback_label = EXCLUDED.feedback_label
		RETURNING preference_example_id
	)
	SELECT feedback_id, request_id, user_id, org_id, accepted, rating, comment, preferred_answer
	FROM upserted_feedback`
}

func (r *InferenceFeedbackRepository) ReadPreferenceDataset(ctx context.Context, request model.PreferenceDatasetBuildRequest) (*model.PreferenceDataset, error) {
	log.Trace("InferenceFeedbackRepository ReadPreferenceDataset")

	raw := ""
	datasetID := ""
	userID := ""
	modelID := ""
	parentModelKindValue := ""
	query := `WITH model_scope AS (
		SELECT
			m.model_kind,
			m.artifact_location,
			m.artifact_checksum,
			m.adapter_uri,
			m.base_model,
			m.name,
			m.lineage_name,
			m.model_version
		FROM ` + r.Name + `.inference_models m
		WHERE m.model_id = @model_id
		  AND (m.org_id = @org_id OR m.org_id IS NULL)
	), eligible_examples AS (
		SELECT DISTINCT ON (p.prompt_text, p.accepted_answer, p.rejected_answer)
			p.preference_example_id::text,
			p.feedback_id::text,
			p.request_id::text,
			p.user_id::text,
			p.org_id::text,
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
		JOIN model_scope s ON TRUE
		WHERE p.accepted_answer <> ''
		  AND p.rejected_answer <> ''
		  AND p.feedback_label = 'REJECTED'
		  AND p.rating < 0
		  AND p.org_id = @org_id
		  AND p.model_id = @model_id
		  AND (@dataset_id::uuid IS NULL OR p.dataset_id = @dataset_id)
		  AND (COALESCE(array_length(@dataset_ids::uuid[], 1), 0) = 0 OR p.dataset_id = ANY(@dataset_ids::uuid[]))
		  AND NOT EXISTS (
		  	SELECT 1
		  	FROM ` + r.Name + `.lineage_eval_examples le
		  	WHERE le.preference_example_id = p.preference_example_id
		  )
			ORDER BY p.prompt_text, p.accepted_answer, p.rejected_answer, p.created_at DESC, p.preference_example_id DESC
		), ranked_examples AS (
			SELECT
				preference_example_id, feedback_id, request_id, user_id, org_id, dataset_id, model_id,
				split, prompt_text, accepted_answer, rejected_answer, rating, feedback_label, created_at,
				ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at DESC, preference_example_id DESC) AS user_rank
			FROM eligible_examples
		), limited_examples AS (
				SELECT
					preference_example_id, feedback_id, request_id, user_id, org_id, dataset_id, model_id,
					split, prompt_text, accepted_answer, rejected_answer, rating, feedback_label, created_at
			FROM ranked_examples
			WHERE (@max_per_user = 0 OR user_rank <= @max_per_user)
			ORDER BY created_at DESC, preference_example_id DESC
			LIMIT @limit
		)
	SELECT
		COALESCE(@dataset_id::uuid::text, (SELECT e.dataset_id FROM limited_examples e LIMIT 1), ''),
		@user_id::uuid::text,
		@org_id::uuid::text,
		@model_id::uuid::text,
		s.model_kind::text,
		s.artifact_location,
		s.artifact_checksum,
		s.adapter_uri,
		s.base_model,
		s.name,
		s.lineage_name,
		s.model_version,
		COALESCE((
			SELECT jsonb_agg(jsonb_build_object(
					'preference_example_id', e.preference_example_id,
					'feedback_id', e.feedback_id,
					'request_id', e.request_id,
					'user_id', e.user_id,
					'org_id', e.org_id,
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
	FROM model_scope s`
	row := r.Pool.QueryRow(ctx, query, preferenceDatasetArgs(request))
	parentArtifactURI := ""
	parentArtifactChecksum := ""
	parentAdapterURI := ""
	parentBaseModel := ""
	parentModelName := ""
	parentLineageName := ""
	parentModelVersion := 0
	orgID := ""
	if err := row.Scan(&datasetID, &userID, &orgID, &modelID, &parentModelKindValue, &parentArtifactURI, &parentArtifactChecksum, &parentAdapterURI, &parentBaseModel, &parentModelName, &parentLineageName, &parentModelVersion, &raw); err != nil {
		if err == pgx.ErrNoRows {
			return nil, domain.ErrValidationFailed.Extend("preference dataset request does not match an inference model for the org")
		}
		return nil, fmt.Errorf("read preference dataset: %w", err)
	}
	parentModelKind := model.ToModelKind(parentModelKindValue)
	if !model.IsKnownModelKind(parentModelKind) {
		return nil, domain.ErrValidationFailed.Extend("preference dataset parent model kind is required")
	}
	if strings.TrimSpace(parentArtifactURI) == "" {
		return nil, domain.ErrValidationFailed.Extend("preference dataset parent artifact uri is required")
	}
	if parentModelKind == model.ModelKindFineTuned && strings.TrimSpace(parentAdapterURI) == "" {
		return nil, domain.ErrValidationFailed.Extend("fine-tuned preference dataset parent adapter uri is required")
	}
	examples, err := decodePreferenceExamples(raw)
	if err != nil {
		return nil, err
	}
	examples = ensurePreferenceTrainEvalSplit(examples)
	primaryDatasetID := optionalParseUUID(datasetID)
	datasetIDs := request.DatasetIDs
	if len(datasetIDs) == 0 {
		datasetIDs = preferenceDatasetExampleDatasetIDs(examples)
	}
	if primaryDatasetID == uuid.Nil && len(datasetIDs) > 0 {
		primaryDatasetID = datasetIDs[0]
	}
	return &model.PreferenceDataset{
		EndpointID:             request.EndpointID,
		UserID:                 uuid.MustParse(userID),
		OrgID:                  uuid.MustParse(orgID),
		DatasetID:              primaryDatasetID,
		DatasetIDs:             datasetIDs,
		ModelID:                uuid.MustParse(modelID),
		ParentModelKind:        parentModelKind,
		ParentArtifactURI:      parentArtifactURI,
		ParentArtifactChecksum: parentArtifactChecksum,
		ParentAdapterURI:       parentAdapterURI,
		ParentBaseModel:        parentBaseModel,
		ParentModelName:        parentModelName,
		ParentLineageName:      parentLineageName,
		ParentModelVersion:     parentModelVersion,
		OutputURI:              request.OutputURI,
		Format:                 "DPO_JSONL",
		EligibilityPolicy:      "complete_rejected_pairs_by_source_model_v1",
		MinExamples:            request.MinExamples,
		Limit:                  request.Limit,
		Examples:               examples,
	}, nil
}

func (r *InferenceFeedbackRepository) RecordPreferenceDatasetSnapshot(ctx context.Context, tx pgx.Tx, dataset *model.PreferenceDataset, request model.PreferenceDatasetBuildRequest) (*model.PreferenceDataset, error) {
	log.Trace("InferenceFeedbackRepository RecordPreferenceDatasetSnapshot")

	query := `INSERT INTO ` + r.Name + `.preference_dataset_snapshots (
					preference_dataset_id, user_id, org_id, endpoint_id, dataset_id, dataset_ids, model_id,
					parent_model_kind, parent_artifact_uri, parent_artifact_checksum, parent_adapter_uri,
					parent_base_model, parent_model_name, parent_lineage_name, parent_model_version,
					source_request_id, output_uri, evaluation_output_uri, format, eligibility_policy,
					example_count, training_example_count, evaluation_example_count, min_examples, limit_count,
					integrity_key
				) VALUES (
					@preference_dataset_id, @user_id, @org_id, @endpoint_id, @dataset_id, @dataset_ids, @model_id,
					@parent_model_kind, @parent_artifact_uri, @parent_artifact_checksum, @parent_adapter_uri,
					@parent_base_model, @parent_model_name, @parent_lineage_name, @parent_model_version,
					@source_request_id, @output_uri, @evaluation_output_uri, @format, @eligibility_policy,
					@example_count, @training_example_count, @evaluation_example_count, @min_examples, @limit_count,
					@integrity_key
		)
		ON CONFLICT (preference_dataset_id) DO NOTHING`
	if _, err := tx.Exec(ctx, query, preferenceDatasetSnapshotArgs(dataset, request)); err != nil {
		if coreDB.IsForeignKeyViolation(err) {
			return nil, domain.ErrValidationFailed.Extend("tenant projection is not ready")
		}
		return nil, fmt.Errorf("record preference dataset snapshot: %w", err)
	}
	return dataset, nil
}

func (r *InferenceFeedbackRepository) ReadPreferenceDatasetSnapshot(ctx context.Context, orgID uuid.UUID, preferenceDatasetID uuid.UUID) (*model.PreferenceDataset, error) {
	log.Trace("InferenceFeedbackRepository ReadPreferenceDatasetSnapshot")

	query := `SELECT ` + preferenceDatasetSnapshotColumns() + `
		FROM ` + r.Name + `.preference_dataset_snapshots
		WHERE org_id = @org_id AND preference_dataset_id = @preference_dataset_id`
	record, err := scanPreferenceDatasetSnapshot(r.Pool.QueryRow(ctx, query, pgx.NamedArgs{
		"org_id":                nullableUUID(orgID),
		"preference_dataset_id": nullableUUID(preferenceDatasetID),
	}))
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, domain.ErrValidationFailed.Extend("preference dataset not found")
		}
		return nil, fmt.Errorf("read preference dataset snapshot: %w", err)
	}
	return record, nil
}

func (r *InferenceFeedbackRepository) ListPreferenceDatasetSnapshots(ctx context.Context, orgID uuid.UUID, filter model.PreferenceDatasetFilter) ([]*model.PreferenceDataset, error) {
	log.Trace("InferenceFeedbackRepository ListPreferenceDatasetSnapshots")

	query := `SELECT ` + preferenceDatasetSnapshotColumns() + `
		FROM ` + r.Name + `.preference_dataset_snapshots
		WHERE org_id = @org_id
		  AND (@model_id::uuid IS NULL OR model_id = @model_id)
		  AND (@endpoint_id::uuid IS NULL OR endpoint_id = @endpoint_id)
		ORDER BY created_at DESC, preference_dataset_id`
	rows, err := r.Pool.Query(ctx, query, pgx.NamedArgs{
		"org_id":      nullableUUID(orgID),
		"model_id":    nullableUUID(filter.ModelID),
		"endpoint_id": nullableUUID(filter.EndpointID),
	})
	if err != nil {
		return nil, fmt.Errorf("list preference dataset snapshots: %w", err)
	}
	defer rows.Close()
	records := []*model.PreferenceDataset{}
	for rows.Next() {
		record, err := scanPreferenceDatasetSnapshot(rows)
		if err != nil {
			return nil, fmt.Errorf("scan preference dataset snapshot: %w", err)
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate preference dataset snapshots: %w", err)
	}
	return records, nil
}

func feedbackArgs(feedback *model.InferenceFeedback, idempotencyKey uuid.UUID) pgx.NamedArgs {
	log.Trace("feedbackArgs")

	return pgx.NamedArgs{
		"feedback_id":      nullableUUID(feedback.FeedbackID),
		"idempotency_key":  nullableUUID(idempotencyKey),
		"request_id":       nullableUUID(feedback.RequestID),
		"user_id":          nullableUUID(feedback.UserID),
		"org_id":           nullableUUID(feedback.OrgID),
		"accepted":         feedback.Accepted,
		"rating":           feedback.Rating,
		"comment":          feedback.Comment,
		"preferred_answer": feedback.PreferredAnswer,
	}
}

func preferenceDatasetArgs(request model.PreferenceDatasetBuildRequest) pgx.NamedArgs {
	log.Trace("preferenceDatasetArgs")

	limit := request.Limit
	if limit <= 0 {
		limit = 1000
	}
	return pgx.NamedArgs{
		"user_id":      nullableUUID(request.UserID),
		"org_id":       nullableUUID(request.OrgID),
		"dataset_id":   nullableUUID(request.DatasetID),
		"dataset_ids":  request.DatasetIDs,
		"model_id":     nullableUUID(request.ModelID),
		"limit":        limit,
		"max_per_user": request.MaxPerUser,
	}
}

func preferenceDatasetSnapshotArgs(dataset *model.PreferenceDataset, request model.PreferenceDatasetBuildRequest) pgx.NamedArgs {
	log.Trace("preferenceDatasetSnapshotArgs")

	return pgx.NamedArgs{
		"preference_dataset_id":    nullableUUID(dataset.PreferenceDatasetID),
		"user_id":                  nullableUUID(dataset.UserID),
		"org_id":                   nullableUUID(dataset.OrgID),
		"endpoint_id":              nullableUUID(dataset.EndpointID),
		"dataset_id":               nullableUUID(dataset.DatasetID),
		"dataset_ids":              dataset.DatasetIDs,
		"model_id":                 nullableUUID(dataset.ModelID),
		"parent_model_kind":        dataset.ParentModelKind.String(),
		"parent_artifact_uri":      strings.TrimSpace(dataset.ParentArtifactURI),
		"parent_artifact_checksum": strings.TrimSpace(dataset.ParentArtifactChecksum),
		"parent_adapter_uri":       strings.TrimSpace(dataset.ParentAdapterURI),
		"parent_base_model":        strings.TrimSpace(dataset.ParentBaseModel),
		"parent_model_name":        strings.TrimSpace(dataset.ParentModelName),
		"parent_lineage_name":      strings.TrimSpace(dataset.ParentLineageName),
		"parent_model_version":     dataset.ParentModelVersion,
		"source_request_id":        nullableUUID(dataset.RequestID),
		"output_uri":               strings.TrimSpace(dataset.OutputURI),
		"evaluation_output_uri":    strings.TrimSpace(dataset.EvaluationOutputURI),
		"format":                   strings.TrimSpace(dataset.Format),
		"eligibility_policy":       strings.TrimSpace(dataset.EligibilityPolicy),
		"example_count":            dataset.ExampleCount(),
		"training_example_count":   dataset.TrainingExampleCount(),
		"evaluation_example_count": dataset.EvaluationExampleCount(),
		"min_examples":             request.MinExamples,
		"limit_count":              request.Limit,
		"integrity_key":            strings.TrimSpace(dataset.IntegrityKey),
	}
}

func ensurePreferenceTrainEvalSplit(examples []model.PreferenceExample) []model.PreferenceExample {
	log.Trace("ensurePreferenceTrainEvalSplit")

	if len(examples) == 0 {
		return examples
	}
	out := make([]model.PreferenceExample, len(examples))
	copy(out, examples)
	trainCount := 0
	evalCount := 0
	for _, example := range examples {
		if example.Split == "" || example.Split == "TRAIN" {
			trainCount++
		}
		if example.Split == "EVAL" {
			evalCount++
		}
	}
	if trainCount == 0 {
		if out[0].Split == "EVAL" {
			evalCount--
		}
		out[0].Split = "TRAIN"
		trainCount = 1
	}
	if evalCount == 0 && len(out) > 1 {
		out[len(out)-1].Split = "EVAL"
	}
	return out
}

func preferenceDatasetSnapshotColumns() string {
	log.Trace("preferenceDatasetSnapshotColumns")

	return `preference_dataset_id::text, COALESCE(endpoint_id::text, ''), user_id::text, org_id::text,
		COALESCE(dataset_id::text, ''), COALESCE(array_to_json(dataset_ids)::text, '[]'), model_id::text,
		parent_model_kind::text, parent_artifact_uri, parent_artifact_checksum, parent_adapter_uri,
		parent_base_model, parent_model_name, parent_lineage_name, parent_model_version,
		output_uri, evaluation_output_uri, format, eligibility_policy, example_count,
		training_example_count, evaluation_example_count, min_examples, limit_count,
		integrity_key, created_at`
}

func scanPreferenceDatasetSnapshot(row pgx.Row) (*model.PreferenceDataset, error) {
	log.Trace("scanPreferenceDatasetSnapshot")

	var preferenceDatasetID, endpointID, userID, orgID, datasetID, datasetIDs, modelID string
	var parentModelKind string
	var createdAt time.Time
	record := &model.PreferenceDataset{}
	if err := row.Scan(
		&preferenceDatasetID,
		&endpointID,
		&userID,
		&orgID,
		&datasetID,
		&datasetIDs,
		&modelID,
		&parentModelKind,
		&record.ParentArtifactURI,
		&record.ParentArtifactChecksum,
		&record.ParentAdapterURI,
		&record.ParentBaseModel,
		&record.ParentModelName,
		&record.ParentLineageName,
		&record.ParentModelVersion,
		&record.OutputURI,
		&record.EvaluationOutputURI,
		&record.Format,
		&record.EligibilityPolicy,
		&record.ExampleTotal,
		&record.TrainingCount,
		&record.EvaluationCount,
		&record.MinExamples,
		&record.Limit,
		&record.IntegrityKey,
		&createdAt,
	); err != nil {
		return nil, err
	}
	record.PreferenceDatasetID = uuid.MustParse(preferenceDatasetID)
	record.EndpointID = optionalParseUUID(endpointID)
	record.UserID = uuid.MustParse(userID)
	record.OrgID = uuid.MustParse(orgID)
	record.DatasetID = optionalParseUUID(datasetID)
	record.DatasetIDs = decodeUUIDArray(datasetIDs)
	record.ModelID = uuid.MustParse(modelID)
	record.ParentModelKind = model.ToModelKind(parentModelKind)
	record.CreatedAt = createdAt
	return record, nil
}

func optionalParseUUID(value string) uuid.UUID {
	log.Trace("optionalParseUUID")

	value = strings.TrimSpace(value)
	if value == "" {
		return uuid.Nil
	}
	return uuid.MustParse(value)
}

func decodeUUIDArray(raw string) []uuid.UUID {
	log.Trace("decodeUUIDArray")

	values := []string{}
	if err := json.Unmarshal([]byte(raw), &values); err != nil {
		return nil
	}
	out := make([]uuid.UUID, 0, len(values))
	for _, value := range values {
		id := optionalParseUUID(value)
		if id != uuid.Nil {
			out = append(out, id)
		}
	}
	return out
}

func preferenceDatasetExampleDatasetIDs(examples []model.PreferenceExample) []uuid.UUID {
	log.Trace("preferenceDatasetExampleDatasetIDs")

	seen := map[uuid.UUID]bool{}
	out := make([]uuid.UUID, 0)
	for _, example := range examples {
		if example.DatasetID == uuid.Nil || seen[example.DatasetID] {
			continue
		}
		seen[example.DatasetID] = true
		out = append(out, example.DatasetID)
	}
	return out
}

func scanInferenceFeedback(row pgx.Row) (*model.InferenceFeedback, error) {
	log.Trace("scanInferenceFeedback")

	var feedbackID, requestID, userID, orgID string
	record := &model.InferenceFeedback{}
	if err := row.Scan(
		&feedbackID,
		&requestID,
		&userID,
		&orgID,
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
	record.OrgID = uuid.MustParse(orgID)
	return record, nil
}

func decodePreferenceExamples(raw string) ([]model.PreferenceExample, error) {
	log.Trace("decodePreferenceExamples")

	var rows []struct {
		PreferenceExampleID string `json:"preference_example_id"`
		FeedbackID          string `json:"feedback_id"`
		RequestID           string `json:"request_id"`
		UserID              string `json:"user_id"`
		OrgID               string `json:"org_id"`
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
			OrgID:               uuid.MustParse(row.OrgID),
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
