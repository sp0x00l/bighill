package db_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"inference_service/pkg/domain"
	"inference_service/pkg/domain/model"
	inferencedb "inference_service/pkg/infra/repo/db"
	coreDB "lib/shared_lib/db"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("InferenceFeedbackRepository", func() {
	var (
		ctx            context.Context
		pool           *connectionPoolStub
		repository     *inferencedb.InferenceFeedbackRepository
		feedbackID     uuid.UUID
		requestID      uuid.UUID
		userID         uuid.UUID
		orgID          uuid.UUID
		idempotencyKey uuid.UUID
		feedback       *model.InferenceFeedback
		tx             pgx.Tx
	)

	BeforeEach(func() {
		ctx = context.Background()
		pool = &connectionPoolStub{}
		tx = &inferenceTxStub{pool: pool}
		repository = inferencedb.NewInferenceFeedbackRepository(coreDB.NewDatabase(pool, "test_db"))
		feedbackID = uuid.New()
		requestID = uuid.New()
		userID = uuid.New()
		orgID = uuid.New()
		idempotencyKey = uuid.New()
		feedback = &model.InferenceFeedback{
			FeedbackID:      feedbackID,
			RequestID:       requestID,
			UserID:          userID,
			OrgID:           orgID,
			Accepted:        false,
			Rating:          -1,
			Comment:         "wrong answer",
			PreferredAnswer: "corrected answer",
		}
	})

	It("records feedback and derives a preference example in one transaction", func() {
		pool.nextRows = []pgx.Row{feedbackRow(feedback)}

		record, err := repository.RecordFeedback(ctx, tx, feedback, idempotencyKey)

		Expect(err).NotTo(HaveOccurred())
		Expect(record).To(Equal(feedback))
		Expect(pool.lastQuery).To(ContainSubstring("INSERT INTO test_db.inference_feedback"))
		Expect(pool.lastQuery).To(ContainSubstring("preferred_answer"))
		Expect(pool.lastQuery).To(ContainSubstring("JOIN test_db.inference_requests"))
		Expect(pool.lastQuery).To(ContainSubstring("WHERE req.model_id IS NOT NULL"))
		Expect(pool.lastQuery).To(ContainSubstring("CASE WHEN f.accepted THEN req.answer_text ELSE f.preferred_answer END"))
		Expect(pool.lastQuery).To(ContainSubstring("INSERT INTO test_db.preference_examples"))
		args := namedArgs(pool.lastArgs)
		Expect(args).To(SatisfyAll(
			HaveKeyWithValue("feedback_id", pgtype.UUID{Bytes: feedbackID, Valid: true}),
			HaveKeyWithValue("idempotency_key", pgtype.UUID{Bytes: idempotencyKey, Valid: true}),
			HaveKeyWithValue("request_id", pgtype.UUID{Bytes: requestID, Valid: true}),
			HaveKeyWithValue("user_id", pgtype.UUID{Bytes: userID, Valid: true}),
			HaveKeyWithValue("org_id", pgtype.UUID{Bytes: orgID, Valid: true}),
			HaveKeyWithValue("accepted", false),
			HaveKeyWithValue("rating", -1),
			HaveKeyWithValue("comment", "wrong answer"),
			HaveKeyWithValue("preferred_answer", "corrected answer"),
		))
		Expect(args).NotTo(HaveKey("preference_example_id"))
	})

	It("wraps database failures", func() {
		pool.nextRows = []pgx.Row{&repositoryRow{err: errors.New("insert failed")}}

		record, err := repository.RecordFeedback(ctx, tx, feedback, idempotencyKey)

		Expect(record).To(BeNil())
		Expect(err).To(MatchError(ContainSubstring("record inference feedback: insert failed")))
	})

	It("maps missing tenant/request projections to a domain validation error", func() {
		pool.nextRows = []pgx.Row{&repositoryRow{err: &pgconn.PgError{Code: "23503"}}}

		record, err := repository.RecordFeedback(ctx, tx, feedback, idempotencyKey)

		Expect(record).To(BeNil())
		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
	})

	It("reads complete preference pairs for the model across org contributors", func() {
		datasetID := uuid.New()
		modelID := uuid.New()
		exampleID := uuid.New()
		feedbackID := uuid.New()
		rawExamples := fmt.Sprintf(`[{
			"preference_example_id": %q,
			"feedback_id": %q,
			"request_id": %q,
			"user_id": %q,
			"org_id": %q,
			"dataset_id": %q,
			"model_id": %q,
			"split": "EVAL",
			"prompt_text": "Prompt",
			"accepted_answer": "Correct answer",
			"rejected_answer": "Wrong answer",
			"rating": -1,
			"feedback_label": "REJECTED"
		}]`, exampleID.String(), feedbackID.String(), requestID.String(), userID.String(), orgID.String(), datasetID.String(), modelID.String())
		pool.nextRows = []pgx.Row{&repositoryRow{values: []any{datasetID.String(), userID.String(), orgID.String(), modelID.String(), "FINE_TUNED", "s3://models/parent-artifact", "sha256:parent", "s3://models/parent", "mistral-7b", "dpo-generation", "fraud-rag-ranker", 7, rawExamples}}}

		dataset, err := repository.ReadPreferenceDataset(ctx, model.PreferenceDatasetBuildRequest{
			UserID:    userID,
			OrgID:     orgID,
			OutputURI: "s3://local-dev-bucket/preferences/{dataset_id}/preference_dataset.jsonl",
			Limit:     100,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(dataset.UserID).To(Equal(userID))
		Expect(dataset.OrgID).To(Equal(orgID))
		Expect(dataset.DatasetID).To(Equal(datasetID))
		Expect(dataset.ModelID).To(Equal(modelID))
		Expect(dataset.ParentModelKind).To(Equal(model.ModelKindFineTuned))
		Expect(dataset.ParentArtifactURI).To(Equal("s3://models/parent-artifact"))
		Expect(dataset.ParentArtifactChecksum).To(Equal("sha256:parent"))
		Expect(dataset.ParentAdapterURI).To(Equal("s3://models/parent"))
		Expect(dataset.ParentBaseModel).To(Equal("mistral-7b"))
		Expect(dataset.ParentModelName).To(Equal("dpo-generation"))
		Expect(dataset.ParentLineageName).To(Equal("fraud-rag-ranker"))
		Expect(dataset.ParentModelVersion).To(Equal(7))
		Expect(dataset.Examples).To(HaveLen(1))
		Expect(dataset.Examples[0].Split).To(Equal("TRAIN"))
		Expect(dataset.Examples[0].AcceptedAnswer).To(Equal("Correct answer"))
		Expect(dataset.Examples[0].RejectedAnswer).To(Equal("Wrong answer"))
		Expect(pool.lastQuery).To(ContainSubstring("p.accepted_answer <> ''"))
		Expect(pool.lastQuery).To(ContainSubstring("p.rejected_answer <> ''"))
		Expect(pool.lastQuery).To(ContainSubstring("p.feedback_label = 'REJECTED'"))
		Expect(pool.lastQuery).To(ContainSubstring("p.rating < 0"))
		Expect(pool.lastQuery).NotTo(ContainSubstring("p.user_id = @user_id"))
		Expect(pool.lastQuery).To(ContainSubstring("FROM model_scope s"))
		Expect(pool.lastQuery).To(ContainSubstring("CASE WHEN substr(md5(p.preference_example_id::text), 1, 1)"))
		Expect(pool.lastQuery).To(ContainSubstring("ranked_examples AS"))
		Expect(pool.lastQuery).To(ContainSubstring("ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at DESC, preference_example_id DESC) AS user_rank"))
		Expect(pool.lastQuery).To(ContainSubstring("FROM test_db.lineage_eval_examples le"))
		Expect(pool.lastQuery).To(ContainSubstring("le.preference_example_id = p.preference_example_id"))
		Expect(pool.lastQuery).To(ContainSubstring("WHERE (@max_per_user = 0 OR user_rank <= @max_per_user)"))
		Expect(pool.lastQuery).To(ContainSubstring("DISTINCT ON"))
		Expect(pool.lastQuery).To(ContainSubstring("preference_example_id, feedback_id, request_id, user_id, org_id, dataset_id, model_id"))
		Expect(pool.lastQuery).To(ContainSubstring("p.preference_example_id DESC"))
		Expect(pool.lastQuery).NotTo(ContainSubstring("m.adapter_uri <> ''"))
		args := namedArgs(pool.lastArgs)
		Expect(args).To(SatisfyAll(
			HaveKeyWithValue("user_id", pgtype.UUID{Bytes: userID, Valid: true}),
			HaveKeyWithValue("org_id", pgtype.UUID{Bytes: orgID, Valid: true}),
			HaveKeyWithValue("limit", 100),
			HaveKeyWithValue("max_per_user", 0),
		))
	})

	It("keeps a held-out eval example when multiple eligible examples are all train-biased", func() {
		datasetID := uuid.New()
		modelID := uuid.New()
		firstExampleID := uuid.New()
		secondExampleID := uuid.New()
		firstFeedbackID := uuid.New()
		secondFeedbackID := uuid.New()
		rawExamples := fmt.Sprintf(`[{
			"preference_example_id": %q,
			"feedback_id": %q,
			"request_id": %q,
			"user_id": %q,
			"org_id": %q,
			"dataset_id": %q,
			"model_id": %q,
			"split": "TRAIN",
			"prompt_text": "Prompt A",
			"accepted_answer": "Correct answer A",
			"rejected_answer": "Wrong answer A",
			"rating": -1,
			"feedback_label": "REJECTED"
		}, {
			"preference_example_id": %q,
			"feedback_id": %q,
			"request_id": %q,
			"user_id": %q,
			"org_id": %q,
			"dataset_id": %q,
			"model_id": %q,
			"split": "TRAIN",
			"prompt_text": "Prompt B",
			"accepted_answer": "Correct answer B",
			"rejected_answer": "Wrong answer B",
			"rating": -1,
			"feedback_label": "REJECTED"
		}]`, firstExampleID.String(), firstFeedbackID.String(), requestID.String(), userID.String(), orgID.String(), datasetID.String(), modelID.String(), secondExampleID.String(), secondFeedbackID.String(), requestID.String(), userID.String(), orgID.String(), datasetID.String(), modelID.String())
		pool.nextRows = []pgx.Row{&repositoryRow{values: []any{datasetID.String(), userID.String(), orgID.String(), modelID.String(), "BASE", "s3://models/base-artifact", "sha256:base", "", "local-test-model:latest", "shared-base", "shared-base", 1, rawExamples}}}

		dataset, err := repository.ReadPreferenceDataset(ctx, model.PreferenceDatasetBuildRequest{
			UserID:    userID,
			OrgID:     orgID,
			DatasetID: datasetID,
			ModelID:   modelID,
			OutputURI: "s3://local-dev-bucket/preferences/{dataset_id}/preference_dataset.jsonl",
			Limit:     100,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(dataset.Examples).To(HaveLen(2))
		Expect(dataset.TrainingExampleCount()).To(Equal(1))
		Expect(dataset.EvaluationExampleCount()).To(Equal(1))
	})

	It("passes the per-user contribution cap into preference dataset selection", func() {
		datasetID := uuid.New()
		modelID := uuid.New()
		pool.nextRows = []pgx.Row{&repositoryRow{values: []any{datasetID.String(), userID.String(), orgID.String(), modelID.String(), "BASE", "s3://models/base-artifact", "sha256:base", "", "local-test-model:latest", "shared-base", "shared-base", 1, "[]"}}}

		_, err := repository.ReadPreferenceDataset(ctx, model.PreferenceDatasetBuildRequest{
			UserID:     userID,
			OrgID:      orgID,
			OutputURI:  "s3://local-dev-bucket/preferences/{dataset_id}/preference_dataset.jsonl",
			Limit:      100,
			MaxPerUser: 1,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(namedArgs(pool.lastArgs)).To(HaveKeyWithValue("max_per_user", 1))
	})

	It("allows preference exports from a base model without a parent adapter", func() {
		datasetID := uuid.New()
		modelID := uuid.New()
		exampleID := uuid.New()
		feedbackID := uuid.New()
		rawExamples := fmt.Sprintf(`[{
			"preference_example_id": %q,
			"feedback_id": %q,
			"request_id": %q,
			"user_id": %q,
			"org_id": %q,
			"dataset_id": %q,
			"model_id": %q,
			"split": "TRAIN",
			"prompt_text": "Prompt",
			"accepted_answer": "Correct answer",
			"rejected_answer": "Wrong answer",
			"rating": -1,
			"feedback_label": "REJECTED"
		}]`, exampleID.String(), feedbackID.String(), requestID.String(), userID.String(), orgID.String(), datasetID.String(), modelID.String())
		pool.nextRows = []pgx.Row{&repositoryRow{values: []any{datasetID.String(), userID.String(), orgID.String(), modelID.String(), "BASE", "s3://models/base-artifact", "sha256:base", "", "local-test-model:latest", "shared-base", "shared-base", 1, rawExamples}}}

		dataset, err := repository.ReadPreferenceDataset(ctx, model.PreferenceDatasetBuildRequest{
			UserID:    userID,
			OrgID:     orgID,
			DatasetID: datasetID,
			ModelID:   modelID,
			OutputURI: "s3://local-dev-bucket/preferences/{dataset_id}/preference_dataset.jsonl",
			Limit:     100,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(dataset.ParentModelKind).To(Equal(model.ModelKindBase))
		Expect(dataset.ParentArtifactURI).To(Equal("s3://models/base-artifact"))
		Expect(dataset.ParentArtifactChecksum).To(Equal("sha256:base"))
		Expect(dataset.ParentAdapterURI).To(Equal(""))
		Expect(dataset.ParentBaseModel).To(Equal("local-test-model:latest"))
		Expect(dataset.ParentModelName).To(Equal("shared-base"))
		Expect(dataset.ParentLineageName).To(Equal("shared-base"))
		Expect(dataset.ParentModelVersion).To(Equal(1))
		Expect(dataset.Examples).To(HaveLen(1))
	})

	It("rejects a fine-tuned preference parent without an adapter", func() {
		datasetID := uuid.New()
		modelID := uuid.New()
		pool.nextRows = []pgx.Row{&repositoryRow{values: []any{datasetID.String(), userID.String(), orgID.String(), modelID.String(), "FINE_TUNED", "s3://models/parent-artifact", "sha256:parent", "", "mistral-7b", "dpo-generation", "fraud-rag-ranker", 7, "[]"}}}

		dataset, err := repository.ReadPreferenceDataset(ctx, model.PreferenceDatasetBuildRequest{
			UserID:    userID,
			OrgID:     orgID,
			DatasetID: datasetID,
			ModelID:   modelID,
			OutputURI: "s3://local-dev-bucket/preferences/{dataset_id}/preference_dataset.jsonl",
			Limit:     100,
		})

		Expect(dataset).To(BeNil())
		Expect(err).To(MatchError(ContainSubstring("fine-tuned preference dataset parent adapter uri is required")))
	})

	It("records preference dataset snapshots in the supplied transaction", func() {
		preferenceDatasetID := uuid.New()
		datasetID := uuid.New()
		modelID := uuid.New()
		dataset := &model.PreferenceDataset{
			PreferenceDatasetID: preferenceDatasetID,
			RequestID:           requestID,
			UserID:              userID,
			OrgID:               orgID,
			DatasetID:           datasetID,
			ModelID:             modelID,
			ParentAdapterURI:    "s3://models/parent",
			ParentBaseModel:     "mistral-7b",
			ParentModelVersion:  7,
			OutputURI:           "s3://local-dev-bucket/preferences/dpo.jsonl",
			EvaluationOutputURI: "s3://local-dev-bucket/preferences/dpo-eval.jsonl",
			Format:              "DPO_JSONL",
			EligibilityPolicy:   "complete_rejected_pairs_train_eval_split_v1",
			Examples: []model.PreferenceExample{{
				PreferenceExampleID: uuid.New(),
				OrgID:               orgID,
				DatasetID:           datasetID,
				ModelID:             modelID,
				Split:               "TRAIN",
			}},
		}

		record, err := repository.RecordPreferenceDatasetSnapshot(ctx, tx, dataset, model.PreferenceDatasetBuildRequest{
			UserID:      userID,
			OrgID:       orgID,
			MinExamples: 10,
			Limit:       100,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(record).To(Equal(dataset))
		Expect(pool.queries[0]).To(ContainSubstring("INSERT INTO test_db.preference_dataset_snapshots"))
	})

	It("maps preference snapshot reference failures to a domain validation error", func() {
		dataset := validPreferenceDatasetSnapshot(userID, orgID)
		pool.nextExecErr = &pgconn.PgError{Code: "23503"}

		record, err := repository.RecordPreferenceDatasetSnapshot(ctx, tx, dataset, model.PreferenceDatasetBuildRequest{
			UserID: userID,
			OrgID:  orgID,
		})

		Expect(record).To(BeNil())
		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
	})

	It("reads a persisted preference dataset snapshot by org and id", func() {
		dataset := validPreferenceDatasetSnapshot(userID, orgID)
		pool.nextRows = []pgx.Row{preferenceDatasetSnapshotRow(dataset)}

		record, err := repository.ReadPreferenceDatasetSnapshot(ctx, orgID, dataset.PreferenceDatasetID)

		Expect(err).NotTo(HaveOccurred())
		Expect(record).To(Equal(dataset))
		Expect(pool.lastQuery).To(ContainSubstring("FROM test_db.preference_dataset_snapshots"))
		Expect(pool.lastQuery).To(ContainSubstring("WHERE org_id = @org_id AND preference_dataset_id = @preference_dataset_id"))
		Expect(namedArgs(pool.lastArgs)).To(SatisfyAll(
			HaveKeyWithValue("org_id", pgtype.UUID{Bytes: orgID, Valid: true}),
			HaveKeyWithValue("preference_dataset_id", pgtype.UUID{Bytes: dataset.PreferenceDatasetID, Valid: true}),
		))
	})

	It("maps missing preference dataset snapshots to a validation error", func() {
		record, err := repository.ReadPreferenceDatasetSnapshot(ctx, orgID, uuid.New())

		Expect(record).To(BeNil())
		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
	})

	It("lists persisted preference dataset snapshots with model and endpoint filters", func() {
		dataset := validPreferenceDatasetSnapshot(userID, orgID)
		pool.nextQueryRows = []pgx.Rows{&repositoryRows{rows: [][]any{preferenceDatasetSnapshotValues(dataset)}}}

		records, err := repository.ListPreferenceDatasetSnapshots(ctx, orgID, model.PreferenceDatasetFilter{
			ModelID:    dataset.ModelID,
			EndpointID: dataset.EndpointID,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(records).To(Equal([]*model.PreferenceDataset{dataset}))
		Expect(pool.lastQuery).To(ContainSubstring("ORDER BY created_at DESC, preference_dataset_id"))
		Expect(namedArgs(pool.lastArgs)).To(SatisfyAll(
			HaveKeyWithValue("org_id", pgtype.UUID{Bytes: orgID, Valid: true}),
			HaveKeyWithValue("model_id", pgtype.UUID{Bytes: dataset.ModelID, Valid: true}),
			HaveKeyWithValue("endpoint_id", pgtype.UUID{Bytes: dataset.EndpointID, Valid: true}),
		))
	})

	It("surfaces preference snapshot iterator errors", func() {
		pool.nextQueryRows = []pgx.Rows{&repositoryRows{err: errors.New("cursor failed")}}

		records, err := repository.ListPreferenceDatasetSnapshots(ctx, orgID, model.PreferenceDatasetFilter{})

		Expect(records).To(BeNil())
		Expect(err).To(MatchError(ContainSubstring("iterate preference dataset snapshots")))
	})
})

func feedbackRow(feedback *model.InferenceFeedback) pgx.Row {
	return &repositoryRow{values: []any{
		feedback.FeedbackID.String(),
		feedback.RequestID.String(),
		feedback.UserID.String(),
		feedback.OrgID.String(),
		feedback.Accepted,
		feedback.Rating,
		feedback.Comment,
		feedback.PreferredAnswer,
	}}
}

func validPreferenceDatasetSnapshot(userID, orgID uuid.UUID) *model.PreferenceDataset {
	datasetID := uuid.New()
	return &model.PreferenceDataset{
		PreferenceDatasetID:    uuid.New(),
		EndpointID:             uuid.New(),
		UserID:                 userID,
		OrgID:                  orgID,
		DatasetID:              datasetID,
		DatasetIDs:             []uuid.UUID{datasetID, uuid.New()},
		ModelID:                uuid.New(),
		ParentModelKind:        model.ModelKindFineTuned,
		ParentArtifactURI:      "s3://models/parent-artifact",
		ParentArtifactChecksum: "sha256:parent",
		ParentAdapterURI:       "s3://models/parent",
		ParentBaseModel:        "mistral-7b",
		ParentModelName:        "dpo-generation",
		ParentLineageName:      "fraud-rag-ranker",
		ParentModelVersion:     7,
		OutputURI:              "s3://local-dev-bucket/preferences/dpo.jsonl",
		EvaluationOutputURI:    "s3://local-dev-bucket/preferences/dpo-eval.jsonl",
		Format:                 "DPO_JSONL",
		EligibilityPolicy:      "complete_rejected_pairs_train_eval_split_v1",
		ExampleTotal:           12,
		TrainingCount:          10,
		EvaluationCount:        2,
		MinExamples:            5,
		Limit:                  100,
		IntegrityKey:           "sha256:preferences",
		CreatedAt:              time.Unix(1_700_000_000, 0).UTC(),
	}
}

func preferenceDatasetSnapshotRow(dataset *model.PreferenceDataset) pgx.Row {
	return &repositoryRow{values: preferenceDatasetSnapshotValues(dataset)}
}

func preferenceDatasetSnapshotValues(dataset *model.PreferenceDataset) []any {
	datasetIDs := make([]string, 0, len(dataset.DatasetIDs))
	for _, datasetID := range dataset.DatasetIDs {
		datasetIDs = append(datasetIDs, datasetID.String())
	}
	rawDatasetIDs, err := json.Marshal(datasetIDs)
	Expect(err).NotTo(HaveOccurred())
	return []any{
		dataset.PreferenceDatasetID.String(),
		dataset.EndpointID.String(),
		dataset.UserID.String(),
		dataset.OrgID.String(),
		dataset.DatasetID.String(),
		string(rawDatasetIDs),
		dataset.ModelID.String(),
		dataset.ParentModelKind.String(),
		dataset.ParentArtifactURI,
		dataset.ParentArtifactChecksum,
		dataset.ParentAdapterURI,
		dataset.ParentBaseModel,
		dataset.ParentModelName,
		dataset.ParentLineageName,
		dataset.ParentModelVersion,
		dataset.OutputURI,
		dataset.EvaluationOutputURI,
		dataset.Format,
		dataset.EligibilityPolicy,
		dataset.ExampleTotal,
		dataset.TrainingCount,
		dataset.EvaluationCount,
		dataset.MinExamples,
		dataset.Limit,
		dataset.IntegrityKey,
		dataset.CreatedAt,
	}
}
