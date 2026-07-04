package db_test

import (
	"context"
	"errors"
	"fmt"

	"inference_service/pkg/domain/model"
	inferencedb "inference_service/pkg/infra/repo/db"
	inferencepb "lib/data_contracts_lib/inference"
	coreDB "lib/shared_lib/db"
	msgConn "lib/shared_lib/messaging"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
)

var _ = Describe("InferenceFeedbackRepository", func() {
	var (
		ctx            context.Context
		pool           *connectionPoolStub
		repository     *inferencedb.InferenceFeedbackRepository
		feedbackID     uuid.UUID
		requestID      uuid.UUID
		userID         uuid.UUID
		idempotencyKey uuid.UUID
		feedback       *model.InferenceFeedback
	)

	BeforeEach(func() {
		ctx = context.Background()
		pool = &connectionPoolStub{}
		repository = inferencedb.NewInferenceFeedbackRepository(coreDB.NewDatabase(pool, "test_db"))
		feedbackID = uuid.New()
		requestID = uuid.New()
		userID = uuid.New()
		idempotencyKey = uuid.New()
		feedback = &model.InferenceFeedback{
			FeedbackID:      feedbackID,
			RequestID:       requestID,
			UserID:          userID,
			Accepted:        false,
			Rating:          -1,
			Comment:         "wrong answer",
			PreferredAnswer: "corrected answer",
		}
	})

	It("records feedback and derives a preference example in one transaction", func() {
		pool.nextRows = []pgx.Row{feedbackRow(feedback)}

		record, err := repository.RecordFeedback(ctx, feedback, idempotencyKey)

		Expect(err).NotTo(HaveOccurred())
		Expect(record).To(Equal(feedback))
		Expect(pool.commitCalled).To(BeTrue())
		Expect(pool.rollbackCalled).To(BeFalse())
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
			HaveKeyWithValue("accepted", false),
			HaveKeyWithValue("rating", -1),
			HaveKeyWithValue("comment", "wrong answer"),
			HaveKeyWithValue("preferred_answer", "corrected answer"),
		))
		Expect(args["preference_example_id"]).To(Equal(pgtype.UUID{
			Bytes: uuid.NewSHA1(uuid.NameSpaceURL, []byte("preference:"+idempotencyKey.String())),
			Valid: true,
		}))
	})

	It("wraps database failures", func() {
		pool.nextRows = []pgx.Row{&repositoryRow{err: errors.New("insert failed")}}

		record, err := repository.RecordFeedback(ctx, feedback, idempotencyKey)

		Expect(record).To(BeNil())
		Expect(err).To(MatchError(ContainSubstring("record inference feedback: insert failed")))
	})

	It("reads complete preference pairs for the request dataset and model", func() {
		datasetID := uuid.New()
		modelID := uuid.New()
		exampleID := uuid.New()
		feedbackID := uuid.New()
		rawExamples := fmt.Sprintf(`[{
			"preference_example_id": %q,
			"feedback_id": %q,
			"request_id": %q,
			"user_id": %q,
			"dataset_id": %q,
			"model_id": %q,
			"split": "EVAL",
			"prompt_text": "Prompt",
			"accepted_answer": "Correct answer",
			"rejected_answer": "Wrong answer",
			"rating": -1,
			"feedback_label": "REJECTED"
		}]`, exampleID.String(), feedbackID.String(), requestID.String(), userID.String(), datasetID.String(), modelID.String())
		pool.nextRows = []pgx.Row{&repositoryRow{values: []any{datasetID.String(), userID.String(), modelID.String(), "s3://models/parent", "mistral-7b", 7, rawExamples}}}

		dataset, err := repository.ReadPreferenceDataset(ctx, model.PreferenceDatasetExportRequest{
			RequestID: requestID,
			UserID:    userID,
			OutputURI: "s3://local-dev-bucket/preferences/{dataset_id}/preference_dataset.jsonl",
			Limit:     100,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(dataset.RequestID).To(Equal(requestID))
		Expect(dataset.UserID).To(Equal(userID))
		Expect(dataset.DatasetID).To(Equal(datasetID))
		Expect(dataset.ModelID).To(Equal(modelID))
		Expect(dataset.ParentAdapterURI).To(Equal("s3://models/parent"))
		Expect(dataset.ParentBaseModel).To(Equal("mistral-7b"))
		Expect(dataset.ParentModelVersion).To(Equal(7))
		Expect(dataset.Examples).To(HaveLen(1))
		Expect(dataset.Examples[0].Split).To(Equal("TRAIN"))
		Expect(dataset.Examples[0].AcceptedAnswer).To(Equal("Correct answer"))
		Expect(dataset.Examples[0].RejectedAnswer).To(Equal("Wrong answer"))
		Expect(pool.lastQuery).To(ContainSubstring("p.accepted_answer <> ''"))
		Expect(pool.lastQuery).To(ContainSubstring("p.rejected_answer <> ''"))
		Expect(pool.lastQuery).To(ContainSubstring("p.feedback_label = 'REJECTED'"))
		Expect(pool.lastQuery).To(ContainSubstring("p.rating < 0"))
		Expect(pool.lastQuery).To(ContainSubstring("CASE WHEN substr(md5(p.preference_example_id::text), 1, 1)"))
		Expect(pool.lastQuery).To(ContainSubstring("DISTINCT ON"))
		Expect(pool.lastQuery).To(ContainSubstring("p.preference_example_id DESC"))
		args := namedArgs(pool.lastArgs)
		Expect(args).To(SatisfyAll(
			HaveKeyWithValue("request_id", pgtype.UUID{Bytes: requestID, Valid: true}),
			HaveKeyWithValue("user_id", pgtype.UUID{Bytes: userID, Valid: true}),
			HaveKeyWithValue("limit", 100),
		))
	})

	It("records preference dataset snapshots and enqueues the ready fact in the same transaction", func() {
		outbox := &orderedOutboxStub{}
		signaled := false
		repository = inferencedb.NewInferenceFeedbackRepository(
			coreDB.NewDatabase(pool, "test_db"),
			inferencedb.WithPreferenceDatasetOutbox(outbox, "inference"),
			inferencedb.WithOutboxSignal(func() { signaled = true }),
		)
		preferenceDatasetID := uuid.New()
		datasetID := uuid.New()
		modelID := uuid.New()
		dataset := &model.PreferenceDataset{
			PreferenceDatasetID: preferenceDatasetID,
			RequestID:           requestID,
			UserID:              userID,
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
				DatasetID:           datasetID,
				ModelID:             modelID,
				Split:               "TRAIN",
			}},
		}

		record, err := repository.RecordPreferenceDatasetSnapshot(ctx, dataset, model.PreferenceDatasetExportRequest{
			RequestID:   requestID,
			UserID:      userID,
			MinExamples: 10,
			Limit:       100,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(record).To(Equal(dataset))
		Expect(pool.commitCalled).To(BeTrue())
		Expect(pool.rollbackCalled).To(BeFalse())
		Expect(pool.queries[0]).To(ContainSubstring("INSERT INTO test_db.preference_dataset_snapshots"))
		Expect(outbox.calls).To(Equal(1))
		Expect(outbox.tx).NotTo(BeNil())
		Expect(outbox.message.Topic).To(Equal("inference"))
		Expect(outbox.message.Message.ResourceKey).To(Equal(datasetID))
		Expect(outbox.message.Message.MsgType).To(Equal(msgConn.MsgTypePreferenceDatasetReady))
		Expect(outbox.message.DispatchKey).To(Equal("preference_dataset_ready:" + preferenceDatasetID.String()))
		Expect(signaled).To(BeTrue())

		var payload inferencepb.PreferenceDatasetReadyEvent
		Expect(proto.Unmarshal(outbox.message.Message.Payload, &payload)).To(Succeed())
		Expect(payload.PreferenceDatasetId).To(Equal(preferenceDatasetID.String()))
		Expect(payload.UserId).To(Equal(userID.String()))
		Expect(payload.OutputUri).To(Equal("s3://local-dev-bucket/preferences/dpo.jsonl"))
		Expect(payload.EvaluationOutputUri).To(Equal("s3://local-dev-bucket/preferences/dpo-eval.jsonl"))
		Expect(payload.Format).To(Equal("DPO_JSONL"))
		Expect(payload.MinExamples).To(Equal(int32(10)))
		Expect(payload.Limit).To(Equal(int32(100)))
		Expect(payload.ParentAdapterUri).To(Equal("s3://models/parent"))
		Expect(payload.ParentBaseModel).To(Equal("mistral-7b"))
		Expect(payload.ParentModelVersion).To(Equal(int32(7)))
	})
})

func feedbackRow(feedback *model.InferenceFeedback) pgx.Row {
	return &repositoryRow{values: []any{
		feedback.FeedbackID.String(),
		feedback.RequestID.String(),
		feedback.UserID.String(),
		feedback.Accepted,
		feedback.Rating,
		feedback.Comment,
		feedback.PreferredAnswer,
	}}
}

type orderedOutboxStub struct {
	tx      pgx.Tx
	message msgConn.OutboundMessage
	err     error
	calls   int
}

func (s *orderedOutboxStub) EnqueueTx(_ context.Context, tx pgx.Tx, msg msgConn.OutboundMessage) error {
	s.tx = tx
	s.message = msg
	s.calls++
	return s.err
}
