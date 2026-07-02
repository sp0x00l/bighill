package db_test

import (
	"context"
	"errors"

	"inference_service/pkg/domain/model"
	inferencedb "inference_service/pkg/infra/repo/db"
	coreDB "lib/shared_lib/db"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
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
			FeedbackID: feedbackID,
			RequestID:  requestID,
			UserID:     userID,
			Accepted:   false,
			Rating:     -1,
			Comment:    "wrong answer",
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
		Expect(pool.lastQuery).To(ContainSubstring("JOIN test_db.inference_requests"))
		Expect(pool.lastQuery).To(ContainSubstring("WHERE req.model_id IS NOT NULL"))
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
})

func feedbackRow(feedback *model.InferenceFeedback) pgx.Row {
	return &repositoryRow{values: []any{
		feedback.FeedbackID.String(),
		feedback.RequestID.String(),
		feedback.UserID.String(),
		feedback.Accepted,
		feedback.Rating,
		feedback.Comment,
	}}
}
