package db_test

import (
	"context"
	"errors"

	"inference_service/pkg/domain/model"
	inferencedb "inference_service/pkg/infra/repo/db"
	coreDB "lib/shared_lib/db"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("InferenceRequestRepository", func() {
	var (
		ctx        context.Context
		pool       *connectionPoolStub
		repository *inferencedb.InferenceRequestRepository
	)

	BeforeEach(func() {
		ctx = context.Background()
		pool = &connectionPoolStub{}
		repository = inferencedb.NewInferenceRequestRepository(coreDB.NewDatabase(pool, "test_db"))
	})

	Describe("RecordInferenceRequest", func() {
		It("upserts the inference request audit row", func() {
			request := validInferenceRequest()

			err := repository.RecordInferenceRequest(ctx, request)

			Expect(err).NotTo(HaveOccurred())
			Expect(pool.execCalled).To(BeTrue())
			Expect(pool.lastQuery).To(ContainSubstring("INSERT INTO test_db.inference_requests"))
			Expect(pool.lastQuery).To(ContainSubstring("retrieved_contexts"))
			Expect(pool.lastQuery).To(ContainSubstring("prompt_text"))
			Expect(pool.lastQuery).To(ContainSubstring("answer_text"))
			Expect(pool.lastQuery).To(ContainSubstring("ON CONFLICT (request_id) DO UPDATE SET"))
			args := namedArgs(pool.lastArgs)
			Expect(args).To(SatisfyAll(
				HaveKeyWithValue("request_id", pgtype.UUID{Bytes: request.RequestID, Valid: true}),
				HaveKeyWithValue("user_id", pgtype.UUID{Bytes: request.UserID, Valid: true}),
				HaveKeyWithValue("org_id", pgtype.UUID{Bytes: request.OrgID, Valid: true}),
				HaveKeyWithValue("dataset_id", pgtype.UUID{Bytes: request.DatasetID, Valid: true}),
				HaveKeyWithValue("model_id", pgtype.UUID{Bytes: request.ModelID, Valid: true}),
				HaveKeyWithValue("embedding_snapshot_id", pgtype.UUID{Bytes: request.EmbeddingSnapshotID, Valid: true}),
				HaveKeyWithValue("query_text", request.QueryText),
				HaveKeyWithValue("top_k", request.TopK),
				HaveKeyWithValue("metadata_filters", request.MetadataFilters),
				HaveKeyWithValue("retrieved_context_ids", request.RetrievedContextIDs),
				HaveKeyWithValue("retrieved_contexts", request.RetrievedContexts),
				HaveKeyWithValue("prompt_text", request.PromptText),
				HaveKeyWithValue("answer_text", request.AnswerText),
				HaveKeyWithValue("generation_protocol", request.GenerationProtocol),
				HaveKeyWithValue("status", model.InferenceRequestStatusCompleted.String()),
			))
		})

		It("records failure requests with optional ids as nullable UUID args", func() {
			request := validInferenceRequest()
			request.ModelID = uuid.Nil
			request.EmbeddingSnapshotID = uuid.Nil
			request.Status = model.InferenceRequestStatusFailed
			request.ErrorMessage = "retrieval failed"

			err := repository.RecordInferenceRequest(ctx, request)

			Expect(err).NotTo(HaveOccurred())
			args := namedArgs(pool.lastArgs)
			Expect(args).To(SatisfyAll(
				HaveKeyWithValue("model_id", pgtype.UUID{}),
				HaveKeyWithValue("embedding_snapshot_id", pgtype.UUID{}),
				HaveKeyWithValue("status", model.InferenceRequestStatusFailed.String()),
				HaveKeyWithValue("error_message", "retrieval failed"),
			))
		})

		It("wraps database errors", func() {
			pool.nextExecErr = errors.New("insert failed")

			err := repository.RecordInferenceRequest(ctx, validInferenceRequest())

			Expect(err).To(MatchError(ContainSubstring("record inference request: insert failed")))
		})
	})
})

func validInferenceRequest() *model.InferenceRequest {
	return &model.InferenceRequest{
		RequestID:             uuid.New(),
		UserID:                uuid.New(),
		OrgID:                 uuid.New(),
		DatasetID:             uuid.New(),
		ModelID:               uuid.New(),
		EmbeddingSnapshotID:   uuid.New(),
		QueryText:             "Which policies mention retention?",
		TopK:                  5,
		MetadataFilters:       `{"department":"risk"}`,
		RetrievedContextIDs:   `["chunk-1","chunk-2"]`,
		RetrievedContexts:     `[{"embedding_record_id":"chunk-1","source_text":"policy text"}]`,
		PromptText:            "Use the retrieved context to answer.",
		AnswerText:            "The policy mentions seven years of retention.",
		PromptStrategyVersion: "rag-default-v1",
		GenerationProtocol:    "OPENAI_CHAT_COMPLETIONS",
		GenerationModel:       "llama3.2",
		LatencyMs:             123,
		Status:                model.InferenceRequestStatusCompleted,
		ErrorMessage:          "",
	}
}
