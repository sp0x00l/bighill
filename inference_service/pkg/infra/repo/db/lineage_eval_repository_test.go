package db_test

import (
	"context"
	"time"

	"inference_service/pkg/domain"
	"inference_service/pkg/domain/model"
	inferencedb "inference_service/pkg/infra/repo/db"
	coreDB "lib/shared_lib/db"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("LineageEvalRepository", func() {
	var (
		ctx        context.Context
		pool       *connectionPoolStub
		tx         pgx.Tx
		repository *inferencedb.LineageEvalRepository
		orgID      uuid.UUID
		frozenAt   time.Time
	)

	BeforeEach(func() {
		ctx = context.Background()
		pool = &connectionPoolStub{}
		tx = &inferenceTxStub{pool: pool}
		repository = inferencedb.NewLineageEvalRepository(coreDB.NewDatabase(pool, "test_db"))
		orgID = uuid.New()
		frozenAt = time.Date(2026, 7, 11, 10, 30, 0, 0, time.UTC)
	})

	It("reads the active eval set for a lineage", func() {
		pool.nextRows = []pgx.Row{&repositoryRow{values: []any{
			orgID.String(),
			"fraud-rag-ranker",
			2,
			"s3://evals/frozen.jsonl",
			"sha256:eval",
			12,
			"FROZEN_GEN0",
			true,
			frozenAt,
		}}}

		record, err := repository.ReadActiveEvalSet(ctx, orgID, " fraud-rag-ranker ")

		Expect(err).NotTo(HaveOccurred())
		Expect(record.OrgID).To(Equal(orgID))
		Expect(record.LineageName).To(Equal("fraud-rag-ranker"))
		Expect(record.Version).To(Equal(2))
		Expect(record.EvalDatasetURI).To(Equal("s3://evals/frozen.jsonl"))
		Expect(record.Checksum).To(Equal("sha256:eval"))
		Expect(record.ExampleCount).To(Equal(12))
		Expect(record.Source).To(Equal(model.LineageEvalSetSourceFrozenGen0))
		Expect(record.Active).To(BeTrue())
		Expect(record.FrozenAt).To(Equal(frozenAt))
		Expect(pool.lastQuery).To(ContainSubstring("FROM test_db.lineage_eval_sets"))
		Expect(pool.lastQuery).To(ContainSubstring("is_active = true"))
		Expect(namedArgs(pool.lastArgs)).To(SatisfyAll(
			HaveKeyWithValue("org_id", pgtype.UUID{Bytes: orgID, Valid: true}),
			HaveKeyWithValue("lineage_name", "fraud-rag-ranker"),
		))
	})

	It("returns a typed not-found error when no active eval set exists", func() {
		pool.nextRows = []pgx.Row{&repositoryRow{err: pgx.ErrNoRows}}

		record, err := repository.ReadActiveEvalSet(ctx, orgID, "fraud-rag-ranker")

		Expect(record).To(BeNil())
		Expect(err).To(MatchError(domain.ErrEvalSetNotFound))
	})

	It("freezes an eval set and records frozen example membership", func() {
		exampleA := uuid.New()
		exampleB := uuid.New()
		evalSet := &model.LineageEvalSet{
			OrgID:          orgID,
			LineageName:    "fraud-rag-ranker",
			EvalDatasetURI: "s3://evals/frozen.jsonl",
			Checksum:       "sha256:eval",
			Source:         model.LineageEvalSetSourceFrozenGen0,
		}

		record, err := repository.FreezeEvalSet(ctx, tx, evalSet, []uuid.UUID{exampleA, exampleB})

		Expect(err).NotTo(HaveOccurred())
		Expect(record.Version).To(Equal(1))
		Expect(record.ExampleCount).To(Equal(2))
		Expect(record.Active).To(BeTrue())
		Expect(record.FrozenAt).NotTo(BeZero())
		Expect(pool.queries).To(HaveLen(2))
		Expect(pool.queries[0]).To(ContainSubstring("INSERT INTO test_db.lineage_eval_sets"))
		Expect(pool.queries[1]).To(ContainSubstring("INSERT INTO test_db.lineage_eval_examples"))
		Expect(namedArgs(pool.args[0])).To(SatisfyAll(
			HaveKeyWithValue("org_id", pgtype.UUID{Bytes: orgID, Valid: true}),
			HaveKeyWithValue("lineage_name", "fraud-rag-ranker"),
			HaveKeyWithValue("eval_set_version", 1),
			HaveKeyWithValue("eval_dataset_uri", "s3://evals/frozen.jsonl"),
			HaveKeyWithValue("checksum", "sha256:eval"),
			HaveKeyWithValue("example_count", 2),
			HaveKeyWithValue("source", "FROZEN_GEN0"),
			HaveKeyWithValue("is_active", true),
		))
		Expect(namedArgs(pool.args[1])).To(SatisfyAll(
			HaveKeyWithValue("org_id", pgtype.UUID{Bytes: orgID, Valid: true}),
			HaveKeyWithValue("lineage_name", "fraud-rag-ranker"),
			HaveKeyWithValue("eval_set_version", 1),
			HaveKeyWithValue("preference_example_ids", []uuid.UUID{exampleA, exampleB}),
		))
	})

	It("registers curated eval sets with the curated source", func() {
		evalSet := &model.LineageEvalSet{
			OrgID:          orgID,
			LineageName:    "fraud-rag-ranker",
			EvalDatasetURI: "s3://evals/curated.jsonl",
			Checksum:       "sha256:curated",
			ExampleCount:   9,
		}
		pool.nextRows = []pgx.Row{&repositoryRow{values: []any{4}}}

		_, err := repository.RegisterCuratedEvalSet(ctx, tx, evalSet, nil)

		Expect(err).NotTo(HaveOccurred())
		Expect(pool.queries).To(HaveLen(3))
		Expect(pool.queries[0]).To(ContainSubstring("COALESCE(MAX(eval_set_version), 0) + 1"))
		Expect(pool.queries[1]).To(ContainSubstring("SET is_active = false"))
		Expect(pool.queries[2]).To(ContainSubstring("INSERT INTO test_db.lineage_eval_sets"))
		Expect(namedArgs(pool.lastArgs)).To(SatisfyAll(
			HaveKeyWithValue("source", "CURATED"),
			HaveKeyWithValue("eval_set_version", 4),
			HaveKeyWithValue("example_count", 9),
		))
	})
})
