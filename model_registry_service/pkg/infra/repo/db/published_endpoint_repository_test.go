package db_test

import (
	"context"
	"errors"

	"model_registry_service/pkg/domain"
	"model_registry_service/pkg/domain/model"
	modeldb "model_registry_service/pkg/infra/repo/db"

	coreDB "lib/shared_lib/db"

	"github.com/google/uuid"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("PublishedEndpointRepository", func() {
	var (
		ctx        context.Context
		pool       *testConnectionPool
		tx         *testTx
		repository *modeldb.PublishedEndpointRepository
		endpoint   *model.PublishedEndpoint
	)

	BeforeEach(func() {
		ctx = context.Background()
		pool = &testConnectionPool{}
		tx = &testTx{pool: pool}
		repository = modeldb.NewPublishedEndpointRepository(coreDB.NewDatabase(pool, "test_db"))
		endpoint = &model.PublishedEndpoint{
			EndpointID:      uuid.New(),
			OrgID:           uuid.New(),
			ModelID:         uuid.New(),
			DatasetID:       uuid.New(),
			Status:          model.PublishedEndpointStatusReady,
			DisplayName:     "Fraud model endpoint",
			CreatedByUserID: uuid.New(),
		}
	})

	It("upserts the inference endpoint projection with named args", func() {
		err := repository.UpsertEndpoint(ctx, tx, endpoint)

		Expect(err).NotTo(HaveOccurred())
		Expect(pool.ExecCalls).To(HaveLen(1))
		Expect(pool.ExecCalls[0]).To(ContainSubstring("INSERT INTO test_db.published_inference_endpoints"))
		Expect(pool.ExecCalls[0]).To(ContainSubstring("ON CONFLICT (org_id, model_id, dataset_id) DO UPDATE SET"))
		args := namedArgs(pool.ExecArgs[0])
		Expect(args).To(SatisfyAll(
			HaveKeyWithValue("org_id", pgtype.UUID{Bytes: endpoint.OrgID, Valid: true}),
			HaveKeyWithValue("model_id", pgtype.UUID{Bytes: endpoint.ModelID, Valid: true}),
			HaveKeyWithValue("dataset_id", pgtype.UUID{Bytes: endpoint.DatasetID, Valid: true}),
			HaveKeyWithValue("status", string(model.PublishedEndpointStatusReady)),
			HaveKeyWithValue("display_name", endpoint.DisplayName),
			HaveKeyWithValue("created_by_user_id", pgtype.UUID{Bytes: endpoint.CreatedByUserID, Valid: true}),
		))
	})

	It("maps unresolved projection references to a domain validation error", func() {
		pool.NextError = &pgconn.PgError{Code: pgerrcode.ForeignKeyViolation}

		err := repository.UpsertEndpoint(ctx, tx, endpoint)

		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
		Expect(err).To(MatchError(ContainSubstring("published endpoint reference is not ready")))
	})

	It("wraps generic upsert failures", func() {
		expectedErr := errors.New("database unavailable")
		pool.NextError = expectedErr

		err := repository.UpsertEndpoint(ctx, tx, endpoint)

		Expect(err).To(MatchError(ContainSubstring("upsert published endpoint")))
		Expect(errors.Is(err, expectedErr)).To(BeTrue())
	})
})
