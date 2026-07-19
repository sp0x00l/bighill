package db_test

import (
	"context"
	"errors"
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

var _ = Describe("PublishedEndpointRepository", func() {
	var (
		ctx        context.Context
		pool       *connectionPoolStub
		repository *inferencedb.PublishedEndpointRepository
		endpoint   *model.PublishedEndpoint
		datasetA   uuid.UUID
		datasetB   uuid.UUID
	)

	BeforeEach(func() {
		ctx = context.Background()
		pool = &connectionPoolStub{}
		repository = inferencedb.NewPublishedEndpointRepository(coreDB.NewDatabase(pool, "test_db"))
		datasetA = uuid.New()
		datasetB = uuid.New()
		endpoint = validPublishedEndpoint(datasetA, datasetB)
	})

	Describe("UpsertEndpoint", func() {
		It("upserts the endpoint, replaces dataset bindings, and reads the saved projection", func() {
			pool.nextRows = []pgx.Row{
				&repositoryRow{values: []any{endpoint.EndpointID.String()}},
				publishedEndpointRow(endpoint),
			}

			record, err := repository.UpsertEndpoint(ctx, endpoint)

			Expect(err).NotTo(HaveOccurred())
			Expect(record).To(Equal(endpoint))
			Expect(pool.commitCalled).To(BeTrue())
			Expect(pool.queries[0]).To(ContainSubstring("INSERT INTO test_db.published_inference_endpoints"))
			Expect(pool.queries[0]).To(ContainSubstring("ON CONFLICT (org_id, model_id) DO UPDATE SET"))
			Expect(pool.queries[1]).To(ContainSubstring("DELETE FROM test_db.published_endpoint_datasets"))
			Expect(pool.queries[2]).To(ContainSubstring("INSERT INTO test_db.published_endpoint_datasets"))
			Expect(pool.queries[4]).To(ContainSubstring("FROM test_db.published_inference_endpoints endpoint"))
			args := namedArgs(pool.args[0])
			Expect(args).To(SatisfyAll(
				HaveKeyWithValue("org_id", pgtype.UUID{Bytes: endpoint.OrgID, Valid: true}),
				HaveKeyWithValue("model_id", pgtype.UUID{Bytes: endpoint.ModelID, Valid: true}),
				HaveKeyWithValue("mode", model.AgentEndpointModeAgent.String()),
				HaveKeyWithValue("agent_spec_id", pgtype.UUID{Bytes: endpoint.AgentSpecID, Valid: true}),
				HaveKeyWithValue("agent_spec_hash", endpoint.AgentSpecHash),
				HaveKeyWithValue("status", string(model.PublishedEndpointStatusReady)),
				HaveKeyWithValue("rag_merge_strategy", string(model.RAGMergeStrategyReranker)),
				HaveKeyWithValue("display_name", endpoint.DisplayName),
				HaveKeyWithValue("created_by_user_id", pgtype.UUID{Bytes: endpoint.CreatedByUserID, Valid: true}),
			))
			Expect(namedArgs(pool.args[2])).To(SatisfyAll(
				HaveKeyWithValue("endpoint_id", pgtype.UUID{Bytes: endpoint.EndpointID, Valid: true}),
				HaveKeyWithValue("dataset_id", pgtype.UUID{Bytes: datasetA, Valid: true}),
				HaveKeyWithValue("position", 0),
			))
			Expect(namedArgs(pool.args[3])).To(HaveKeyWithValue("position", 1))
		})

		It("maps missing tenant/model/spec references to a domain validation error", func() {
			pool.nextRows = []pgx.Row{&repositoryRow{err: &pgconn.PgError{Code: "23503"}}}

			record, err := repository.UpsertEndpoint(ctx, endpoint)

			Expect(record).To(BeNil())
			Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
			Expect(pool.commitCalled).To(BeFalse())
		})
	})

	Describe("SetEndpointDatasets", func() {
		It("rejects nil dataset ids after verifying the endpoint exists", func() {
			pool.nextRows = []pgx.Row{publishedEndpointRow(endpoint)}

			record, err := repository.SetEndpointDatasets(ctx, endpoint.OrgID, endpoint.EndpointID, []uuid.UUID{datasetA, uuid.Nil})

			Expect(record).To(BeNil())
			Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
			Expect(pool.queries[0]).To(ContainSubstring("endpoint.endpoint_id = @endpoint_id AND endpoint.org_id = @org_id"))
			Expect(pool.queries[1]).To(ContainSubstring("DELETE FROM test_db.published_endpoint_datasets"))
			Expect(pool.commitCalled).To(BeFalse())
		})
	})

	Describe("ListEndpoints", func() {
		It("lists endpoints for a tenant with dataset bindings", func() {
			second := validPublishedEndpoint(uuid.New())
			second.EndpointID = uuid.New()
			second.OrgID = endpoint.OrgID
			second.DisplayName = "Back office agent"
			pool.nextQueryRows = []pgx.Rows{&repositoryRows{rows: [][]any{
				publishedEndpointValues(endpoint),
				publishedEndpointValues(second),
			}}}

			records, err := repository.ListEndpoints(ctx, endpoint.OrgID)

			Expect(err).NotTo(HaveOccurred())
			Expect(records).To(HaveLen(2))
			Expect(records[0]).To(Equal(endpoint))
			Expect(records[1]).To(Equal(second))
			Expect(pool.lastQuery).To(ContainSubstring("ORDER BY endpoint.display_name ASC, endpoint.created_at DESC"))
			Expect(namedArgs(pool.lastArgs)).To(HaveKeyWithValue("org_id", pgtype.UUID{Bytes: endpoint.OrgID, Valid: true}))
		})
	})

	Describe("ReadEndpoint", func() {
		It("maps missing endpoints to the domain not-found error", func() {
			record, err := repository.ReadEndpoint(ctx, endpoint.OrgID, endpoint.EndpointID)

			Expect(record).To(BeNil())
			Expect(errors.Is(err, domain.ErrModelNotFound)).To(BeTrue())
		})

		It("surfaces invalid persisted endpoint modes", func() {
			row := publishedEndpointRow(endpoint).(*repositoryRow)
			row.values[4] = "BROKEN"
			pool.nextRows = []pgx.Row{row}

			record, err := repository.ReadEndpoint(ctx, endpoint.OrgID, endpoint.EndpointID)

			Expect(record).To(BeNil())
			Expect(err).To(MatchError(ContainSubstring("parse published endpoint mode")))
		})
	})

	Describe("ApplyAgentChampionUpdate", func() {
		It("updates the endpoint binding when the champion decision is newer", func() {
			specID := uuid.New()
			decisionID := uuid.New()
			servingModelID := uuid.New()
			decidedAt := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
			updated := *endpoint
			updated.ServingModelID = servingModelID
			updated.AgentSpecID = specID
			updated.AgentSpecHash = "sha256:new-spec"
			updated.ChampionDecisionID = decisionID
			updated.ChampionDecidedAt = decidedAt
			pool.nextRows = []pgx.Row{
				publishedEndpointRow(endpoint),
				&repositoryRow{values: []any{specID.String(), "support_agent"}},
				publishedEndpointRow(&updated),
			}

			record, err := repository.ApplyAgentChampionUpdate(ctx, model.AgentChampionUpdate{
				OrgID:          endpoint.OrgID,
				EndpointID:     endpoint.EndpointID,
				AgentLineage:   "support_agent",
				AgentSpecHash:  "sha256:new-spec",
				ServingModelID: servingModelID,
				DecisionID:     decisionID,
				DecidedAt:      decidedAt,
			})

			Expect(err).NotTo(HaveOccurred())
			Expect(record.ModelID).To(Equal(endpoint.ModelID))
			Expect(record.ServingModelID).To(Equal(servingModelID))
			Expect(record.AgentSpecID).To(Equal(specID))
			Expect(record.AgentSpecHash).To(Equal("sha256:new-spec"))
			Expect(record.ChampionDecisionID).To(Equal(decisionID))
			Expect(record.ChampionDecidedAt).To(Equal(decidedAt))
			Expect(pool.commitCalled).To(BeTrue())
			Expect(pool.queries[2]).To(ContainSubstring("UPDATE test_db.published_inference_endpoints"))
			Expect(namedArgs(pool.args[2])).To(SatisfyAll(
				HaveKeyWithValue("agent_spec_hash", "sha256:new-spec"),
				HaveKeyWithValue("serving_model_id", pgtype.UUID{Bytes: servingModelID, Valid: true}),
				HaveKeyWithValue("decision_id", pgtype.UUID{Bytes: decisionID, Valid: true}),
			))
		})

		It("ignores stale champion decisions without updating the endpoint", func() {
			endpoint.ChampionDecisionID = uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff")
			endpoint.ChampionDecidedAt = time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
			pool.nextRows = []pgx.Row{
				publishedEndpointRow(endpoint),
				&repositoryRow{values: []any{uuid.NewString(), "support_agent"}},
			}

			record, err := repository.ApplyAgentChampionUpdate(ctx, model.AgentChampionUpdate{
				OrgID:         endpoint.OrgID,
				EndpointID:    endpoint.EndpointID,
				AgentLineage:  "support_agent",
				AgentSpecHash: "sha256:old-spec",
				DecisionID:    uuid.MustParse("00000000-0000-0000-0000-000000000001"),
				DecidedAt:     endpoint.ChampionDecidedAt.Add(-time.Minute),
			})

			Expect(err).NotTo(HaveOccurred())
			Expect(record).To(Equal(endpoint))
			Expect(pool.commitCalled).To(BeTrue())
			Expect(pool.execCalled).To(BeFalse())
		})

		It("fails closed when the local spec lineage does not match the event", func() {
			pool.nextRows = []pgx.Row{
				publishedEndpointRow(endpoint),
				&repositoryRow{values: []any{uuid.NewString(), "other_agent"}},
			}

			record, err := repository.ApplyAgentChampionUpdate(ctx, model.AgentChampionUpdate{
				OrgID:         endpoint.OrgID,
				EndpointID:    endpoint.EndpointID,
				AgentLineage:  "support_agent",
				AgentSpecHash: "sha256:new-spec",
				DecisionID:    uuid.New(),
				DecidedAt:     time.Now(),
			})

			Expect(record).To(BeNil())
			Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
			Expect(pool.commitCalled).To(BeFalse())
		})
	})
})

func validPublishedEndpoint(datasetIDs ...uuid.UUID) *model.PublishedEndpoint {
	return &model.PublishedEndpoint{
		EndpointID:      uuid.New(),
		OrgID:           uuid.New(),
		ModelID:         uuid.New(),
		Mode:            model.AgentEndpointModeAgent,
		AgentSpecID:     uuid.New(),
		AgentSpecHash:   "sha256:agent-spec",
		DatasetIDs:      datasetIDs,
		MergeStrategy:   model.RAGMergeStrategyReranker,
		Status:          model.PublishedEndpointStatusReady,
		DisplayName:     "Support agent",
		CreatedByUserID: uuid.New(),
	}
}

func publishedEndpointRow(endpoint *model.PublishedEndpoint) pgx.Row {
	return &repositoryRow{values: publishedEndpointValues(endpoint)}
}

func publishedEndpointValues(endpoint *model.PublishedEndpoint) []any {
	datasetIDs := make([]string, 0, len(endpoint.DatasetIDs))
	for _, datasetID := range endpoint.DatasetIDs {
		datasetIDs = append(datasetIDs, datasetID.String())
	}
	return []any{
		endpoint.EndpointID.String(),
		endpoint.OrgID.String(),
		endpoint.ModelID.String(),
		optionalPublishedEndpointUUIDString(endpoint.ServingModelID),
		endpoint.Mode.String(),
		optionalPublishedEndpointUUIDString(endpoint.AgentSpecID),
		endpoint.AgentSpecHash,
		string(endpoint.Status),
		string(endpoint.MergeStrategy),
		endpoint.DisplayName,
		endpoint.CreatedByUserID.String(),
		optionalPublishedEndpointUUIDString(endpoint.ChampionDecisionID),
		pgtype.Timestamptz{Time: endpoint.ChampionDecidedAt, Valid: !endpoint.ChampionDecidedAt.IsZero()},
		datasetIDs,
	}
}

func optionalPublishedEndpointUUIDString(value uuid.UUID) string {
	if value == uuid.Nil {
		return ""
	}
	return value.String()
}
