package db

import (
	"regexp"
	"strings"

	"feature_materializer_service/pkg/domain/model"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("retrieval policy SQL", func() {
	Describe("retrievalPolicyNamedArgs", func() {
		It("normalizes allowed and denied resources and reports the ANN scan budget", func() {
			allowedID := uuid.New()
			deniedID := uuid.New()

			args := retrievalPolicyNamedArgs(model.RetrievalPolicy{
				AllowedResourceIDs: []uuid.UUID{uuid.Nil, allowedID},
				DeniedResourceIDs:  []uuid.UUID{deniedID, uuid.Nil},
			}, 3)

			Expect(args).To(HaveKeyWithValue("assertion_status_admitted", model.AssertionStatusAdmitted.String()))
			Expect(args).To(HaveKeyWithValue("allow_resource_filter_disabled", false))
			Expect(args).To(HaveKeyWithValue("allow_resource_ids", []string{allowedID.String()}))
			Expect(args).To(HaveKeyWithValue("deny_resource_ids", []string{deniedID.String()}))
			Expect(args).To(HaveKeyWithValue("scan_budget", 24))
		})

		It("uses the requested top-k as scan budget for exact-authorized retrieval", func() {
			args := retrievalPolicyNamedArgs(model.RetrievalPolicy{
				Mode: model.RetrievalModeExactAuthorized,
			}, 3)

			Expect(args).To(HaveKeyWithValue("allow_resource_filter_disabled", true))
			Expect(args).To(HaveKeyWithValue("allow_resource_ids", []string{}))
			Expect(args).To(HaveKeyWithValue("deny_resource_ids", []string{}))
			Expect(args).To(HaveKeyWithValue("scan_budget", 3))
		})
	})

	Describe("retrievalAuthorizationAnyPredicate", func() {
		It("builds a typed admitted-status and allow/deny UUID predicate", func() {
			predicate := retrievalAuthorizationAnyPredicate([]string{
				" embedding_record_id ",
				"graph_node_id",
				"",
			}, "assertion_status")

			expected := `assertion_status = @assertion_status_admitted::assertion_status_enum
				AND (@allow_resource_filter_disabled::boolean OR (embedding_record_id = ANY(@allow_resource_ids::uuid[]) OR graph_node_id = ANY(@allow_resource_ids::uuid[])))
				AND NOT (embedding_record_id = ANY(@deny_resource_ids::uuid[]) OR graph_node_id = ANY(@deny_resource_ids::uuid[]))`

			Expect(compactSQL(predicate)).To(Equal(compactSQL(expected)))
			Expect(predicate).NotTo(ContainSubstring("::text"))
		})

		It("fails closed when no resource expressions are supplied", func() {
			predicate := retrievalAuthorizationAnyPredicate([]string{"", "   "}, "assertion_status")

			Expect(compactSQL(predicate)).To(ContainSubstring(compactSQL("(@allow_resource_filter_disabled::boolean OR (false))")))
			Expect(compactSQL(predicate)).To(ContainSubstring(compactSQL("AND NOT (false)")))
		})
	})

	Describe("assertionStatusValue", func() {
		It("normalizes assertion status values", func() {
			Expect(assertionStatusValue("")).To(Equal(model.AssertionStatusAdmitted.String()))
			Expect(assertionStatusValue(model.AssertionStatus(" ReVoKeD "))).To(Equal(model.AssertionStatusRevoked.String()))
		})
	})

	Describe("mergeNamedArgs", func() {
		It("keeps left values and lets right values override matching keys", func() {
			merged := mergeNamedArgs(pgx.NamedArgs{
				"keep":     "left",
				"override": "left",
			}, pgx.NamedArgs{
				"override": "right",
				"add":      "right",
			})

			Expect(merged).To(HaveKeyWithValue("keep", "left"))
			Expect(merged).To(HaveKeyWithValue("override", "right"))
			Expect(merged).To(HaveKeyWithValue("add", "right"))
		})
	})

	Describe("retrievalDisclosure", func() {
		It("reports default ANN disclosure values and underfill status", func() {
			disclosure := retrievalDisclosure(model.RetrievalPolicy{
				PolicyID:    "policy-1",
				PolicyHash:  "hash-1",
				PrincipalID: uuid.New(),
				Purpose:     "answer",
			}, 3, 2)

			Expect(disclosure.Mode).To(Equal(model.RetrievalModeANNIterative))
			Expect(disclosure.ScanBudget).To(Equal(24))
			Expect(disclosure.CandidateCount).To(Equal(24))
			Expect(disclosure.Underfilled).To(BeTrue())
		})
	})
})

func compactSQL(value string) string {
	return strings.TrimSpace(regexp.MustCompile(`\s+`).ReplaceAllString(value, " "))
}
