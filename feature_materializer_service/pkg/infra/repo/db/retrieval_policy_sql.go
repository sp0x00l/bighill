package db

import (
	"feature_materializer_service/pkg/domain/model"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	log "github.com/sirupsen/logrus"
)

func normalizeRetrievalPolicy(policy model.RetrievalPolicy, topK int) model.RetrievalPolicy {
	log.Trace("normalizeRetrievalPolicy")

	return model.NormalizeRetrievalPolicy(policy, topK)
}

func retrievalPolicyNamedArgs(policy model.RetrievalPolicy, topK int) pgx.NamedArgs {
	log.Trace("retrievalPolicyNamedArgs")

	policy = normalizeRetrievalPolicy(policy, topK)
	return pgx.NamedArgs{
		"assertion_status_admitted":      model.AssertionStatusAdmitted.String(),
		"allow_resource_filter_disabled": len(policy.AllowedResourceIDs) == 0,
		"allow_resource_ids":             uuidStrings(policy.AllowedResourceIDs),
		"deny_resource_ids":              uuidStrings(policy.DeniedResourceIDs),
		"scan_budget":                    policy.ScanBudget,
	}
}

func assertionStatusValue(status model.AssertionStatus) string {
	log.Trace("assertionStatusValue")

	return model.ParseAssertionStatus(status.String()).String()
}

func mergeNamedArgs(left pgx.NamedArgs, right pgx.NamedArgs) pgx.NamedArgs {
	log.Trace("mergeNamedArgs")

	out := pgx.NamedArgs{}
	for key, value := range left {
		out[key] = value
	}
	for key, value := range right {
		out[key] = value
	}
	return out
}

func retrievalAuthorizationPredicate(resourceIDExpression string, assertionStatusExpression string) string {
	log.Trace("retrievalAuthorizationPredicate")

	return retrievalAuthorizationAnyPredicate([]string{resourceIDExpression}, assertionStatusExpression)
}

func retrievalAuthorizationAnyPredicate(resourceIDExpressions []string, assertionStatusExpression string) string {
	log.Trace("retrievalAuthorizationAnyPredicate")

	allowClauses := make([]string, 0, len(resourceIDExpressions))
	denyClauses := make([]string, 0, len(resourceIDExpressions))
	for _, expression := range resourceIDExpressions {
		expression = strings.TrimSpace(expression)
		if expression == "" {
			continue
		}
		allowClauses = append(allowClauses, expression+` = ANY(@allow_resource_ids::uuid[])`)
		denyClauses = append(denyClauses, expression+` = ANY(@deny_resource_ids::uuid[])`)
	}
	if len(allowClauses) == 0 {
		allowClauses = append(allowClauses, "false")
	}
	if len(denyClauses) == 0 {
		denyClauses = append(denyClauses, "false")
	}
	return assertionStatusExpression + ` = @assertion_status_admitted::assertion_status_enum
			AND (@allow_resource_filter_disabled::boolean OR (` + strings.Join(allowClauses, " OR ") + `))
			AND NOT (` + strings.Join(denyClauses, " OR ") + `)`
}

func retrievalDisclosure(policy model.RetrievalPolicy, topK int, resultCount int) model.RetrievalDisclosure {
	log.Trace("retrievalDisclosure")

	return model.NewRetrievalDisclosure(policy, topK, resultCount)
}

func uuidStrings(values []uuid.UUID) []string {
	log.Trace("uuidStrings")

	out := make([]string, 0, len(values))
	for _, value := range values {
		if value == uuid.Nil {
			continue
		}
		out = append(out, value.String())
	}
	return out
}
