package model

import (
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	AssertionStatusCandidate AssertionStatus = "candidate"
	AssertionStatusAdmitted  AssertionStatus = "admitted"
	AssertionStatusRejected  AssertionStatus = "rejected"
	AssertionStatusRevoked   AssertionStatus = "revoked"

	RetrievalModeANNIterative    RetrievalMode = "ann_iterative"
	RetrievalModeExactAuthorized RetrievalMode = "exact_authorized"
)

type AssertionStatus string

func ParseAssertionStatus(value string) AssertionStatus {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case string(AssertionStatusCandidate):
		return AssertionStatusCandidate
	case "", string(AssertionStatusAdmitted):
		return AssertionStatusAdmitted
	case string(AssertionStatusRejected):
		return AssertionStatusRejected
	case string(AssertionStatusRevoked):
		return AssertionStatusRevoked
	default:
		return AssertionStatus(strings.ToLower(strings.TrimSpace(value)))
	}
}

func (s AssertionStatus) String() string {
	if s == "" {
		return string(AssertionStatusAdmitted)
	}
	return string(s)
}

func (s AssertionStatus) IsValid() bool {
	switch s {
	case "", AssertionStatusCandidate, AssertionStatusAdmitted, AssertionStatusRejected, AssertionStatusRevoked:
		return true
	default:
		return false
	}
}

func (s AssertionStatus) IsRetrievable() bool {
	return ParseAssertionStatus(s.String()) == AssertionStatusAdmitted
}

type RetrievalMode string

func ParseRetrievalMode(value string) RetrievalMode {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", string(RetrievalModeANNIterative):
		return RetrievalModeANNIterative
	case string(RetrievalModeExactAuthorized):
		return RetrievalModeExactAuthorized
	default:
		return RetrievalMode(strings.ToLower(strings.TrimSpace(value)))
	}
}

func (m RetrievalMode) String() string {
	if m == "" {
		return string(RetrievalModeANNIterative)
	}
	return string(m)
}

func (m RetrievalMode) IsValid() bool {
	switch m {
	case "", RetrievalModeANNIterative, RetrievalModeExactAuthorized:
		return true
	default:
		return false
	}
}

type RetrievalPolicy struct {
	Mode               RetrievalMode
	PolicyID           string
	PolicyHash         string
	PrincipalID        uuid.UUID
	Purpose            string
	Context            map[string]string
	AsOf               time.Time
	AllowedResourceIDs []uuid.UUID
	DeniedResourceIDs  []uuid.UUID
	ScanBudget         int
}

type RetrievalDisclosure struct {
	Mode            RetrievalMode
	PolicyID        string
	PolicyHash      string
	PrincipalID     uuid.UUID
	Purpose         string
	AsOf            time.Time
	ScanBudget      int
	CandidateCount  int
	AuthorizedCount int
	ResultCount     int
	Underfilled     bool
}

func NormalizeRetrievalPolicy(policy RetrievalPolicy, topK int) RetrievalPolicy {
	policy.Mode = ParseRetrievalMode(policy.Mode.String())
	if policy.Context == nil {
		policy.Context = map[string]string{}
	}
	if policy.AsOf.IsZero() {
		policy.AsOf = time.Now().UTC()
	}
	if topK <= 0 {
		topK = 5
	}
	if policy.ScanBudget <= 0 {
		policy.ScanBudget = topK
		if policy.Mode == RetrievalModeANNIterative {
			policy.ScanBudget = topK * 8
		}
	}
	if policy.ScanBudget < topK {
		policy.ScanBudget = topK
	}
	return policy
}

func NewRetrievalDisclosure(policy RetrievalPolicy, topK int, resultCount int) RetrievalDisclosure {
	policy = NormalizeRetrievalPolicy(policy, topK)
	candidateCount := resultCount
	if policy.Mode == RetrievalModeANNIterative {
		candidateCount = policy.ScanBudget
	}
	return RetrievalDisclosure{
		Mode:            policy.Mode,
		PolicyID:        policy.PolicyID,
		PolicyHash:      policy.PolicyHash,
		PrincipalID:     policy.PrincipalID,
		Purpose:         policy.Purpose,
		AsOf:            policy.AsOf,
		ScanBudget:      policy.ScanBudget,
		CandidateCount:  candidateCount,
		AuthorizedCount: resultCount,
		ResultCount:     resultCount,
		Underfilled:     resultCount < topK,
	}
}
