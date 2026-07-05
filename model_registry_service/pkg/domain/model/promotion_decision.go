package model

import (
	"fmt"
	"strings"
)

type PromotionDecisionOutcome int

const (
	PromotionDecisionOutcomeAccepted PromotionDecisionOutcome = iota
	PromotionDecisionOutcomeRejected
)

func (o PromotionDecisionOutcome) String() string {
	if o < PromotionDecisionOutcomeAccepted || o > PromotionDecisionOutcomeRejected {
		return "UNKNOWN"
	}
	return [...]string{"PROMOTION_ACCEPTED", "PROMOTION_REJECTED"}[o]
}

func ToPromotionDecisionOutcome(value string) (PromotionDecisionOutcome, error) {
	switch strings.TrimSpace(value) {
	case PromotionDecisionOutcomeAccepted.String():
		return PromotionDecisionOutcomeAccepted, nil
	case PromotionDecisionOutcomeRejected.String():
		return PromotionDecisionOutcomeRejected, nil
	default:
		return 0, fmt.Errorf("invalid promotion decision outcome %q", value)
	}
}

func PromotionDecisionReason(outcome PromotionDecisionOutcome, reason string) string {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return outcome.String()
	}
	return outcome.String() + ": " + reason
}
