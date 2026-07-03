package messaging

import (
	"fmt"

	"github.com/google/uuid"

	profileeventpb "lib/data_contracts_lib/profile"
)

// ParseUUID parses a required string UUID field from a deserialized event
// payload. Invalid values are marked non-retryable so subscribers can route
// deterministic deserialization failures directly to the DLQ.
func ParseUUID(field, raw string) (uuid.UUID, error) {
	if raw == "" {
		return uuid.Nil, NonRetryable(fmt.Errorf("%s required", field))
	}
	id, err := uuid.Parse(raw)
	if err != nil {
		return uuid.Nil, NonRetryable(fmt.Errorf("invalid %s uuid %q: %w", field, raw, err))
	}
	if id == uuid.Nil {
		return uuid.Nil, NonRetryable(fmt.Errorf("%s must not be the nil UUID", field))
	}
	return id, nil
}

// ParseOptionalUUID is like ParseUUID but returns uuid.Nil without error when
// the raw string is empty.
func ParseOptionalUUID(field, raw string) (uuid.UUID, error) {
	if raw == "" {
		return uuid.Nil, nil
	}
	return ParseUUID(field, raw)
}

func EmailVerificationStatusFromProfileEventProto(field string, status profileeventpb.EmailVerificationStatus) (bool, error) {
	switch status {
	case profileeventpb.EmailVerificationStatus_EMAIL_VERIFICATION_STATUS_UNVERIFIED:
		return false, nil
	case profileeventpb.EmailVerificationStatus_EMAIL_VERIFICATION_STATUS_VERIFIED:
		return true, nil
	case profileeventpb.EmailVerificationStatus_EMAIL_VERIFICATION_STATUS_UNSPECIFIED:
		return false, NonRetryable(fmt.Errorf("%s requires email verification status", field))
	default:
		return false, NonRetryable(fmt.Errorf("%s email verification status is invalid: %s", field, status.String()))
	}
}

func EmailVerificationStatusToProfileEventProto(verified bool) profileeventpb.EmailVerificationStatus {
	if verified {
		return profileeventpb.EmailVerificationStatus_EMAIL_VERIFICATION_STATUS_VERIFIED
	}
	return profileeventpb.EmailVerificationStatus_EMAIL_VERIFICATION_STATUS_UNVERIFIED
}
