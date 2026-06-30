package messaging

import (
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/google/uuid"

	"lib/shared_lib/idem"
)

func TestDeriveOutboxEventIDUsesCanonicalPartOrder(t *testing.T) {
	resourceKey := uuid.MustParse("33333333-3333-3333-3333-333333333333")
	payload := []byte(`{"event":"created"}`)
	createdAt := "2026-05-07T00:00:00Z"
	payloadHash := sha256.Sum256(payload)

	got := deriveOutboxEventID("profile", Message{
		ResourceKey: resourceKey,
		MsgType:     MsgTypeUserUpdated,
	}, payload, createdAt)

	want := idem.FromParts(
		idem.Outbox,
		"profile",
		MsgTypeUserUpdated.String(),
		resourceKey.String(),
		fmt.Sprintf("%x", payloadHash),
		createdAt,
	).String()
	if got != want {
		t.Fatalf("unexpected outbox event id: got=%s want=%s", got, want)
	}
}
