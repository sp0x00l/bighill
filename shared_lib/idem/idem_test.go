package idem

import (
	"testing"

	"github.com/google/uuid"
)

func TestKeyMatchesFromPartsForUUIDSeed(t *testing.T) {
	seed := uuid.MustParse("11111111-1111-1111-1111-111111111111")

	got := Key(seed, OAuthProfile, "verified")
	want := FromParts(seed.String(), OAuthProfile, "verified")
	if got != want {
		t.Fatalf("Key and FromParts mismatch: got=%s want=%s", got, want)
	}
}

func TestJoinPreservesCanonicalPartOrder(t *testing.T) {
	got := Join(Outbox, "profile", "user-updated", "22222222-2222-2222-2222-222222222222", "payload-hash", "2026-05-07T00:00:00Z")
	want := "outbox:profile:user-updated:22222222-2222-2222-2222-222222222222:payload-hash:2026-05-07T00:00:00Z"
	if got != want {
		t.Fatalf("unexpected join seed: got=%q want=%q", got, want)
	}
}

func TestJoinRejectsEmptyParts(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected Join to panic on empty part")
		}
	}()

	Join(Outbox, "")
}
