package messaging

import (
	"crypto/sha256"
	"fmt"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"lib/shared_lib/idem"
)

var _ = Describe("deriveOutboxEventID", func() {
	It("uses the canonical idempotency part order", func() {
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
		Expect(got).To(Equal(want))
	})
})
