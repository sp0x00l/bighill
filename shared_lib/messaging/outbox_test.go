package messaging_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"lib/shared_lib/messaging"
)

var _ = Describe("Outbox Factory", func() {
	ctx := context.Background()

	It("returns NoopOutbox for empty url", func() {
		outbox := messaging.NewOutbox(ctx, "")
		err := outbox.WriteMessage(ctx, messaging.OutboxMessage{})
		Expect(err).ToNot(HaveOccurred())
	})

	It("returns NoopOutbox for noop url", func() {
		outbox := messaging.NewOutbox(ctx, "noop://local")
		err := outbox.WriteMessage(ctx, messaging.OutboxMessage{})
		Expect(err).ToNot(HaveOccurred())
	})

	It("returns NoopOutbox for dynamodb url with empty table", func() {
		outbox := messaging.NewOutbox(ctx, "dynamodb://")
		err := outbox.WriteMessage(ctx, messaging.OutboxMessage{})
		Expect(err).ToNot(HaveOccurred())
	})

	It("returns NoopOutbox for unsupported url", func() {
		outbox := messaging.NewOutbox(ctx, "unknown://backend")
		err := outbox.WriteMessage(ctx, messaging.OutboxMessage{})
		Expect(err).ToNot(HaveOccurred())
	})
})
