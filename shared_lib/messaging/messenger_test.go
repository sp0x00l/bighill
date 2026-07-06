package messaging

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Messenger", func() {
	It("waits for the subscriber goroutine to finish when closing", func() {
		_, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan struct{})
		m := &messenger{subscriberCancel: cancel}
		m.setSubscriberDone(done)
		closeReturned := make(chan error, 1)

		go func() {
			closeReturned <- m.Close(context.Background())
		}()

		Consistently(closeReturned, 25*time.Millisecond).ShouldNot(Receive())
		close(done)
		Eventually(closeReturned).Should(Receive(Succeed()))
	})

	It("returns the close context error when subscriber drain exceeds the close budget", func() {
		_, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan struct{})
		m := &messenger{subscriberCancel: cancel}
		m.setSubscriberDone(done)
		closeCtx, closeCancel := context.WithCancel(context.Background())
		closeCancel()

		err := m.Close(closeCtx)

		Expect(err).To(MatchError(context.Canceled))
	})
})
