package messaging

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("PublisherInternal", func() {
	Describe("isNoopOutbox", func() {
		It("detects direct and wrapped noop outboxes", func() {
			Expect(isNoopOutbox(NewNoopOutbox())).To(BeTrue())
			Expect(isNoopOutbox(newSignalOutbox(NewNoopOutbox(), make(chan struct{}, 1)))).To(BeTrue())
		})
	})
})
