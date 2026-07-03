package ctxutil_test

import (
	"context"
	"errors"
	"lib/shared_lib/ctxutil"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestCtxutil(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Context utility unit test suite")
}

var _ = Describe("IsCanceled", func() {
	It("matches context cancellation errors", func() {
		Expect(ctxutil.IsCanceled(context.Canceled)).To(BeTrue())
		Expect(ctxutil.IsCanceled(context.DeadlineExceeded)).To(BeTrue())
		Expect(ctxutil.IsCanceled(errors.New("other"))).To(BeFalse())
	})
})
