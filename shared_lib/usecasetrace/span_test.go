package usecasetrace_test

import (
	"context"
	"errors"
	"testing"

	sharedtrace "lib/shared_lib/trace"
	usecasetrace "lib/shared_lib/usecasetrace"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestUsecaseTrace(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "UsecaseTrace Suite")
}

var _ = Describe("StartSpanWithExpectedErrors", func() {
	It("adds the expected error classifier to the returned context", func() {
		expected := errors.New("expected")
		unexpected := errors.New("unexpected")

		ctx, span := usecasetrace.StartSpanWithExpectedErrors(context.Background(), "test", "test.span", func(err error) bool {
			return errors.Is(err, expected)
		})
		defer span.End()

		Expect(sharedtrace.IsExpectedSpanError(ctx, expected)).To(BeTrue())
		Expect(sharedtrace.IsExpectedSpanError(ctx, unexpected)).To(BeFalse())
	})
})
