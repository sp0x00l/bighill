package trace

import (
	"context"
	"errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Expected span error classifier", func() {
	It("composes classifiers", func() {
		errA := errors.New("a")
		errB := errors.New("b")
		errC := errors.New("c")

		ctx := ContextWithExpectedErrorClassifier(context.Background(), func(err error) bool {
			return errors.Is(err, errA)
		})
		ctx = ContextWithExpectedErrorClassifier(ctx, func(err error) bool {
			return errors.Is(err, errB)
		})

		Expect(IsExpectedSpanError(ctx, errA)).To(BeTrue())
		Expect(IsExpectedSpanError(ctx, errB)).To(BeTrue())
		Expect(IsExpectedSpanError(ctx, errC)).To(BeFalse())
	})
})
