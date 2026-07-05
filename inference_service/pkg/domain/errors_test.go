package domain

import (
	"errors"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestDomain(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Inference domain unit test suite")
}

var _ = Describe("ServiceError", func() {
	It("extends and matches by service error code", func() {
		err := ErrModelNotReady.Extend("warming")

		Expect(err.Error()).To(Equal("model not ready: warming"))
		Expect(errors.Is(err, ErrModelNotReady)).To(BeTrue())
	})

	It("does not match a different service error code", func() {
		err := ErrModelMismatch.Extend("wrong dataset")

		Expect(errors.Is(err, ErrModelNotReady)).To(BeFalse())
		Expect(errors.Is(errors.New("plain"), ErrModelNotReady)).To(BeFalse())
	})
})
