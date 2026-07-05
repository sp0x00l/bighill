package domain

import (
	"errors"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestDomain(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Model serving domain unit test suite")
}

var _ = Describe("ServiceError", func() {
	It("extends and matches by code", func() {
		err := ErrModelServe.Extend("runtime unavailable")

		Expect(err.Error()).To(Equal("model serve failed: runtime unavailable"))
		Expect(errors.Is(err, ErrModelServe)).To(BeTrue())
	})

	It("does not match unrelated service errors", func() {
		Expect(errors.Is(ErrValidationFailed, ErrModelServe)).To(BeFalse())
		Expect(errors.Is(errors.New("plain"), ErrModelServe)).To(BeFalse())
	})
})
