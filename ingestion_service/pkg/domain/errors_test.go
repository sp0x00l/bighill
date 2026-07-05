package domain

import (
	"errors"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestDomain(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Ingestion domain unit test suite")
}

var _ = Describe("ServiceError", func() {
	It("extends and matches by service error code", func() {
		err := ErrForbidden.Extend("wrong tenant")

		Expect(err.Error()).To(Equal("wrong tenant"))
		Expect(IsServiceError(err, ErrForbidden)).To(BeTrue())
		Expect(errors.Is(err, ErrForbidden)).To(BeTrue())
	})

	It("does not match unrelated errors", func() {
		Expect(errors.Is(errors.New("plain"), ErrForbidden)).To(BeFalse())
	})
})
