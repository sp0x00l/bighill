package domain

import (
	"errors"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestDomain(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Data stream domain unit test suite")
}

var _ = Describe("SourceType", func() {
	It("converts known source type strings", func() {
		sourceType, err := ToSourceType("POSTGRES")

		Expect(err).NotTo(HaveOccurred())
		Expect(sourceType).To(Equal(SourceTypePostgres))
		Expect(SourceTypeS3.String()).To(Equal("S3"))
	})

	It("rejects unknown source type strings", func() {
		_, err := ToSourceType("SALESFORCE")

		Expect(err).To(HaveOccurred())
	})
})

var _ = Describe("ServiceError", func() {
	It("extends and matches service errors by code", func() {
		err := ErrValidationFailed.Extend("bad query")

		Expect(err.Error()).To(Equal("bad query"))
		Expect(IsServiceError(err, ErrValidationFailed)).To(BeTrue())
		Expect(errors.Is(err, ErrValidationFailed)).To(BeTrue())
	})

	It("does not match unrelated errors", func() {
		Expect(errors.Is(errors.New("plain"), ErrValidationFailed)).To(BeFalse())
	})
})
