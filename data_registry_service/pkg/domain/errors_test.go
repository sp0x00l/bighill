package domain_test

import (
	"errors"
	"testing"

	domainErrors "data_registry_service/pkg/domain"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestDomain(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Data registry domain unit test suite")
}

var _ = Describe("ServiceError", func() {
	It("matches errors by service error code", func() {
		err := domainErrors.ErrValidationFailed.Extend("invalid dataset")

		Expect(errors.Is(err, domainErrors.ErrValidationFailed)).To(BeTrue())
		Expect(domainErrors.IsServiceError(err, domainErrors.ErrValidationFailed)).To(BeTrue())
		Expect(domainErrors.IsServiceError(err, domainErrors.ErrResourceNotFound)).To(BeFalse())
	})

	It("preserves the original service error when extended", func() {
		err := domainErrors.ErrResourceNotFound.Extend("dataset not found")

		Expect(err.Code).To(Equal(domainErrors.ErrResourceNotFound.Code))
		Expect(err.Error()).To(Equal("dataset not found"))
		Expect(errors.Is(err, domainErrors.ErrResourceNotFound)).To(BeTrue())
	})
})
