package domain

import (
	"errors"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestDomain(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Model registry domain unit test suite")
}

var _ = Describe("Domain errors", func() {
	It("matches exported sentinel errors", func() {
		Expect(errors.Is(ErrModelNotFound, ErrModelNotFound)).To(BeTrue())
		Expect(errors.Is(ErrModelExists, ErrModelExists)).To(BeTrue())
	})

	It("does not match unrelated errors", func() {
		Expect(errors.Is(errors.New("plain"), ErrModelServe)).To(BeFalse())
	})
})
