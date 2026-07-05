package domain

import (
	"errors"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestDomain(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Training domain unit test suite")
}

var _ = Describe("ServiceError", func() {
	It("extends and matches by code", func() {
		err := ErrTrainModel.Extend("ray job failed")

		Expect(err.Error()).To(Equal("train model failed: ray job failed"))
		Expect(errors.Is(err, ErrTrainModel)).To(BeTrue())
	})

	It("does not match unrelated service errors", func() {
		Expect(errors.Is(ErrPrepareDataset, ErrTrainModel)).To(BeFalse())
		Expect(errors.Is(errors.New("plain"), ErrTrainModel)).To(BeFalse())
	})
})
