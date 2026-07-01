package model_test

import (
	"testing"

	"inference_service/pkg/domain/model"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestModel(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Inference service model unit test suite")
}

var _ = Describe("ModelStatus", func() {
	It("converts known model statuses", func() {
		status, err := model.ToModelStatus("READY")

		Expect(err).NotTo(HaveOccurred())
		Expect(status).To(Equal(model.ModelStatusReady))
		Expect(model.ModelStatusPending.String()).To(Equal("PENDING"))
		Expect(model.ModelStatusFailed.String()).To(Equal("FAILED"))
	})

	It("rejects unknown model statuses", func() {
		_, err := model.ToModelStatus("UNKNOWN")

		Expect(err).To(HaveOccurred())
	})
})
