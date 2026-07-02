package model_test

import (
	"testing"

	"model_registry_service/pkg/domain/model"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestModel(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Model registry model unit test suite")
}

var _ = Describe("ModelStatus", func() {
	It("converts known statuses", func() {
		status, err := model.ToModelStatus("READY")

		Expect(err).NotTo(HaveOccurred())
		Expect(status).To(Equal(model.ModelStatusReady))
		Expect(model.ModelStatusPending.String()).To(Equal("PENDING"))
		Expect(model.ModelStatusCandidate.String()).To(Equal("CANDIDATE"))
		Expect(model.ModelStatusEvaluated.String()).To(Equal("EVALUATED"))
		Expect(model.ModelStatusFailed.String()).To(Equal("FAILED"))
	})

	It("rejects unknown statuses", func() {
		_, err := model.ToModelStatus("UNKNOWN")

		Expect(err).To(HaveOccurred())
	})
})

var _ = Describe("ModelLoadStatus", func() {
	It("converts known load statuses", func() {
		status, err := model.ToModelLoadStatus("LOADED")

		Expect(err).NotTo(HaveOccurred())
		Expect(status).To(Equal(model.ModelLoadStatusLoaded))
		Expect(model.ModelLoadStatusNotLoaded.String()).To(Equal("NOT_LOADED"))
		Expect(model.ModelLoadStatusFailed.String()).To(Equal("FAILED"))
	})

	It("rejects unknown load statuses", func() {
		_, err := model.ToModelLoadStatus("UNKNOWN")

		Expect(err).To(HaveOccurred())
	})
})
