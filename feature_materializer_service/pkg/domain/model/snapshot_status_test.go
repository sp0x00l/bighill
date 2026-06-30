package model_test

import (
	"testing"

	"feature_materializer_service/pkg/domain/model"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestModel(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Feature materializer model unit test suite")
}

var _ = Describe("SnapshotStatus", func() {
	It("converts known statuses", func() {
		status, err := model.ToSnapshotStatus("PENDING")
		Expect(err).NotTo(HaveOccurred())
		Expect(status).To(Equal(model.SnapshotStatusPending))
		Expect(model.SnapshotStatusReady.String()).To(Equal("READY"))
		Expect(model.SnapshotStatusFailed.String()).To(Equal("FAILED"))
	})

	It("rejects unknown statuses", func() {
		_, err := model.ToSnapshotStatus("UNKNOWN")
		Expect(err).To(HaveOccurred())
	})
})
