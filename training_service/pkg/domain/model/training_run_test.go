package model_test

import (
	"testing"

	"training_service/pkg/domain/model"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestModel(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Training service model unit test suite")
}

var _ = Describe("TrainingRunStatus", func() {
	It("converts known statuses", func() {
		status, err := model.ToTrainingRunStatus("COMPLETED")

		Expect(err).NotTo(HaveOccurred())
		Expect(status).To(Equal(model.TrainingRunStatusCompleted))
		Expect(model.TrainingRunStatusTraining.String()).To(Equal("TRAINING"))
	})

	It("rejects unknown statuses", func() {
		_, err := model.ToTrainingRunStatus("UNKNOWN")

		Expect(err).To(HaveOccurred())
	})
})
