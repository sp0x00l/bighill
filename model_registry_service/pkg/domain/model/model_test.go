package model_test

import (
	"model_registry_service/pkg/domain/model"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Model", func() {
	It("normalizes default identity and metadata", func() {
		registeredModel := &model.Model{}

		model.NormalizeModel(registeredModel)

		Expect(registeredModel.ModelID.String()).NotTo(BeEmpty())
		Expect(registeredModel.Name).To(HavePrefix("model_"))
		Expect(registeredModel.ModelVersion).To(Equal(1))
		Expect(registeredModel.MetricsMetadata).To(Equal("{}"))
	})
})
