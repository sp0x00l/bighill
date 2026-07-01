package app_test

import (
	"context"
	"errors"
	"testing"

	"inference_service/pkg/app"
	"inference_service/pkg/domain"
	"inference_service/pkg/domain/model"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestApp(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Inference service app unit test suite")
}

var _ = Describe("InferenceUsecase", func() {
	It("accepts a complete model registration", func() {
		uc := app.NewInferenceUsecase()

		err := uc.RegisterModel(context.Background(), model.InferenceModel{
			ModelID:      "model-1",
			ModelName:    "sentence-transformer",
			ModelVersion: "local-dev",
			ModelURI:     "s3://local-dev-bucket/models/model-1",
		})

		Expect(err).NotTo(HaveOccurred())
	})

	It("rejects missing model identity", func() {
		uc := app.NewInferenceUsecase()

		err := uc.RegisterModel(context.Background(), model.InferenceModel{})

		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
	})
})
