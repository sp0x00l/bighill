package generation_test

import (
	"context"
	"testing"

	"inference_service/pkg/domain/model"
	"inference_service/pkg/infra/generation"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestGeneration(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Inference generation unit test suite")
}

var _ = Describe("DeterministicGenerator", func() {
	It("generates from the highest ranked context", func() {
		generator := generation.NewDeterministicGenerator()

		answer, err := generator.Generate(context.Background(), model.GenerationRequest{
			Query: "what is the policy?",
			Contexts: []model.RetrievedContext{{
				SourceText: "The policy is stored in the registry.",
			}},
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(answer).To(ContainSubstring("The policy is stored in the registry."))
	})

	It("handles empty retrieval results", func() {
		generator := generation.NewDeterministicGenerator()

		answer, err := generator.Generate(context.Background(), model.GenerationRequest{
			Query: "what is the policy?",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(answer).To(ContainSubstring("No relevant context"))
	})
})
