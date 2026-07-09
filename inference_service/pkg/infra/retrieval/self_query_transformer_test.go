package retrieval_test

import (
	"context"
	"errors"

	"inference_service/pkg/domain/model"
	"inference_service/pkg/infra/retrieval"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type queryGeneratorStub struct {
	request model.GenerationRequest
	answer  string
	err     error
}

func (s *queryGeneratorStub) Generate(_ context.Context, request model.GenerationRequest) (string, error) {
	s.request = request
	if s.err != nil {
		return "", s.err
	}
	return s.answer, nil
}

var _ = Describe("SelfQueryTransformer", func() {
	It("maps structured generator output into a retrieval query", func() {
		generator := &queryGeneratorStub{answer: "```json\n{\"query\":\"semantic risk query\",\"filters\":{\"section\":\"risk\",\"tenant_id\":\"other\"}}\n```"}
		transformer := retrieval.NewSelfQueryTransformer(map[string]retrieval.QueryGenerator{
			model.ServingProtocolOpenAIChatCompletions.String(): generator,
		})
		requestID := uuid.New()
		inferenceModel := &model.InferenceModel{
			ServingProtocol: model.ServingProtocolOpenAIChatCompletions,
			ServingTarget:   "http://served-model.local",
			ServingModel:    "served-model",
		}

		result, err := transformer.TransformQuery(context.Background(), model.QueryTransformRequest{
			RequestID: requestID,
			Model:     inferenceModel,
			QueryText: "Show risks in the filing",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(generator.request.RequestID).To(Equal(requestID))
		Expect(generator.request.Model).To(Equal(inferenceModel))
		Expect(generator.request.Prompt).To(ContainSubstring("Return only a JSON object"))
		Expect(result.QueryText).To(Equal("semantic risk query"))
		Expect(result.MetadataFilters).To(Equal(map[string]string{"section": "risk"}))
	})

	It("rejects malformed generator output", func() {
		transformer := retrieval.NewSelfQueryTransformer(map[string]retrieval.QueryGenerator{
			model.ServingProtocolOpenAIChatCompletions.String(): &queryGeneratorStub{answer: "not json"},
		})

		result, err := transformer.TransformQuery(context.Background(), model.QueryTransformRequest{
			QueryText: "query",
			Model: &model.InferenceModel{
				ServingProtocol: model.ServingProtocolOpenAIChatCompletions,
				ServingTarget:   "http://served-model.local",
				ServingModel:    "served-model",
			},
		})

		Expect(result).To(BeNil())
		Expect(err).To(HaveOccurred())
	})

	It("returns generator errors", func() {
		expected := errors.New("generator unavailable")
		transformer := retrieval.NewSelfQueryTransformer(map[string]retrieval.QueryGenerator{
			model.ServingProtocolOpenAIChatCompletions.String(): &queryGeneratorStub{err: expected},
		})

		result, err := transformer.TransformQuery(context.Background(), model.QueryTransformRequest{
			QueryText: "query",
			Model: &model.InferenceModel{
				ServingProtocol: model.ServingProtocolOpenAIChatCompletions,
				ServingTarget:   "http://served-model.local",
				ServingModel:    "served-model",
			},
		})

		Expect(result).To(BeNil())
		Expect(err).To(MatchError(expected))
	})
})
