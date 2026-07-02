package generation_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
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
		Expect(generator.Provider()).To(Equal("deterministic"))
		Expect(generator.Model()).To(Equal("deterministic"))
	})

	It("rejects empty retrieval results", func() {
		generator := generation.NewDeterministicGenerator()

		answer, err := generator.Generate(context.Background(), model.GenerationRequest{
			Query: "what is the policy?",
		})

		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("retrieved context is required"))
		Expect(answer).To(BeEmpty())
	})
})

var _ = Describe("HTTPGenerator", func() {
	It("calls Ollama with the built prompt", func() {
		var received struct {
			Model  string `json:"model"`
			Prompt string `json:"prompt"`
			Stream bool   `json:"stream"`
		}
		generator, err := generation.NewHTTPGeneratorWithClient("ollama", "http://ollama.local", "llama3.1:8b", 0, &http.Client{
			Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				Expect(r.URL.String()).To(Equal("http://ollama.local/api/generate"))
				Expect(json.NewDecoder(r.Body).Decode(&received)).To(Succeed())
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewBufferString(`{"response":"generated from ollama"}`)),
					Header:     make(http.Header),
				}, nil
			}),
		})
		Expect(err).NotTo(HaveOccurred())

		answer, err := generator.Generate(context.Background(), model.GenerationRequest{
			Query:  "question",
			Prompt: "prompt text",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(answer).To(Equal("generated from ollama"))
		Expect(received.Model).To(Equal("llama3.1:8b"))
		Expect(received.Prompt).To(Equal("prompt text"))
		Expect(received.Stream).To(BeFalse())
		Expect(generator.Provider()).To(Equal("ollama"))
		Expect(generator.Model()).To(Equal("llama3.1:8b"))
	})

	It("rejects unsupported providers", func() {
		generator, err := generation.NewHTTPGenerator("tei", "http://localhost:8080", "model", 0)
		Expect(err).NotTo(HaveOccurred())

		_, err = generator.Generate(context.Background(), model.GenerationRequest{
			Query: "question",
		})

		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("unsupported generation provider"))
	})

	It("requires callers to provide an already built prompt", func() {
		generator, err := generation.NewHTTPGenerator("ollama", "http://ollama.local", "llama3.1:8b", 0)
		Expect(err).NotTo(HaveOccurred())

		_, err = generator.Generate(context.Background(), model.GenerationRequest{
			Query: "question",
			Contexts: []model.RetrievedContext{{
				SourceText: "context",
			}},
		})

		Expect(err).To(MatchError("prompt is required"))
	})

	It("calls vLLM through the OpenAI-compatible chat completions API", func() {
		var received struct {
			Model    string `json:"model"`
			Messages []struct {
				Role    string `json:"role"`
				Content string `json:"content"`
			} `json:"messages"`
			Temperature float64 `json:"temperature"`
		}
		generator, err := generation.NewHTTPGeneratorWithClient("vllm", "http://vllm.local", "base-model", 0, &http.Client{
			Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				Expect(r.URL.String()).To(Equal("http://vllm.local/v1/chat/completions"))
				Expect(json.NewDecoder(r.Body).Decode(&received)).To(Succeed())
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewBufferString(`{"choices":[{"message":{"content":"generated from vllm"}}]}`)),
					Header:     make(http.Header),
				}, nil
			}),
		})
		Expect(err).NotTo(HaveOccurred())

		answer, err := generator.Generate(context.Background(), model.GenerationRequest{
			Query:  "question",
			Prompt: "prompt text",
			Model: &model.InferenceModel{
				ServingModel: "movie-ranker-lora",
			},
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(answer).To(Equal("generated from vllm"))
		Expect(received.Model).To(Equal("movie-ranker-lora"))
		Expect(received.Messages).To(HaveLen(1))
		Expect(received.Messages[0].Role).To(Equal("user"))
		Expect(received.Messages[0].Content).To(Equal("prompt text"))
		Expect(received.Temperature).To(Equal(0.0))
		Expect(generator.Provider()).To(Equal("vllm"))
		Expect(generator.Model()).To(Equal("base-model"))
	})

	It("routes vLLM requests to the model serving target when present", func() {
		generator, err := generation.NewHTTPGeneratorWithClient("vllm", "http://fallback-vllm.local", "base-model", 0, &http.Client{
			Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				Expect(r.URL.String()).To(Equal("http://served-model.default.svc.cluster.local:8000/v1/chat/completions"))
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewBufferString(`{"choices":[{"message":{"content":"generated from served model"}}]}`)),
					Header:     make(http.Header),
				}, nil
			}),
		})
		Expect(err).NotTo(HaveOccurred())

		answer, err := generator.Generate(context.Background(), model.GenerationRequest{
			Query:  "question",
			Prompt: "prompt text",
			Model: &model.InferenceModel{
				ServingTarget: "http://served-model.default.svc.cluster.local:8000",
				ServingModel:  "movie-ranker-lora",
			},
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(answer).To(Equal("generated from served model"))
	})
})

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
