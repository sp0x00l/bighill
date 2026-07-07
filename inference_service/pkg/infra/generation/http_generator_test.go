package generation_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"inference_service/pkg/domain/model"
	"inference_service/pkg/infra/generation"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestGeneration(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Inference generation unit test suite")
}

var _ = Describe("HTTPGenerator", func() {
	It("calls Ollama with the built prompt", func() {
		var received struct {
			Model  string `json:"model"`
			Prompt string `json:"prompt"`
			Stream bool   `json:"stream"`
		}
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			Expect(r.URL.Path).To(Equal("/api/generate"))
			Expect(json.NewDecoder(r.Body).Decode(&received)).To(Succeed())
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"response":"generated from ollama"}`))
		}))
		defer server.Close()
		generator := generation.NewHTTPGenerator("ollama", server.URL, "llama3.1:8b", time.Second)

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
		generator := generation.NewHTTPGenerator("tei", "http://localhost:8080", "model", time.Second)

		_, err := generator.Generate(context.Background(), model.GenerationRequest{
			Query: "question",
		})

		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("unsupported generation provider"))
	})

	It("requires callers to provide an already built prompt", func() {
		generator := generation.NewHTTPGenerator("ollama", "http://ollama.local", "llama3.1:8b", time.Second)

		_, err := generator.Generate(context.Background(), model.GenerationRequest{
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
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			Expect(r.URL.Path).To(Equal("/v1/chat/completions"))
			Expect(json.NewDecoder(r.Body).Decode(&received)).To(Succeed())
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"generated from vllm"}}]}`))
		}))
		defer server.Close()
		generator := generation.NewHTTPGenerator("vllm", server.URL, "base-model", time.Second)

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
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			Expect(r.URL.Path).To(Equal("/v1/chat/completions"))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"generated from served model"}}]}`))
		}))
		defer server.Close()
		generator := generation.NewHTTPGenerator("vllm", "http://fallback-vllm.local", "base-model", time.Second)

		answer, err := generator.Generate(context.Background(), model.GenerationRequest{
			Query:  "question",
			Prompt: "prompt text",
			Model: &model.InferenceModel{
				ServingTarget: server.URL,
				ServingModel:  "movie-ranker-lora",
			},
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(answer).To(Equal("generated from served model"))
	})
})
