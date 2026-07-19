package generation

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"inference_service/pkg/domain/model"

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
			Model   string `json:"model"`
			Prompt  string `json:"prompt"`
			Stream  bool   `json:"stream"`
			Options struct {
				NumPredict int `json:"num_predict"`
			} `json:"options"`
		}
		generator := NewHTTPGenerator("OLLAMA_GENERATE", time.Second, 128)
		generator.client = httpGeneratorTestClient(`{"response":"generated from ollama"}`, func(r *http.Request) {
			Expect(r.URL.Host).To(Equal("ollama.local"))
			Expect(r.URL.Path).To(Equal("/api/generate"))
			Expect(json.NewDecoder(r.Body).Decode(&received)).To(Succeed())
		})

		result, err := generator.Generate(context.Background(), model.GenerationRequest{
			Query:  "question",
			Prompt: "prompt text",
			Model: &model.InferenceModel{
				ServingTarget: "http://ollama.local",
				ServingModel:  "local-test-model:latest",
			},
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(result.Content).To(Equal("generated from ollama"))
		Expect(received.Model).To(Equal("local-test-model:latest"))
		Expect(received.Prompt).To(Equal("prompt text"))
		Expect(received.Stream).To(BeFalse())
		Expect(received.Options.NumPredict).To(Equal(128))
	})

	It("rejects unsupported protocols", func() {
		generator := NewHTTPGenerator("tei", time.Second, 128)

		_, err := generator.Generate(context.Background(), model.GenerationRequest{
			Query: "question",
		})

		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("unsupported serving protocol"))
	})

	It("requires callers to provide an already built prompt", func() {
		generator := NewHTTPGenerator("OLLAMA_GENERATE", time.Second, 128)

		_, err := generator.Generate(context.Background(), model.GenerationRequest{
			Query: "question",
			Contexts: []model.RetrievedContext{{
				SourceText: "context",
			}},
		})

		Expect(err).To(MatchError("prompt is required"))
	})

	It("requires the selected model to provide a serving model name", func() {
		generator := NewHTTPGenerator("OLLAMA_GENERATE", time.Second, 128)

		_, err := generator.Generate(context.Background(), model.GenerationRequest{
			Query:  "question",
			Prompt: "prompt text",
			Model:  &model.InferenceModel{},
		})

		Expect(err).To(MatchError("served model name is required"))
	})

	It("calls an OpenAI-compatible chat completions endpoint", func() {
		var received struct {
			Model    string `json:"model"`
			Messages []struct {
				Role    string `json:"role"`
				Content string `json:"content"`
			} `json:"messages"`
			Temperature float64 `json:"temperature"`
			MaxTokens   int     `json:"max_tokens"`
		}
		generator := NewHTTPGenerator("OPENAI_CHAT_COMPLETIONS", time.Second, 96)
		generator.client = httpGeneratorTestClient(`{"choices":[{"message":{"content":"generated from vllm"}}]}`, func(r *http.Request) {
			Expect(r.URL.Host).To(Equal("vllm.local"))
			Expect(r.URL.Path).To(Equal("/v1/chat/completions"))
			Expect(json.NewDecoder(r.Body).Decode(&received)).To(Succeed())
		})

		result, err := generator.Generate(context.Background(), model.GenerationRequest{
			Query:  "question",
			Prompt: "prompt text",
			Model: &model.InferenceModel{
				ServingTarget: "http://vllm.local",
				ServingModel:  "movie-ranker-lora",
			},
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(result.Content).To(Equal("generated from vllm"))
		Expect(received.Model).To(Equal("movie-ranker-lora"))
		Expect(received.Messages).To(HaveLen(1))
		Expect(received.Messages[0].Role).To(Equal("user"))
		Expect(received.Messages[0].Content).To(Equal("prompt text"))
		Expect(received.Temperature).To(Equal(0.0))
		Expect(received.MaxTokens).To(Equal(96))
	})

	It("routes chat completions requests to the model serving target", func() {
		generator := NewHTTPGenerator("OPENAI_CHAT_COMPLETIONS", time.Second, 128)
		generator.client = httpGeneratorTestClient(`{"choices":[{"message":{"content":"generated from served model"}}]}`, func(r *http.Request) {
			Expect(r.URL.Host).To(Equal("served-vllm.local"))
			Expect(r.URL.Path).To(Equal("/v1/chat/completions"))
		})

		result, err := generator.Generate(context.Background(), model.GenerationRequest{
			Query:  "question",
			Prompt: "prompt text",
			Model: &model.InferenceModel{
				ServingTarget: "http://served-vllm.local",
				ServingModel:  "movie-ranker-lora",
			},
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(result.Content).To(Equal("generated from served model"))
	})

	It("uses the selected LoRA name as the OpenAI-compatible model", func() {
		var received struct {
			Model string `json:"model"`
		}
		generator := NewHTTPGenerator("OPENAI_CHAT_COMPLETIONS", time.Second, 128)
		generator.client = httpGeneratorTestClient(`{"choices":[{"message":{"content":"generated from lora"}}]}`, func(r *http.Request) {
			Expect(r.URL.Host).To(Equal("base-vllm.local"))
			Expect(r.URL.Path).To(Equal("/v1/chat/completions"))
			Expect(json.NewDecoder(r.Body).Decode(&received)).To(Succeed())
		})

		result, err := generator.Generate(context.Background(), model.GenerationRequest{
			Query:  "question",
			Prompt: "prompt text",
			Model: &model.InferenceModel{
				ServingTarget: "http://base-vllm.local",
				ServingModel:  "base-model",
			},
			LoraName: "candidate-agent-lora",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(result.Content).To(Equal("generated from lora"))
		Expect(received.Model).To(Equal("candidate-agent-lora"))
	})

	It("does not serialize tool provenance into the model-facing OpenAI tool payload", func() {
		var received map[string]any
		generator := NewHTTPGenerator("OPENAI_CHAT_COMPLETIONS", time.Second, 128)
		generator.client = httpGeneratorTestClient(`{"choices":[{"message":{"content":"generated with tool schema"}}]}`, func(r *http.Request) {
			body, err := io.ReadAll(r.Body)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(body)).NotTo(ContainSubstring("implementation_version"))
			Expect(string(body)).NotTo(ContainSubstring("locality"))
			Expect(json.Unmarshal(body, &received)).To(Succeed())
		})

		result, err := generator.Generate(context.Background(), model.GenerationRequest{
			Model: &model.InferenceModel{
				ServingTarget: "http://vllm.local",
				ServingModel:  "tool-model",
			},
			Messages: []model.ChatMessage{{
				Role:    model.ChatMessageRoleUser,
				Content: "use a tool",
			}},
			Tools: []model.ToolSpec{{
				Name:                  "search_knowledge",
				Description:           "Search tenant knowledge.",
				Parameters:            []byte(`{"type":"object","properties":{"query":{"type":"string"}}}`),
				ImplementationVersion: "search_knowledge:v1",
				Locality:              "local",
			}},
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(result.Content).To(Equal("generated with tool schema"))
		tools, ok := received["tools"].([]any)
		Expect(ok).To(BeTrue())
		Expect(tools).To(HaveLen(1))
		tool := tools[0].(map[string]any)
		Expect(tool).To(HaveKeyWithValue("type", "function"))
		function := tool["function"].(map[string]any)
		Expect(function).To(HaveKeyWithValue("name", "search_knowledge"))
		Expect(function).NotTo(HaveKey("implementation_version"))
		Expect(function).NotTo(HaveKey("locality"))
	})
})

type httpGeneratorRoundTripFunc func(*http.Request) (*http.Response, error)

func (f httpGeneratorRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func httpGeneratorTestClient(payload string, assert func(*http.Request)) *http.Client {
	return &http.Client{Transport: httpGeneratorRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		if assert != nil {
			assert(req)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(payload)),
			Header:     make(http.Header),
		}, nil
	})}
}
