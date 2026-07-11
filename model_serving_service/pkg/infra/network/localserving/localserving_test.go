package localserving

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	localstore "lib/shared_lib/servedmodel"
	"model_serving_service/pkg/app"
	"model_serving_service/pkg/domain"
	"model_serving_service/pkg/domain/model"
	servingkubernetes "model_serving_service/pkg/infra/network/k8s"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/watch"
)

func TestLocalServing(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Model serving local serving unit test suite")
}

func newTestRuntime(endpoint string, options ...RuntimeOption) *Runtime {
	testOptions := []RuntimeOption{
		WithArtifactCache(GinkgoT().TempDir()),
		WithGGUFInspectorCommand("sh -c true"),
		WithCreateTimeout(time.Second),
	}
	testOptions = append(testOptions, options...)
	runtime, err := NewRuntime("default", 8080, endpoint, testOptions...)
	Expect(err).NotTo(HaveOccurred())
	return runtime
}

var _ = Describe("Runtime", func() {
	It("rejects incomplete runtime config at the infra boundary", func() {
		_, err := NewRuntime("", 8080, "http://ollama.local",
			WithArtifactCache(GinkgoT().TempDir()),
			WithGGUFInspectorCommand("sh -c true"),
			WithCreateTimeout(time.Second),
		)
		Expect(err).To(MatchError(ContainSubstring("local serving namespace is required")))

		_, err = NewRuntime("default", 8080, "http://ollama.local",
			WithArtifactCache(""),
			WithGGUFInspectorCommand("sh -c true"),
			WithCreateTimeout(time.Second),
		)
		Expect(err).To(MatchError(ContainSubstring("local artifact cache is required")))

		_, err = NewRuntime("default", 8080, "http://ollama.local",
			WithArtifactCache(GinkgoT().TempDir()),
			WithGGUFInspectorCommand(""),
			WithCreateTimeout(time.Second),
		)
		Expect(err).To(MatchError(ContainSubstring("GGUF inspector command is required")))
	})

	It("returns a ready local runtime state for base-backed served models", func() {
		modelID := uuid.New()
		runtime := newTestRuntime("http://ollama.local")
		runtime.client = &http.Client{Transport: newLocalOllamaTagsTransport(`{"models":[{"name":"local-test-model:latest"}]}`, func(req *http.Request) {
			Expect(req.URL.Path).To(Equal("/api/tags"))
		})}

		state, err := runtime.EnsureServedModel(context.Background(), &model.ServedModel{
			ModelID:      modelID,
			ModelKind:    "BASE",
			Name:         "llama",
			ModelVersion: 2,
			BaseModel:    "local-test-model:latest",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(state.Ready).To(BeTrue())
		Expect(state.ServingTarget).To(Equal("http://ollama.local"))
		Expect(state.ServingModel).To(Equal("local-test-model:latest"))
		Expect(state.ServingProtocol).To(Equal(model.ServingProtocolOllamaGenerate))
		Expect(state.ReadyReplicas).To(Equal(int32(1)))
	})

	It("matches Ollama tags that omit the explicit latest suffix", func() {
		runtime := newTestRuntime("http://ollama.local")
		runtime.client = &http.Client{Transport: newLocalOllamaTagsTransport(`{"models":[{"name":"llama3.1:latest"}]}`, nil)}

		state, err := runtime.EnsureServedModel(context.Background(), &model.ServedModel{
			ModelID:      uuid.New(),
			ModelKind:    "BASE",
			Name:         "llama",
			ModelVersion: 1,
			BaseModel:    "llama3.1",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(state.ServingModel).To(Equal("llama3.1"))
	})

	It("treats major local model families as runtime data, not provider or protocol variants", func() {
		families := []string{
			"local-test-model:latest",
			"mistral:7b",
			"qwen2.5:7b",
			"deepseek-r1:7b",
			"gemma3:4b",
		}

		for _, baseModel := range families {
			runtime := newTestRuntime("http://ollama.local")
			runtime.client = &http.Client{Transport: newLocalOllamaTagsTransport(`{"models":[{"name":"`+baseModel+`"}]}`, nil)}

			state, err := runtime.EnsureServedModel(context.Background(), &model.ServedModel{
				ModelID:      uuid.New(),
				ModelKind:    "BASE",
				Name:         "base",
				ModelVersion: 1,
				BaseModel:    baseModel,
			})

			Expect(err).NotTo(HaveOccurred(), baseModel)
			Expect(state.ServingModel).To(Equal(baseModel), baseModel)
			Expect(state.ServingProtocol).To(Equal(model.ServingProtocolOllamaGenerate), baseModel)
		}
	})

	It("fails closed instead of defaulting fine-tuned models to the base model", func() {
		state, err := newTestRuntime("http://ollama.local").EnsureServedModel(context.Background(), &model.ServedModel{
			ModelID:      uuid.New(),
			ModelKind:    "FINE_TUNED",
			Name:         "fine-tune",
			ModelVersion: 1,
			BaseModel:    "local-test-model:latest",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(state.Ready).To(BeFalse())
		Expect(state.Failed).To(BeTrue())
		Expect(state.FailureReason).To(ContainSubstring("fine-tuned model has no adapter URI"))
	})

	It("fails closed when a local fine-tuned adapter rank is unknown", func() {
		state, err := newTestRuntime("http://ollama.local").EnsureServedModel(context.Background(), &model.ServedModel{
			ModelID:      uuid.New(),
			ModelKind:    "FINE_TUNED",
			Name:         "fine-tune",
			ModelVersion: 1,
			BaseModel:    "local-test-model:latest",
			AdapterURI:   "s3://bucket/adapter.gguf",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(state.Ready).To(BeFalse())
		Expect(state.Failed).To(BeTrue())
		Expect(state.FailureReason).To(ContainSubstring("unknown adapter rank"))
	})

	It("rejects base models that are not loaded in local Ollama", func() {
		runtime := newTestRuntime("http://ollama.local")
		runtime.client = &http.Client{Transport: newLocalOllamaTagsTransport(`{"models":[{"name":"other-model"}]}`, nil)}

		_, err := runtime.EnsureServedModel(context.Background(), &model.ServedModel{
			ModelID:      uuid.New(),
			ModelKind:    "BASE",
			Name:         "llama",
			ModelVersion: 1,
			BaseModel:    "local-test-model:latest",
		})

		Expect(err).To(MatchError(ContainSubstring(`local ollama model "local-test-model:latest" is not available`)))
		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
	})

	It("rejects served models with no base model", func() {
		_, err := newTestRuntime("http://ollama.local").EnsureServedModel(context.Background(), &model.ServedModel{})

		Expect(err).To(MatchError(ContainSubstring("base model is required")))
		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
	})

	It("creates a GGUF chat model through Ollama blobs only after the chat template validator passes", func() {
		artifactPath, checksum, localS3Root := writeLocalS3Artifact("local-dev-bucket", "models/model.gguf", []byte("gguf bytes"))
		_ = artifactPath
		inspector := writeInspectorScript(GinkgoT().TempDir(), 0, llama3InspectionJSON())
		transport := &ollamaCreateTransport{
			existingTags:     map[string]bool{},
			inferredTemplate: "{{ range .Messages }}{{ .Content }}{{ end }}",
		}
		runtime := newTestRuntime("http://ollama.local",
			WithLocalS3Dir(localS3Root),
			WithArtifactCache(GinkgoT().TempDir()),
			WithGGUFInspectorCommand(inspector),
		)
		runtime.client = &http.Client{Transport: transport}
		modelID := uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")

		state, err := runtime.EnsureServedModel(context.Background(), &model.ServedModel{
			ModelID:          modelID,
			ModelKind:        "BASE",
			Name:             "Meta Llama 3 Instruct",
			ModelVersion:     7,
			BaseModel:        "meta-llama/Meta-Llama-3-8B-Instruct",
			ArtifactLocation: "s3://local-dev-bucket/models/model.gguf",
			ArtifactFormat:   "GGUF_MODEL",
			ArtifactChecksum: checksum,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(state.Ready).To(BeTrue())
		Expect(state.ServingProtocol).To(Equal(model.ServingProtocolOpenAIChatCompletions))
		Expect(state.ServingModel).To(Equal("bighill-meta-llama-3-instruct-v7-aaaaaaaa-" + checksumTagPart(checksum)))
		Expect(transport.blobUploads).To(Equal(1))
		Expect(transport.createRequests).To(HaveLen(1))
		Expect(transport.createRequests[0]["model"]).To(Equal(state.ServingModel))
		Expect(transport.createRequests[0]).To(HaveKey("files"))
		Expect(transport.createRequests[0]).NotTo(HaveKey("template"))
		parameters, ok := transport.createRequests[0]["parameters"].(map[string]any)
		Expect(ok).To(BeTrue())
		Expect(parameters["stop"]).To(ConsistOf("<|eot_id|>", "<|custom_stop|>"))
	})

	It("keeps deterministic GGUF serving tags inside Ollama's model name limit", func() {
		checksum := "sha256:86c8ea6c8b755687d0b723176fcd0b2411ef80533d23e2a5030f845d13ab2db7"
		tag := deterministicServingTag(&model.ServedModel{
			ModelID:      uuid.MustParse("dda89114-7f7b-439f-9cf7-a893a555ac70"),
			Name:         "hf-real-e2e-quantfactory-meta-llama-3-8b-instruct-gguf-eda5ff89",
			ModelVersion: 1,
		}, checksum)

		Expect(len(tag)).To(BeNumerically("<=", maxOllamaModelNameLength))
		Expect(tag).To(HavePrefix("bighill-"))
		Expect(tag).To(ContainSubstring("dda89114"))
		Expect(tag).To(HaveSuffix("-" + checksumTagPart(checksum)))
		Expect(tag).To(Equal("bighill-hf-real-e2e-quantfactory-meta-l-v1-dda89114-86c8ea6c8b75"))
	})

	It("uploads GGUF blobs when the Ollama blob check method is not implemented", func() {
		_, checksum, localS3Root := writeLocalS3Artifact("local-dev-bucket", "models/model.gguf", []byte("gguf bytes"))
		inspector := writeInspectorScript(GinkgoT().TempDir(), 0, llama3InspectionJSON())
		transport := &ollamaCreateTransport{
			existingTags:     map[string]bool{},
			blobHeadStatus:   http.StatusNotImplemented,
			inferredTemplate: "{{ range .Messages }}{{ .Content }}{{ end }}",
		}
		runtime := newTestRuntime("http://ollama.local",
			WithLocalS3Dir(localS3Root),
			WithArtifactCache(GinkgoT().TempDir()),
			WithGGUFInspectorCommand(inspector),
		)
		runtime.client = &http.Client{Transport: transport}

		state, err := runtime.EnsureServedModel(context.Background(), &model.ServedModel{
			ModelID:          uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
			ModelKind:        "BASE",
			Name:             "Meta Llama 3 Instruct",
			ModelVersion:     7,
			BaseModel:        "meta-llama/Meta-Llama-3-8B-Instruct",
			ArtifactLocation: "s3://local-dev-bucket/models/model.gguf",
			ArtifactFormat:   "GGUF_MODEL",
			ArtifactChecksum: checksum,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(state.Ready).To(BeTrue())
		Expect(transport.blobUploads).To(Equal(1))
		Expect(transport.createRequests).To(HaveLen(1))
	})

	It("fails closed for GGUF chat templates that cannot be rendered by Ollama", func() {
		_, checksum, localS3Root := writeLocalS3Artifact("local-dev-bucket", "models/model.gguf", []byte("gguf bytes"))
		inspector := writeInspectorScript(GinkgoT().TempDir(), 0, `{"architecture":"unknown","chat_template_present":true,"chat_template":"{% for message in messages %}{{ message['content'] }}{% endfor %}","stop_tokens":["<|end|>"]}`)
		transport := &ollamaCreateTransport{existingTags: map[string]bool{}}
		runtime := newTestRuntime("http://ollama.local",
			WithLocalS3Dir(localS3Root),
			WithArtifactCache(GinkgoT().TempDir()),
			WithGGUFInspectorCommand(inspector),
		)
		runtime.client = &http.Client{Transport: transport}

		_, err := runtime.EnsureServedModel(context.Background(), &model.ServedModel{
			ModelID:          uuid.New(),
			ModelKind:        "BASE",
			Name:             "unsupported-chat-template",
			ModelVersion:     1,
			BaseModel:        "unsupported",
			ArtifactLocation: "s3://local-dev-bucket/models/model.gguf",
			ArtifactFormat:   "GGUF_MODEL",
			ArtifactChecksum: checksum,
		})

		Expect(err).To(MatchError(ContainSubstring("GGUF chat template is not supported by local Ollama provisioning")))
		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
		Expect(transport.blobUploads).To(Equal(1))
		Expect(transport.createRequests).To(HaveLen(1))
		Expect(transport.deletedModels).To(HaveLen(1))
	})

	It("falls back to a family template when Ollama does not infer a usable chat definition", func() {
		_, checksum, localS3Root := writeLocalS3Artifact("local-dev-bucket", "models/model.gguf", []byte("gguf bytes"))
		inspector := writeInspectorScript(GinkgoT().TempDir(), 0, llama3InspectionJSON())
		transport := &ollamaCreateTransport{existingTags: map[string]bool{}}
		runtime := newTestRuntime("http://ollama.local",
			WithLocalS3Dir(localS3Root),
			WithArtifactCache(GinkgoT().TempDir()),
			WithGGUFInspectorCommand(inspector),
		)
		runtime.client = &http.Client{Transport: transport}

		state, err := runtime.EnsureServedModel(context.Background(), &model.ServedModel{
			ModelID:          uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
			ModelKind:        "BASE",
			Name:             "fallback",
			ModelVersion:     1,
			BaseModel:        "base",
			ArtifactLocation: "s3://local-dev-bucket/models/model.gguf",
			ArtifactFormat:   "GGUF_MODEL",
			ArtifactChecksum: checksum,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(state.Ready).To(BeTrue())
		Expect(transport.createRequests).To(HaveLen(2))
		Expect(transport.createRequests[0]).NotTo(HaveKey("template"))
		Expect(transport.deletedModels).To(HaveLen(1))
		Expect(transport.createRequests[1]).To(HaveKeyWithValue("template", llama3OllamaChatTemplate()))
		parameters, ok := transport.createRequests[1]["parameters"].(map[string]any)
		Expect(ok).To(BeTrue())
		Expect(parameters["stop"]).To(ConsistOf("<|start_header_id|>", "<|end_header_id|>", "<|eot_id|>", "<|custom_stop|>"))
	})

	It("does not mark a GGUF chat model loaded when tokenizer.chat_template validation fails", func() {
		_, checksum, localS3Root := writeLocalS3Artifact("local-dev-bucket", "models/model.gguf", []byte("gguf bytes"))
		inspector := writeInspectorScript(GinkgoT().TempDir(), 1, "missing tokenizer.chat_template")
		transport := &ollamaCreateTransport{existingTags: map[string]bool{}}
		runtime := newTestRuntime("http://ollama.local",
			WithLocalS3Dir(localS3Root),
			WithArtifactCache(GinkgoT().TempDir()),
			WithGGUFInspectorCommand(inspector),
		)
		runtime.client = &http.Client{Transport: transport}

		_, err := runtime.EnsureServedModel(context.Background(), &model.ServedModel{
			ModelID:          uuid.New(),
			ModelKind:        "BASE",
			Name:             "base",
			ModelVersion:     1,
			BaseModel:        "base",
			ArtifactLocation: "s3://local-dev-bucket/models/model.gguf",
			ArtifactFormat:   "GGUF_MODEL",
			ArtifactChecksum: checksum,
		})

		Expect(err).To(MatchError(ContainSubstring("GGUF validation failed")))
		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
		Expect(transport.blobUploads).To(Equal(0))
		Expect(transport.createRequests).To(BeEmpty())
	})

	It("skips GGUF create when the deterministic Ollama tag already exists", func() {
		tag := "bighill-existing-v1-aaaaaaaa-abc"
		transport := &ollamaCreateTransport{
			existingTags: map[string]bool{tag: true},
			templates:    map[string]string{tag: "{{ .Prompt }}"},
			parameters:   map[string]string{tag: `stop "<|eot_id|>"`},
		}
		runtime := newTestRuntime("http://ollama.local",
			WithArtifactCache(GinkgoT().TempDir()),
			WithGGUFInspectorCommand(writeInspectorScript(GinkgoT().TempDir(), 1, "should not run")),
		)
		runtime.client = &http.Client{Transport: transport}

		state, err := runtime.EnsureServedModel(context.Background(), &model.ServedModel{
			ModelID:          uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
			ModelKind:        "BASE",
			Name:             "existing",
			ModelVersion:     1,
			BaseModel:        "base",
			ArtifactLocation: "s3://local-dev-bucket/models/model.gguf",
			ArtifactFormat:   "GGUF_MODEL",
			ArtifactChecksum: "sha256:abc",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(state.ServingModel).To(Equal(tag))
		Expect(transport.createRequests).To(BeEmpty())
	})

	It("does not treat a stale pre-checksum GGUF tag as loaded", func() {
		_, checksum, localS3Root := writeLocalS3Artifact("local-dev-bucket", "models/model.gguf", []byte("new gguf bytes"))
		transport := &ollamaCreateTransport{
			existingTags:     map[string]bool{"bighill-existing-v1-aaaaaaaa": true},
			templates:        map[string]string{"bighill-existing-v1-aaaaaaaa": "{{ .Prompt }}"},
			parameters:       map[string]string{"bighill-existing-v1-aaaaaaaa": `stop "<|eot_id|>"`},
			inferredTemplate: "{{ range .Messages }}{{ .Content }}{{ end }}",
		}
		runtime := newTestRuntime("http://ollama.local",
			WithLocalS3Dir(localS3Root),
			WithArtifactCache(GinkgoT().TempDir()),
			WithGGUFInspectorCommand(writeInspectorScript(GinkgoT().TempDir(), 0, llama3InspectionJSON())),
		)
		runtime.client = &http.Client{Transport: transport}

		state, err := runtime.EnsureServedModel(context.Background(), &model.ServedModel{
			ModelID:          uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
			ModelKind:        "BASE",
			Name:             "existing",
			ModelVersion:     1,
			BaseModel:        "base",
			ArtifactLocation: "s3://local-dev-bucket/models/model.gguf",
			ArtifactFormat:   "GGUF_MODEL",
			ArtifactChecksum: checksum,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(state.ServingModel).To(Equal("bighill-existing-v1-aaaaaaaa-" + checksumTagPart(checksum)))
		Expect(transport.createRequests).To(HaveLen(1))
		Expect(transport.createRequests[0]["model"]).To(Equal(state.ServingModel))
	})

	It("does not mark an existing GGUF chat tag loaded without a chat template", func() {
		transport := &ollamaCreateTransport{existingTags: map[string]bool{"bighill-existing-v1-aaaaaaaa-abc": true}}
		runtime := newTestRuntime("http://ollama.local",
			WithArtifactCache(GinkgoT().TempDir()),
			WithGGUFInspectorCommand(writeInspectorScript(GinkgoT().TempDir(), 1, "should not run")),
		)
		runtime.client = &http.Client{Transport: transport}

		_, err := runtime.EnsureServedModel(context.Background(), &model.ServedModel{
			ModelID:          uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
			ModelKind:        "BASE",
			Name:             "existing",
			ModelVersion:     1,
			BaseModel:        "base",
			ArtifactLocation: "s3://local-dev-bucket/models/model.gguf",
			ArtifactFormat:   "GGUF_MODEL",
			ArtifactChecksum: "sha256:abc",
		})

		Expect(err).To(MatchError(ContainSubstring("missing a chat template")))
		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
		Expect(transport.createRequests).To(BeEmpty())
	})

	It("does not mark an existing GGUF chat tag loaded without stop parameters", func() {
		tag := "bighill-existing-v1-aaaaaaaa-abc"
		transport := &ollamaCreateTransport{
			existingTags: map[string]bool{tag: true},
			templates:    map[string]string{tag: "{{ range .Messages }}{{ .Content }}{{ end }}"},
		}
		runtime := newTestRuntime("http://ollama.local",
			WithArtifactCache(GinkgoT().TempDir()),
			WithGGUFInspectorCommand(writeInspectorScript(GinkgoT().TempDir(), 1, "should not run")),
		)
		runtime.client = &http.Client{Transport: transport}

		_, err := runtime.EnsureServedModel(context.Background(), &model.ServedModel{
			ModelID:          uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
			ModelKind:        "BASE",
			Name:             "existing",
			ModelVersion:     1,
			BaseModel:        "base",
			ArtifactLocation: "s3://local-dev-bucket/models/model.gguf",
			ArtifactFormat:   "GGUF_MODEL",
			ArtifactChecksum: "sha256:abc",
		})

		Expect(err).To(MatchError(ContainSubstring("missing stop parameters")))
		Expect(errors.Is(err, domain.ErrValidationFailed)).To(BeTrue())
		Expect(transport.createRequests).To(BeEmpty())
	})
})

type localOllamaTagsTransport struct {
	payload string
	assert  func(*http.Request)
}

func newLocalOllamaTagsTransport(payload string, assert func(*http.Request)) localOllamaTagsTransport {
	return localOllamaTagsTransport{payload: payload, assert: assert}
}

func (t localOllamaTagsTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method == http.MethodPost && req.URL.Path == "/api/generate" {
		return jsonResponse(http.StatusOK, map[string]any{"response": "", "done": true}), nil
	}
	if t.assert != nil {
		t.assert(req)
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(t.payload)),
		Header:     make(http.Header),
	}, nil
}

type ollamaCreateTransport struct {
	existingTags     map[string]bool
	templates        map[string]string
	parameters       map[string]string
	inferredTemplate string
	blobHeadStatus   int
	blobUploads      int
	createRequests   []map[string]any
	deletedModels    []string
}

func (t *ollamaCreateTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	switch {
	case req.Method == http.MethodGet && req.URL.Path == "/api/tags":
		models := make([]map[string]string, 0, len(t.existingTags))
		for tag := range t.existingTags {
			models = append(models, map[string]string{"name": tag})
		}
		return jsonResponse(http.StatusOK, map[string]any{"models": models}), nil
	case req.Method == http.MethodPost && req.URL.Path == "/api/show":
		var payload map[string]string
		Expect(json.NewDecoder(req.Body).Decode(&payload)).To(Succeed())
		return jsonResponse(http.StatusOK, map[string]any{
			"template":   t.templates[payload["model"]],
			"parameters": t.parameters[payload["model"]],
		}), nil
	case req.Method == http.MethodHead && strings.HasPrefix(req.URL.Path, "/api/blobs/"):
		status := t.blobHeadStatus
		if status == 0 {
			status = http.StatusNotFound
		}
		return jsonResponse(status, map[string]any{}), nil
	case req.Method == http.MethodPost && strings.HasPrefix(req.URL.Path, "/api/blobs/"):
		_, _ = io.Copy(io.Discard, req.Body)
		t.blobUploads++
		return jsonResponse(http.StatusCreated, map[string]any{}), nil
	case req.Method == http.MethodPost && req.URL.Path == "/api/create":
		var payload map[string]any
		Expect(json.NewDecoder(req.Body).Decode(&payload)).To(Succeed())
		t.createRequests = append(t.createRequests, payload)
		tag, _ := payload["model"].(string)
		if t.existingTags == nil {
			t.existingTags = map[string]bool{}
		}
		t.existingTags[tag] = true
		if t.templates == nil {
			t.templates = map[string]string{}
		}
		if t.parameters == nil {
			t.parameters = map[string]string{}
		}
		if template, ok := payload["template"].(string); ok {
			t.templates[tag] = template
		} else if t.inferredTemplate != "" {
			t.templates[tag] = t.inferredTemplate
		}
		if parameters, ok := payload["parameters"]; ok {
			t.parameters[tag] = ollamaParametersString(parameters)
		}
		return jsonResponse(http.StatusOK, map[string]any{"status": "success"}), nil
	case req.Method == http.MethodDelete && req.URL.Path == "/api/delete":
		var payload map[string]string
		Expect(json.NewDecoder(req.Body).Decode(&payload)).To(Succeed())
		tag := payload["model"]
		delete(t.existingTags, tag)
		delete(t.templates, tag)
		delete(t.parameters, tag)
		t.deletedModels = append(t.deletedModels, tag)
		return jsonResponse(http.StatusOK, map[string]any{"status": "success"}), nil
	case req.Method == http.MethodPost && req.URL.Path == "/api/generate":
		var payload map[string]any
		Expect(json.NewDecoder(req.Body).Decode(&payload)).To(Succeed())
		Expect(payload).To(HaveKey("model"))
		Expect(payload).To(HaveKeyWithValue("stream", false))
		return jsonResponse(http.StatusOK, map[string]any{"response": "", "done": true}), nil
	default:
		return jsonResponse(http.StatusNotFound, map[string]any{"error": req.Method + " " + req.URL.Path}), nil
	}
}

func jsonResponse(status int, payload any) *http.Response {
	body, err := json.Marshal(payload)
	Expect(err).NotTo(HaveOccurred())
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(strings.NewReader(string(body))),
		Header:     make(http.Header),
	}
}

func writeLocalS3Artifact(bucket string, key string, payload []byte) (string, string, string) {
	root := GinkgoT().TempDir()
	path := filepath.Join(root, bucket, key)
	Expect(os.MkdirAll(filepath.Dir(path), 0o755)).To(Succeed())
	Expect(os.WriteFile(path, payload, 0o644)).To(Succeed())
	sum := sha256.Sum256(payload)
	return path, "sha256:" + hex.EncodeToString(sum[:]), root
}

func writeInspectorScript(dir string, exitCode int, output string) string {
	path := filepath.Join(dir, "inspect-gguf")
	outputPath := filepath.Join(dir, "inspect-output")
	Expect(os.WriteFile(outputPath, []byte(output+"\n"), 0o644)).To(Succeed())
	quotedOutputPath := "'" + strings.ReplaceAll(outputPath, "'", "'\\''") + "'"
	var script string
	if exitCode == 0 {
		script = "#!/usr/bin/env sh\ncat " + quotedOutputPath + "\n"
	} else {
		script = "#!/usr/bin/env sh\ncat " + quotedOutputPath + " >&2\nexit 1\n"
	}
	Expect(os.WriteFile(path, []byte(script), 0o755)).To(Succeed())
	return path
}

func llama3InspectionJSON() string {
	payload := map[string]any{
		"architecture":          "llama",
		"chat_template_present": true,
		"chat_template":         llama3JinjaChatTemplate(),
		"stop_tokens":           []string{"<|eot_id|>", "<|custom_stop|>"},
	}
	raw, err := json.Marshal(payload)
	Expect(err).NotTo(HaveOccurred())
	return string(raw)
}

func llama3JinjaChatTemplate() string {
	return `{% set loop_messages = messages %}{% for message in loop_messages %}{% set content = '<|start_header_id|>' + message['role'] + '<|end_header_id|>

'+ message['content'] | trim + '<|eot_id|>' %}{% if loop.index0 == 0 %}{% set content = bos_token + content %}{% endif %}{{ content }}{% endfor %}{% if add_generation_prompt %}{{ '<|start_header_id|>assistant<|end_header_id|>

' }}{% endif %}`
}

var _ = Describe("Store record conversion", func() {
	It("converts local store records to served models", func() {
		modelID := uuid.New()
		trainingRunID := uuid.New()
		datasetID := uuid.New()
		record := localstore.Record{
			Name:       "served-model",
			Namespace:  "default",
			Generation: 2,
			Spec: localstore.Spec{
				ModelID:       modelID.String(),
				TrainingRunID: trainingRunID.String(),
				DatasetID:     datasetID.String(),
				ModelKind:     "BASE",
				Name:          "llama",
				ModelVersion:  1,
				BaseModel:     "meta-llama/Llama",
			},
			Status: localstore.Status{
				ServingLoadStatus:  model.ModelLoadStatusLoaded.String(),
				ServingTarget:      "http://runtime",
				ServingModel:       "llama",
				ObservedGeneration: 2,
				ReadyReplicas:      1,
			},
		}

		served, err := recordToServedModel(record)

		Expect(err).NotTo(HaveOccurred())
		Expect(served.ModelID).To(Equal(modelID))
		Expect(served.TrainingRunID).To(Equal(trainingRunID))
		Expect(served.DatasetID).To(Equal(datasetID))
		Expect(served.ModelKind).To(Equal("BASE"))
		Expect(served.Status.ServingLoadStatus).To(Equal(model.ModelLoadStatusLoaded))
	})

	It("rejects invalid local store IDs and statuses", func() {
		_, err := recordToServedModel(localstore.Record{Spec: localstore.Spec{ModelID: "bad"}})
		Expect(err).To(HaveOccurred())

		_, err = recordToServedModel(localstore.Record{Spec: localstore.Spec{ModelID: uuid.NewString(), TrainingRunID: "bad"}})
		Expect(err).To(HaveOccurred())

		_, err = recordToServedModel(localstore.Record{
			Spec:   localstore.Spec{ModelID: uuid.NewString()},
			Status: localstore.Status{ServingLoadStatus: "WARMING"},
		})
		Expect(err).To(HaveOccurred())
	})

	It("handles optional UUIDs and Kubernetes watch objects", func() {
		id, err := parseOptionalUUID("")
		Expect(err).NotTo(HaveOccurred())
		Expect(id).To(Equal(uuid.Nil))

		_, err = parseOptionalUUID("bad")
		Expect(err).To(HaveOccurred())

		record := localstore.Record{Name: "served-model", Namespace: "default", Spec: localstore.Spec{ModelID: uuid.NewString()}}
		Expect(recordsByName([]localstore.Record{record})).To(HaveKey("served-model"))
		Expect(deletedObject("served-model").GetName()).To(Equal("served-model"))
		Expect(recordToObject(record).GetKind()).To(Equal("ServedModel"))
	})
})

var _ = Describe("Store", func() {
	It("reconciles dataset-bound local store intent into loaded model status", func() {
		path := filepath.Join(GinkgoT().TempDir(), "served_models.json")
		store, err := NewStore("default", path)
		Expect(err).NotTo(HaveOccurred())
		modelID := uuid.New()
		datasetID := uuid.New()
		name := localstore.ResourceName(modelID.String(), 1)
		Expect(store.store.UpsertSpec(name, "default", localstore.Spec{
			ModelID:        modelID.String(),
			DatasetID:      datasetID.String(),
			ModelKind:      "BASE",
			Name:           "rag-e2e-uploaded-base",
			ModelVersion:   1,
			BaseModel:      "local-user-model:latest",
			ArtifactFormat: "HF_MODEL",
		})).To(Succeed())
		runtime := newTestRuntime("http://ollama.local")
		runtime.client = &http.Client{Transport: newLocalOllamaTagsTransport(`{"models":[{"name":"local-user-model:latest"}]}`, nil)}
		reconciler := app.NewServedModelReconciler(runtime, store)
		controller := servingkubernetes.NewServedModelController(store, reconciler, time.Millisecond)

		Expect(controller.ProcessOnce(context.Background())).To(Succeed())

		served, err := store.Read(context.Background(), name)
		Expect(err).NotTo(HaveOccurred())
		Expect(served.DatasetID).To(Equal(datasetID))
		Expect(served.Status.ServingLoadStatus).To(Equal(model.ModelLoadStatusLoaded))
		Expect(served.Status.ServingTarget).To(Equal("http://ollama.local"))
		Expect(served.Status.ServingModel).To(Equal("local-user-model:latest"))
		Expect(served.Status.ServingProtocol).To(Equal(model.ServingProtocolOllamaGenerate))
	})

	It("reads and lists served models from the local store", func() {
		path := filepath.Join(GinkgoT().TempDir(), "served_models.json")
		store, err := NewStore("default", path)
		Expect(err).NotTo(HaveOccurred())
		modelID := uuid.New()
		name := localstore.ResourceName(modelID.String(), 1)
		Expect(store.store.UpsertSpec(name, "default", localstore.Spec{
			ModelID:      modelID.String(),
			Name:         "llama",
			ModelVersion: 1,
			BaseModel:    "meta-llama/Llama",
		})).To(Succeed())

		served, err := store.Read(context.Background(), name)
		Expect(err).NotTo(HaveOccurred())
		Expect(served.ModelID).To(Equal(modelID))

		list, resourceVersion, err := store.ListWithResourceVersion(context.Background())
		Expect(err).NotTo(HaveOccurred())
		Expect(list).To(HaveLen(1))
		Expect(resourceVersion).NotTo(BeEmpty())
		Expect(store.Namespace()).To(Equal("default"))
	})

	It("returns served-model-not-found errors for missing records", func() {
		store, err := NewStore("default", filepath.Join(GinkgoT().TempDir(), "served_models.json"))
		Expect(err).NotTo(HaveOccurred())

		_, err = store.Read(context.Background(), "missing")

		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, domain.ErrServedModelNotFound)).To(BeTrue())
	})

	It("updates local status records", func() {
		path := filepath.Join(GinkgoT().TempDir(), "served_models.json")
		store, err := NewStore("default", path)
		Expect(err).NotTo(HaveOccurred())
		modelID := uuid.New()
		name := localstore.ResourceName(modelID.String(), 1)
		Expect(store.store.UpsertSpec(name, "default", localstore.Spec{ModelID: modelID.String(), Name: "llama", ModelVersion: 1, BaseModel: "meta-llama/Llama"})).To(Succeed())

		Expect(store.UpdateStatus(context.Background(), name, &model.ServedModelStatus{
			ServingLoadStatus:  model.ModelLoadStatusLoaded,
			ServingTarget:      "http://runtime",
			ServingModel:       "llama",
			ObservedGeneration: 1,
			ReadyReplicas:      1,
		})).To(Succeed())

		served, err := store.Read(context.Background(), name)
		Expect(err).NotTo(HaveOccurred())
		Expect(served.Status.ServingLoadStatus).To(Equal(model.ModelLoadStatusLoaded))
	})

	It("does not block sending watch events after cancellation", func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		events := make(chan watch.Event)

		Expect(sendWatchEvent(ctx, events, watch.Event{Type: watch.Added})).To(BeFalse())
	})
})
