package localserving

import (
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"lib/shared_lib/processrunner"
	"model_serving_service/pkg/domain"
	"model_serving_service/pkg/domain/model"

	log "github.com/sirupsen/logrus"
)

const (
	modelKindBase                 = "BASE"
	artifactFormatGGUFModel       = "GGUF_MODEL"
	artifactFormatGGUFLoRAAdapter = "GGUF_LORA_ADAPTER"
	legacyArtifactFormatGGUF      = "GGUF"
	servingProtocolOpenAIChat     = "OPENAI_CHAT_COMPLETIONS"
	ollamaTagsPath                = "/api/tags"
	ollamaShowPath                = "/api/show"
	ollamaCreatePath              = "/api/create"
	ollamaDeletePath              = "/api/delete"
	ollamaBlobPath                = "/api/blobs/"
	maxOllamaModelNameLength      = 64
)

type Runtime struct {
	namespace      string
	port           int32
	ollamaEndpoint string
	client         *http.Client
	artifactCache  string
	localS3Dir     string
	inspector      []string
	createTimeout  time.Duration
}

type RuntimeOption func(*Runtime)

type ggufInspection struct {
	Architecture        string   `json:"architecture"`
	ChatTemplatePresent bool     `json:"chat_template_present"`
	ChatTemplate        string   `json:"chat_template"`
	StopTokens          []string `json:"stop_tokens"`
}

func WithArtifactCache(path string) RuntimeOption {
	return func(r *Runtime) {
		r.artifactCache = strings.TrimSpace(path)
	}
}

func WithLocalS3Dir(path string) RuntimeOption {
	return func(r *Runtime) {
		r.localS3Dir = strings.TrimSpace(path)
	}
}

func WithGGUFInspectorCommand(command string) RuntimeOption {
	return func(r *Runtime) {
		r.inspector = strings.Fields(command)
	}
}

func WithCreateTimeout(timeout time.Duration) RuntimeOption {
	return func(r *Runtime) {
		r.createTimeout = timeout
	}
}

func NewRuntime(namespace string, port int32, ollamaEndpoint string, options ...RuntimeOption) (*Runtime, error) {
	log.Trace("localserving NewRuntime")

	runtime := &Runtime{
		namespace:      strings.TrimSpace(namespace),
		port:           port,
		ollamaEndpoint: strings.TrimRight(strings.TrimSpace(ollamaEndpoint), "/"),
		client:         &http.Client{},
	}
	for _, option := range options {
		option(runtime)
	}
	if runtime.namespace == "" {
		return nil, domain.ErrValidationFailed.Extend("local serving namespace is required")
	}
	if runtime.port <= 0 {
		return nil, domain.ErrValidationFailed.Extend("local serving port is required")
	}
	if runtime.ollamaEndpoint == "" {
		return nil, domain.ErrValidationFailed.Extend("local Ollama endpoint is required")
	}
	if strings.TrimSpace(runtime.artifactCache) == "" {
		return nil, domain.ErrValidationFailed.Extend("local artifact cache is required")
	}
	if len(runtime.inspector) == 0 || strings.TrimSpace(runtime.inspector[0]) == "" {
		return nil, domain.ErrValidationFailed.Extend("GGUF inspector command is required")
	}
	if runtime.createTimeout <= 0 {
		return nil, domain.ErrValidationFailed.Extend("local Ollama create timeout is required")
	}
	return runtime, nil
}

func (r *Runtime) EnsureServedModel(ctx context.Context, servedModel *model.ServedModel) (*model.ServingRuntimeState, error) {
	log.Trace("localserving Runtime EnsureServedModel")

	if strings.TrimSpace(servedModel.BaseModel) == "" {
		return nil, domain.ErrValidationFailed.Extend("base model is required")
	}
	servingModel := strings.TrimSpace(servedModel.ServingModel)
	if servingModel == "" {
		if strings.EqualFold(strings.TrimSpace(servedModel.ModelKind), modelKindBase) {
			servingModel = strings.TrimSpace(servedModel.BaseModel)
		} else {
			return nil, domain.ErrValidationFailed.Extend("serving model is required for non-base local served models")
		}
	}
	servingTarget := strings.TrimSpace(servedModel.ServingTarget)
	if servingTarget == "" {
		servingTarget = r.ollamaEndpoint
	}
	if servingTarget == "" {
		return nil, domain.ErrValidationFailed.Extend("local serving target is required")
	}
	if isGGUFArtifact(servedModel.ArtifactFormat) {
		return r.ensureGGUFServedModel(ctx, servedModel, servingTarget)
	}
	if strings.EqualFold(strings.TrimSpace(servedModel.ModelKind), modelKindBase) {
		if err := r.ensureOllamaTag(ctx, servingTarget, servingModel); err != nil {
			return nil, err
		}
	}
	return &model.ServingRuntimeState{
		Ready:           true,
		ServingTarget:   servingTarget,
		ServingModel:    servingModel,
		ServingProtocol: model.ServingProtocolOllamaGenerate,
		ReadyReplicas:   1,
	}, nil
}

func (r *Runtime) ensureGGUFServedModel(ctx context.Context, servedModel *model.ServedModel, servingTarget string) (*model.ServingRuntimeState, error) {
	log.Trace("localserving Runtime ensureGGUFServedModel")

	if strings.TrimSpace(servedModel.ArtifactLocation) == "" {
		return nil, domain.ErrValidationFailed.Extend("GGUF artifact location is required")
	}
	checksum := normalizeChecksum(servedModel.ArtifactChecksum)
	artifactPath := ""
	if checksum == "" {
		var err error
		artifactPath, checksum, err = r.cacheArtifact(ctx, servedModel.ArtifactLocation, servedModel.ArtifactChecksum)
		if err != nil {
			return nil, err
		}
	}
	servingModel := deterministicServingTag(servedModel, checksum)
	if err := r.ensureOllamaTag(ctx, servingTarget, servingModel); err == nil {
		if err := r.ensureOllamaChatModel(ctx, servingTarget, servingModel); err != nil {
			return nil, err
		}
		return &model.ServingRuntimeState{
			Ready:           true,
			ServingTarget:   servingTarget,
			ServingModel:    servingModel,
			ServingProtocol: model.ServingProtocolOpenAIChatCompletions,
			ReadyReplicas:   1,
		}, nil
	}
	if artifactPath == "" {
		var err error
		artifactPath, checksum, err = r.cacheArtifact(ctx, servedModel.ArtifactLocation, checksum)
		if err != nil {
			return nil, err
		}
	}
	var inspection ggufInspection
	createTemplate := ""
	var createParameters map[string]any
	if isGGUFLoRAAdapter(servedModel.ArtifactFormat) {
		var err error
		inspection, err = r.inspectGGUF(ctx, artifactPath, false)
		if err != nil {
			return nil, err
		}
		if err := r.ensureOllamaTag(ctx, servingTarget, strings.TrimSpace(servedModel.BaseModel)); err != nil {
			return nil, fmt.Errorf("%w: base model for GGUF adapter is not available: %w", domain.ErrValidationFailed, err)
		}
		baseDefinition, err := r.ollamaChatDefinition(ctx, servingTarget, strings.TrimSpace(servedModel.BaseModel))
		if err != nil {
			return nil, err
		}
		createTemplate = baseDefinition.Template
	} else {
		var err error
		inspection, err = r.inspectGGUF(ctx, artifactPath, true)
		if err != nil {
			return nil, err
		}
		createParameters = stopParameters(inspection.StopTokens)
	}
	if err := r.ensureBlob(ctx, servingTarget, artifactPath, checksum); err != nil {
		return nil, err
	}
	if strings.TrimSpace(createTemplate) == "" && !isGGUFLoRAAdapter(servedModel.ArtifactFormat) {
		if err := r.createAndVerifyInferredGGUFModel(ctx, servingTarget, servedModel, servingModel, artifactPath, checksum, inspection, createParameters); err != nil {
			return nil, err
		}
	} else {
		if err := r.createGGUFModel(ctx, servingTarget, servedModel, servingModel, artifactPath, checksum, createTemplate, createParameters); err != nil {
			return nil, err
		}
		if err := r.ensureOllamaTag(ctx, servingTarget, servingModel); err != nil {
			return nil, err
		}
		if err := r.ensureOllamaChatModel(ctx, servingTarget, servingModel); err != nil {
			return nil, err
		}
	}
	return &model.ServingRuntimeState{
		Ready:           true,
		ServingTarget:   servingTarget,
		ServingModel:    servingModel,
		ServingProtocol: model.ServingProtocolOpenAIChatCompletions,
		ReadyReplicas:   1,
	}, nil
}

func (r *Runtime) cacheArtifact(ctx context.Context, artifactURI string, expectedChecksum string) (string, string, error) {
	log.Trace("localserving Runtime cacheArtifact")

	checksum := normalizeChecksum(expectedChecksum)
	cacheKey := checksum
	if cacheKey == "" {
		cacheKey = sha1Hex(artifactURI)
	}
	cacheKey = strings.TrimPrefix(cacheKey, "sha256:")
	if err := os.MkdirAll(r.artifactCache, 0o755); err != nil {
		return "", "", fmt.Errorf("%w: create artifact cache: %w", domain.ErrModelServe, err)
	}
	targetPath := filepath.Join(r.artifactCache, cacheKey+".gguf")
	if checksum != "" {
		if ok, err := fileMatchesChecksum(targetPath, checksum); err != nil {
			return "", "", err
		} else if ok {
			return targetPath, checksum, nil
		}
	}
	actualChecksum, err := r.downloadArtifact(ctx, artifactURI, targetPath)
	if err != nil {
		return "", "", err
	}
	if checksum != "" && actualChecksum != checksum {
		_ = os.Remove(targetPath)
		return "", "", domain.ErrValidationFailed.Extend(fmt.Sprintf("artifact checksum mismatch: expected %s got %s", checksum, actualChecksum))
	}
	if checksum == "" {
		checksum = actualChecksum
	}
	return targetPath, checksum, nil
}

func (r *Runtime) downloadArtifact(ctx context.Context, artifactURI string, targetPath string) (string, error) {
	log.Trace("localserving Runtime downloadArtifact")

	parsed, err := url.Parse(strings.TrimSpace(artifactURI))
	if err != nil {
		return "", fmt.Errorf("%w: parse artifact uri: %w", domain.ErrValidationFailed, err)
	}
	tmpPath := targetPath + ".tmp"
	_ = os.Remove(tmpPath)
	out, err := os.Create(tmpPath)
	if err != nil {
		return "", fmt.Errorf("%w: create artifact cache file: %w", domain.ErrModelServe, err)
	}
	hash := sha256.New()
	writer := io.MultiWriter(out, hash)
	closeAndRemove := func() {
		_ = out.Close()
		_ = os.Remove(tmpPath)
	}
	switch parsed.Scheme {
	case "", "file":
		sourcePath := parsed.Path
		if parsed.Scheme == "" {
			sourcePath = artifactURI
		}
		source, err := os.Open(sourcePath)
		if err != nil {
			closeAndRemove()
			return "", fmt.Errorf("%w: open artifact file: %w", domain.ErrModelServe, err)
		}
		if _, err := io.Copy(writer, source); err != nil {
			_ = source.Close()
			closeAndRemove()
			return "", fmt.Errorf("%w: copy artifact file: %w", domain.ErrModelServe, err)
		}
		_ = source.Close()
	case "s3":
		if strings.TrimSpace(r.localS3Dir) == "" {
			closeAndRemove()
			return "", domain.ErrValidationFailed.Extend("local S3 storage directory is required for local Ollama s3 artifacts")
		}
		sourcePath := filepath.Join(r.localS3Dir, parsed.Host, strings.TrimPrefix(parsed.Path, "/"))
		source, err := os.Open(sourcePath)
		if err != nil {
			closeAndRemove()
			return "", fmt.Errorf("%w: open local s3 artifact: %w", domain.ErrModelServe, err)
		}
		if _, err := io.Copy(writer, source); err != nil {
			_ = source.Close()
			closeAndRemove()
			return "", fmt.Errorf("%w: copy local s3 artifact: %w", domain.ErrModelServe, err)
		}
		_ = source.Close()
	default:
		closeAndRemove()
		return "", domain.ErrValidationFailed.Extend(fmt.Sprintf("unsupported artifact uri scheme %q", parsed.Scheme))
	}
	if err := out.Close(); err != nil {
		closeAndRemove()
		return "", fmt.Errorf("%w: close artifact cache file: %w", domain.ErrModelServe, err)
	}
	if err := os.Rename(tmpPath, targetPath); err != nil {
		closeAndRemove()
		return "", fmt.Errorf("%w: commit artifact cache file: %w", domain.ErrModelServe, err)
	}
	return "sha256:" + hex.EncodeToString(hash.Sum(nil)), nil
}

func (r *Runtime) inspectGGUF(ctx context.Context, path string, requireChatTemplate bool) (ggufInspection, error) {
	log.Trace("localserving Runtime inspectGGUF")

	if len(r.inspector) == 0 || strings.TrimSpace(r.inspector[0]) == "" {
		return ggufInspection{}, domain.ErrValidationFailed.Extend("GGUF inspector command is required")
	}
	args := append([]string{}, r.inspector[1:]...)
	if requireChatTemplate {
		args = append(args, "--require-chat-template")
	}
	args = append(args, path)
	result, err := processrunner.Run(ctx, processrunner.Command{
		Name:             r.inspector[0],
		Args:             args,
		Timeout:          2 * time.Minute,
		StdoutLimitBytes: 64 * 1024,
		StderrLimitBytes: 64 * 1024,
	})
	if err != nil {
		return ggufInspection{}, fmt.Errorf("%w: GGUF validation failed: %w: %s", domain.ErrValidationFailed, err, strings.TrimSpace(result.Stderr))
	}
	var payload ggufInspection
	if err := json.Unmarshal(result.Stdout, &payload); err != nil {
		return ggufInspection{}, fmt.Errorf("%w: parse GGUF validation result: %w", domain.ErrValidationFailed, err)
	}
	if requireChatTemplate && !payload.ChatTemplatePresent {
		return ggufInspection{}, domain.ErrValidationFailed.Extend("GGUF chat model is missing tokenizer.chat_template")
	}
	return payload, nil
}

func (r *Runtime) ensureBlob(ctx context.Context, endpoint string, path string, checksum string) error {
	log.Trace("localserving Runtime ensureBlob")

	digest := strings.TrimPrefix(normalizeChecksum(checksum), "sha256:")
	if digest == "" {
		return domain.ErrValidationFailed.Extend("artifact checksum is required for Ollama blob upload")
	}
	blobURL := strings.TrimRight(endpoint, "/") + ollamaBlobPath + "sha256:" + digest
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, blobURL, nil)
	if err != nil {
		return fmt.Errorf("%w: build ollama blob check request: %w", domain.ErrValidationFailed, err)
	}
	resp, err := r.client.Do(req)
	if err == nil && resp != nil {
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			return nil
		}
		if resp.StatusCode != http.StatusNotFound &&
			resp.StatusCode != http.StatusMethodNotAllowed &&
			resp.StatusCode != http.StatusNotImplemented {
			return domain.ErrValidationFailed.Extend(fmt.Sprintf("ollama blob check returned status %d", resp.StatusCode))
		}
	}
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("%w: open cached artifact for blob upload: %w", domain.ErrModelServe, err)
	}
	defer file.Close()
	uploadCtx := ctx
	cancel := func() {}
	if r.createTimeout > 0 {
		uploadCtx, cancel = context.WithTimeout(ctx, r.createTimeout)
	}
	defer cancel()
	req, err = http.NewRequestWithContext(uploadCtx, http.MethodPost, blobURL, file)
	if err != nil {
		return fmt.Errorf("%w: build ollama blob upload request: %w", domain.ErrValidationFailed, err)
	}
	resp, err = r.client.Do(req)
	if err != nil {
		return fmt.Errorf("%w: upload ollama blob: %w", domain.ErrModelServe, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return domain.ErrModelServe.Extend(fmt.Sprintf("ollama blob upload returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(raw))))
	}
	return nil
}

func (r *Runtime) createAndVerifyInferredGGUFModel(ctx context.Context, endpoint string, servedModel *model.ServedModel, servingModel string, artifactPath string, checksum string, inspection ggufInspection, parameters map[string]any) error {
	log.Trace("localserving Runtime createAndVerifyInferredGGUFModel")

	inferredErr := r.createGGUFModel(ctx, endpoint, servedModel, servingModel, artifactPath, checksum, "", parameters)
	if inferredErr == nil {
		if err := r.ensureOllamaTag(ctx, endpoint, servingModel); err == nil {
			if err := r.ensureOllamaChatModel(ctx, endpoint, servingModel); err == nil {
				return nil
			} else {
				inferredErr = err
			}
		} else {
			inferredErr = err
		}
	}
	_ = r.deleteOllamaModel(ctx, endpoint, servingModel)
	fallbackTemplate, fallbackParameters, fallbackErr := fallbackOllamaTemplateForGGUF(inspection)
	if fallbackErr != nil {
		return fmt.Errorf("%w: Ollama did not infer a usable chat model from GGUF metadata: %w", fallbackErr, inferredErr)
	}
	if err := r.createGGUFModel(ctx, endpoint, servedModel, servingModel, artifactPath, checksum, fallbackTemplate, mergeStopParameters(fallbackParameters, inspection.StopTokens)); err != nil {
		return err
	}
	if err := r.ensureOllamaTag(ctx, endpoint, servingModel); err != nil {
		return err
	}
	return r.ensureOllamaChatModel(ctx, endpoint, servingModel)
}

func (r *Runtime) createGGUFModel(ctx context.Context, endpoint string, servedModel *model.ServedModel, servingModel string, artifactPath string, checksum string, chatTemplate string, parameters map[string]any) error {
	log.Trace("localserving Runtime createGGUFModel")

	fileName := filepath.Base(artifactPath)
	digest := "sha256:" + strings.TrimPrefix(normalizeChecksum(checksum), "sha256:")
	payload := map[string]any{
		"model":  servingModel,
		"stream": false,
	}
	if strings.TrimSpace(chatTemplate) != "" {
		payload["template"] = chatTemplate
	}
	if len(parameters) > 0 {
		payload["parameters"] = parameters
	}
	if isGGUFLoRAAdapter(servedModel.ArtifactFormat) {
		if strings.TrimSpace(servedModel.BaseModel) == "" {
			return domain.ErrValidationFailed.Extend("base model is required for GGUF LoRA adapter serving")
		}
		payload["from"] = strings.TrimSpace(servedModel.BaseModel)
		payload["adapters"] = map[string]string{fileName: digest}
	} else {
		payload["files"] = map[string]string{fileName: digest}
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	createCtx := ctx
	cancel := func() {}
	if r.createTimeout > 0 {
		createCtx, cancel = context.WithTimeout(ctx, r.createTimeout)
	}
	defer cancel()
	req, err := http.NewRequestWithContext(createCtx, http.MethodPost, strings.TrimRight(endpoint, "/")+ollamaCreatePath, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("%w: build ollama create request: %w", domain.ErrValidationFailed, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.client.Do(req)
	if err != nil {
		return fmt.Errorf("%w: create ollama model: %w", domain.ErrModelServe, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return domain.ErrModelServe.Extend(fmt.Sprintf("ollama create returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(raw))))
	}
	return nil
}

func (r *Runtime) deleteOllamaModel(ctx context.Context, endpoint string, tag string) error {
	log.Trace("localserving Runtime deleteOllamaModel")

	body, err := json.Marshal(map[string]any{"model": strings.TrimSpace(tag)})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, strings.TrimRight(endpoint, "/")+ollamaDeletePath, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("%w: build ollama delete request: %w", domain.ErrValidationFailed, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return domain.ErrModelServe.Extend(fmt.Sprintf("ollama delete returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(raw))))
	}
	return nil
}

func fallbackOllamaTemplateForGGUF(inspection ggufInspection) (string, map[string]any, error) {
	log.Trace("fallbackOllamaTemplateForGGUF")

	template := strings.TrimSpace(inspection.ChatTemplate)
	if template == "" {
		return "", nil, domain.ErrValidationFailed.Extend("GGUF chat model is missing tokenizer.chat_template")
	}
	switch {
	case isLlama3ChatTemplate(template):
		return llama3OllamaChatTemplate(), stopParameters([]string{"<|start_header_id|>", "<|end_header_id|>", "<|eot_id|>"}), nil
	case isChatMLTemplate(template):
		return chatMLOllamaChatTemplate(), stopParameters([]string{"<|im_end|>", "<|endoftext|>"}), nil
	case isGemmaChatTemplate(template):
		return gemmaOllamaChatTemplate(), stopParameters([]string{"<end_of_turn>"}), nil
	case isPhiChatTemplate(template):
		return phiOllamaChatTemplate(), stopParameters([]string{"<|end|>"}), nil
	case isLlama2ChatTemplate(template):
		return llama2OllamaChatTemplate(), stopParameters([]string{"</s>"}), nil
	case isMistralChatTemplate(template):
		return mistralOllamaChatTemplate(), stopParameters([]string{"</s>"}), nil
	}
	return "", nil, domain.ErrValidationFailed.Extend("GGUF chat template is not supported by local Ollama provisioning")
}

func isLlama3ChatTemplate(template string) bool {
	log.Trace("isLlama3ChatTemplate")

	return strings.Contains(template, "<|start_header_id|>") &&
		strings.Contains(template, "<|end_header_id|>") &&
		strings.Contains(template, "<|eot_id|>") &&
		strings.Contains(template, "add_generation_prompt")
}

func isChatMLTemplate(template string) bool {
	log.Trace("isChatMLTemplate")

	return strings.Contains(template, "<|im_start|>") &&
		strings.Contains(template, "<|im_end|>")
}

func isGemmaChatTemplate(template string) bool {
	log.Trace("isGemmaChatTemplate")

	return strings.Contains(template, "<start_of_turn>") &&
		strings.Contains(template, "<end_of_turn>")
}

func isPhiChatTemplate(template string) bool {
	log.Trace("isPhiChatTemplate")

	return strings.Contains(template, "<|user|>") &&
		strings.Contains(template, "<|assistant|>") &&
		strings.Contains(template, "<|end|>")
}

func isLlama2ChatTemplate(template string) bool {
	log.Trace("isLlama2ChatTemplate")

	return strings.Contains(template, "[INST]") &&
		strings.Contains(template, "[/INST]") &&
		strings.Contains(template, "<<SYS>>")
}

func isMistralChatTemplate(template string) bool {
	log.Trace("isMistralChatTemplate")

	return strings.Contains(template, "[INST]") &&
		strings.Contains(template, "[/INST]")
}

func llama3OllamaChatTemplate() string {
	log.Trace("llama3OllamaChatTemplate")

	return `{{- if .Messages }}{{- range .Messages }}<|start_header_id|>{{ .Role }}<|end_header_id|>

{{ .Content }}<|eot_id|>{{- end }}<|start_header_id|>assistant<|end_header_id|>

{{- else }}{{- if .System }}<|start_header_id|>system<|end_header_id|>

{{ .System }}<|eot_id|>{{ end }}{{ if .Prompt }}<|start_header_id|>user<|end_header_id|>

{{ .Prompt }}<|eot_id|>{{ end }}<|start_header_id|>assistant<|end_header_id|>{{- end }}

{{ .Response }}<|eot_id|>`
}

func chatMLOllamaChatTemplate() string {
	log.Trace("chatMLOllamaChatTemplate")

	return `{{- if .Messages }}{{- range .Messages }}<|im_start|>{{ .Role }}
{{ .Content }}<|im_end|>
{{- end }}<|im_start|>assistant
{{- else }}{{- if .System }}<|im_start|>system
{{ .System }}<|im_end|>
{{- end }}{{- if .Prompt }}<|im_start|>user
{{ .Prompt }}<|im_end|>
{{- end }}<|im_start|>assistant
{{- end }}{{ .Response }}<|im_end|>`
}

func gemmaOllamaChatTemplate() string {
	log.Trace("gemmaOllamaChatTemplate")

	return `{{- if .Messages }}{{- range .Messages }}<start_of_turn>{{ if eq .Role "assistant" }}model{{ else }}{{ .Role }}{{ end }}
{{ .Content }}<end_of_turn>
{{- end }}<start_of_turn>model
{{- else }}{{- if .System }}<start_of_turn>user
{{ .System }}

{{ .Prompt }}<end_of_turn>
{{- else }}<start_of_turn>user
{{ .Prompt }}<end_of_turn>
{{- end }}<start_of_turn>model
{{- end }}{{ .Response }}<end_of_turn>`
}

func phiOllamaChatTemplate() string {
	log.Trace("phiOllamaChatTemplate")

	return `{{- if .Messages }}{{- range .Messages }}{{- if eq .Role "user" }}<|user|>
{{ .Content }}<|end|>
{{- else if eq .Role "assistant" }}<|assistant|>
{{ .Content }}<|end|>
{{- else if eq .Role "system" }}<|system|>
{{ .Content }}<|end|>
{{- end }}{{- end }}<|assistant|>
{{- else }}{{- if .System }}<|system|>
{{ .System }}<|end|>
{{- end }}<|user|>
{{ .Prompt }}<|end|>
<|assistant|>
{{- end }}{{ .Response }}<|end|>`
}

func llama2OllamaChatTemplate() string {
	log.Trace("llama2OllamaChatTemplate")

	return `{{- if .Messages }}{{- $system := "" }}{{- range .Messages }}{{- if eq .Role "system" }}{{- $system = .Content }}{{- else if eq .Role "user" }}[INST] {{ if $system }}<<SYS>>
{{ $system }}
<</SYS>>

{{ $system = "" }}{{ end }}{{ .Content }} [/INST]{{- else if eq .Role "assistant" }} {{ .Content }}</s>{{- end }}{{- end }}{{- else }}[INST] {{ if .System }}<<SYS>>
{{ .System }}
<</SYS>>

{{ end }}{{ .Prompt }} [/INST]{{- end }} {{ .Response }}</s>`
}

func mistralOllamaChatTemplate() string {
	log.Trace("mistralOllamaChatTemplate")

	return `{{- if .Messages }}{{- $system := "" }}{{- range .Messages }}{{- if eq .Role "system" }}{{- $system = .Content }}{{- else if eq .Role "user" }}[INST] {{ if $system }}{{ $system }}

{{ $system = "" }}{{ end }}{{ .Content }} [/INST]{{- else if eq .Role "assistant" }} {{ .Content }}</s>{{- end }}{{- end }}{{- else }}[INST] {{ if .System }}{{ .System }}

{{ end }}{{ .Prompt }} [/INST]{{- end }} {{ .Response }}</s>`
}

func (r *Runtime) ensureOllamaChatModel(ctx context.Context, endpoint string, tag string) error {
	log.Trace("localserving Runtime ensureOllamaChatModel")

	if _, err := r.ollamaChatDefinition(ctx, endpoint, tag); err != nil {
		return err
	}
	return nil
}

type ollamaChatDefinition struct {
	Template   string
	Parameters string
}

func (r *Runtime) ollamaChatDefinition(ctx context.Context, endpoint string, tag string) (ollamaChatDefinition, error) {
	log.Trace("localserving Runtime ollamaChatDefinition")

	body, err := json.Marshal(map[string]any{"model": strings.TrimSpace(tag)})
	if err != nil {
		return ollamaChatDefinition{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(endpoint, "/")+ollamaShowPath, bytes.NewReader(body))
	if err != nil {
		return ollamaChatDefinition{}, fmt.Errorf("%w: build ollama show request: %w", domain.ErrValidationFailed, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.client.Do(req)
	if err != nil {
		return ollamaChatDefinition{}, fmt.Errorf("%w: read ollama model template: %w", domain.ErrValidationFailed, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return ollamaChatDefinition{}, domain.ErrValidationFailed.Extend(fmt.Sprintf("ollama show returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(raw))))
	}
	var payload map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return ollamaChatDefinition{}, fmt.Errorf("%w: decode ollama show response: %w", domain.ErrValidationFailed, err)
	}
	template := strings.TrimSpace(fmt.Sprint(payload["template"]))
	if template == "" {
		return ollamaChatDefinition{}, domain.ErrValidationFailed.Extend(fmt.Sprintf("local Ollama chat model %q is missing a chat template", tag))
	}
	if strings.Contains(template, "{%") {
		return ollamaChatDefinition{}, domain.ErrValidationFailed.Extend(fmt.Sprintf("local Ollama chat model %q stored a raw Hugging Face chat template", tag))
	}
	parameters := ollamaParametersString(payload["parameters"])
	if !ollamaParametersIncludeStop(parameters) {
		return ollamaChatDefinition{}, domain.ErrValidationFailed.Extend(fmt.Sprintf("local Ollama chat model %q is missing stop parameters", tag))
	}
	return ollamaChatDefinition{Template: template, Parameters: parameters}, nil
}

func stopParameters(stopTokens []string) map[string]any {
	log.Trace("stopParameters")

	stops := uniqueNonEmpty(stopTokens...)
	if len(stops) == 0 {
		return nil
	}
	return map[string]any{"stop": stops}
}

func mergeStopParameters(parameters map[string]any, stopTokens []string) map[string]any {
	log.Trace("mergeStopParameters")

	stops := uniqueNonEmpty(stopTokens...)
	if existing, ok := parameters["stop"]; ok {
		stops = uniqueNonEmpty(append(stops, stopValues(existing)...)...)
	}
	if len(stops) == 0 {
		return parameters
	}
	merged := make(map[string]any, len(parameters)+1)
	for key, value := range parameters {
		if key != "stop" {
			merged[key] = value
		}
	}
	merged["stop"] = stops
	return merged
}

func stopValues(value any) []string {
	log.Trace("stopValues")

	switch typed := value.(type) {
	case string:
		return []string{typed}
	case []string:
		return typed
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			out = append(out, fmt.Sprint(item))
		}
		return out
	default:
		return nil
	}
}

func uniqueNonEmpty(values ...string) []string {
	log.Trace("uniqueNonEmpty")

	out := make([]string, 0, len(values))
	seen := map[string]bool{}
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" || seen[trimmed] {
			continue
		}
		seen[trimmed] = true
		out = append(out, trimmed)
	}
	return out
}

func ollamaParametersString(value any) string {
	log.Trace("ollamaParametersString")

	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(typed)
	default:
		raw, err := json.Marshal(typed)
		if err != nil {
			return strings.TrimSpace(fmt.Sprint(typed))
		}
		return strings.TrimSpace(string(raw))
	}
}

func ollamaParametersIncludeStop(parameters string) bool {
	log.Trace("ollamaParametersIncludeStop")

	return strings.Contains(strings.ToLower(parameters), "stop")
}

func (r *Runtime) ensureOllamaTag(ctx context.Context, endpoint string, tag string) error {
	log.Trace("localserving Runtime ensureOllamaTag")

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(endpoint, "/")+ollamaTagsPath, nil)
	if err != nil {
		return fmt.Errorf("%w: build ollama tags request: %w", domain.ErrValidationFailed, err)
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return fmt.Errorf("%w: local ollama endpoint is not available: %w", domain.ErrValidationFailed, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return domain.ErrValidationFailed.Extend(fmt.Sprintf("local ollama endpoint returned status %d", resp.StatusCode))
	}
	var payload struct {
		Models []struct {
			Name string `json:"name"`
		} `json:"models"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return fmt.Errorf("%w: decode ollama tags response: %w", domain.ErrValidationFailed, err)
	}
	for _, candidate := range payload.Models {
		if normalizedOllamaTag(candidate.Name) == normalizedOllamaTag(tag) {
			return nil
		}
	}
	return domain.ErrValidationFailed.Extend(fmt.Sprintf("local ollama model %q is not available; run `ollama pull %s`", tag, tag))
}

func normalizedOllamaTag(tag string) string {
	trimmed := strings.TrimSpace(tag)
	if trimmed == "" || strings.Contains(trimmed, ":") {
		return trimmed
	}
	return trimmed + ":latest"
}

func isGGUFArtifact(format string) bool {
	normalized := normalizeToken(format)
	return normalized == artifactFormatGGUFModel ||
		normalized == artifactFormatGGUFLoRAAdapter ||
		normalized == legacyArtifactFormatGGUF
}

func isGGUFLoRAAdapter(format string) bool {
	return normalizeToken(format) == artifactFormatGGUFLoRAAdapter
}

func normalizeToken(value string) string {
	return strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(value), "-", "_"))
}

func deterministicServingTag(servedModel *model.ServedModel, checksum string) string {
	name := strings.TrimSpace(servedModel.Name)
	if name == "" {
		name = "model"
	}
	id := strings.ReplaceAll(servedModel.ModelID.String(), "-", "")
	if len(id) > 8 {
		id = id[:8]
	}
	checksumPart := checksumTagPart(checksum)
	suffix := fmt.Sprintf("-v%d-%s", servedModel.ModelVersion, id)
	if checksumPart != "" {
		suffix += "-" + checksumPart
	}
	const prefix = "bighill-"
	maxNameLength := maxOllamaModelNameLength - len(prefix) - len(suffix)
	if maxNameLength < 1 {
		maxNameLength = 1
	}
	tag := prefix + dnsTagPart(name, maxNameLength) + suffix
	if len(tag) > maxOllamaModelNameLength {
		overflow := len(tag) - maxOllamaModelNameLength
		namePart := dnsTagPart(name, maxNameLength)
		if len(namePart) > overflow {
			namePart = strings.Trim(namePart[:len(namePart)-overflow], "-")
		}
		if namePart == "" {
			namePart = "model"
		}
		tag = prefix + namePart + suffix
	}
	return tag
}

func checksumTagPart(checksum string) string {
	normalized := strings.TrimPrefix(normalizeChecksum(checksum), "sha256:")
	if normalized == "" {
		return ""
	}
	if len(normalized) > 12 {
		return normalized[:12]
	}
	return normalized
}

func dnsTagPart(value string, maxLength int) string {
	lower := strings.ToLower(strings.TrimSpace(value))
	var builder strings.Builder
	previousDash := false
	for _, r := range lower {
		valid := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if valid {
			builder.WriteRune(r)
			previousDash = false
			continue
		}
		if !previousDash {
			builder.WriteByte('-')
			previousDash = true
		}
	}
	out := strings.Trim(builder.String(), "-")
	if out == "" {
		return "model"
	}
	if maxLength < 1 {
		maxLength = 1
	}
	if len(out) > maxLength {
		out = strings.Trim(out[:maxLength], "-")
	}
	if out == "" {
		return "model"
	}
	return out
}

func normalizeChecksum(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}
	if strings.HasPrefix(trimmed, "sha256:") {
		return strings.ToLower(trimmed)
	}
	return "sha256:" + strings.ToLower(trimmed)
}

func fileMatchesChecksum(path string, expected string) (bool, error) {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("%w: stat artifact cache file: %w", domain.ErrModelServe, err)
	}
	if info.IsDir() {
		return false, domain.ErrModelServe.Extend("artifact cache path is a directory")
	}
	actual, err := fileChecksum(path)
	if err != nil {
		return false, err
	}
	return actual == normalizeChecksum(expected), nil
}

func fileChecksum(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("%w: open artifact cache file: %w", domain.ErrModelServe, err)
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", fmt.Errorf("%w: checksum artifact cache file: %w", domain.ErrModelServe, err)
	}
	return "sha256:" + hex.EncodeToString(hash.Sum(nil)), nil
}

func sha1Hex(value string) string {
	sum := sha1.Sum([]byte(value))
	return hex.EncodeToString(sum[:])
}
