package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"model_serving_service/pkg/domain/model"
	"model_serving_service/pkg/infra/network/localserving"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestModelServingIntegration(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Model serving integration test suite")
}

var _ = Describe("GGUF local serving integration", func() {
	It("validates a real GGUF file through the production inspector command", func() {
		ggufPath := filepath.Join(GinkgoT().TempDir(), "tiny-llama3.gguf")
		writeTinyGGUF(ggufPath, llama3JinjaChatTemplate())

		inspection := inspectGGUF(ggufPath, true)

		Expect(inspection["architecture"]).To(Equal("llama"))
		Expect(inspection["chat_template_present"]).To(Equal(true))
		Expect(inspection["chat_template"]).To(ContainSubstring("<|start_header_id|>"))
		Expect(inspection["chat_template"]).To(ContainSubstring("add_generation_prompt"))
		Expect(inspection["stop_tokens"]).To(ConsistOf("<|eot_id|>", "<|custom_stop|>"))
		Expect(inspection["tensor_count"]).To(BeNumerically(">", 0))
	})

	It("rejects a real GGUF chat artifact when tokenizer.chat_template is missing", func() {
		ggufPath := filepath.Join(GinkgoT().TempDir(), "tiny-base.gguf")
		writeTinyGGUF(ggufPath, "")

		_, stderr, err := runInspector(ggufPath, true)

		Expect(err).To(HaveOccurred())
		Expect(stderr).To(ContainSubstring("tokenizer.chat_template"))
	})

	It("provisions a real GGUF artifact into real Ollama when explicitly configured", Label("real-ollama"), func() {
		configuredArtifactPath := strings.TrimSpace(os.Getenv("BIGHILL_MODEL_SERVING_REAL_OLLAMA_GGUF_PATH"))
		if configuredArtifactPath == "" {
			Fail("set BIGHILL_MODEL_SERVING_REAL_OLLAMA_GGUF_PATH to run real Ollama GGUF provisioning integration")
		}
		artifactPath, err := filepath.Abs(configuredArtifactPath)
		Expect(err).NotTo(HaveOccurred())
		endpoint := strings.TrimRight(envOrDefault("MODEL_SERVING_SERVICE_LOCAL_OLLAMA_ENDPOINT", "http://localhost:11434"), "/")
		requireRealOllama(endpoint)

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
		defer cancel()

		checksum := fileSHA256(artifactPath)
		modelID := uuid.New()
		createdModelName := ""
		DeferCleanup(func() {
			if createdModelName != "" {
				deleteOllamaModel(endpoint, createdModelName)
			}
		})
		runtime, err := localserving.NewRuntime("default", 8080, endpoint,
			localserving.WithArtifactCache(GinkgoT().TempDir()),
			localserving.WithGGUFInspectorCommand("sh "+filepath.Join(repoRoot(), "model_serving_service", "scripts", "gguf-inspector.sh")),
			localserving.WithCreateTimeout(20*time.Minute),
		)
		Expect(err).NotTo(HaveOccurred())

		state, err := runtime.EnsureServedModel(ctx, &model.ServedModel{
			ModelID:          modelID,
			ModelKind:        "BASE",
			Name:             "real-ollama-gguf",
			ModelVersion:     1,
			BaseModel:        filepath.Base(artifactPath),
			ArtifactLocation: "file://" + artifactPath,
			ArtifactFormat:   "GGUF_MODEL",
			ArtifactChecksum: checksum,
		})

		Expect(err).NotTo(HaveOccurred())
		createdModelName = state.ServingModel
		Expect(state.Ready).To(BeTrue())
		Expect(state.ServingProtocol).To(Equal(model.ServingProtocolOpenAIChatCompletions))
		Expect(state.ServingModel).To(ContainSubstring(strings.TrimPrefix(checksum, "sha256:")[:12]))
		definition := readOllamaChatDefinition(endpoint, state.ServingModel)
		Expect(definition.Template).To(SatisfyAny(ContainSubstring(".Messages"), ContainSubstring(".Prompt")))
		Expect(definition.Template).NotTo(ContainSubstring("{%"))
		Expect(strings.ToLower(definition.Parameters)).To(ContainSubstring("stop"))
	})
})

func inspectGGUF(path string, requireChatTemplate bool) map[string]any {
	stdout, stderr, err := runInspector(path, requireChatTemplate)
	Expect(err).NotTo(HaveOccurred(), stderr)
	var payload map[string]any
	Expect(json.Unmarshal(stdout, &payload)).To(Succeed())
	return payload
}

func runInspector(path string, requireChatTemplate bool) ([]byte, string, error) {
	args := []string{filepath.Join(repoRoot(), "model_serving_service", "scripts", "gguf-inspector.sh")}
	if requireChatTemplate {
		args = append(args, "--require-chat-template")
	}
	args = append(args, path)
	cmd := exec.Command("sh", args...)
	cmd.Env = append(os.Environ(), "BIGHILL_ROOT="+repoRoot())
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, err := cmd.Output()
	return stdout, stderr.String(), err
}

func writeTinyGGUF(path string, chatTemplate string) {
	source := `
from pathlib import Path
import sys
import numpy as np
from gguf import GGUFWriter

path = Path(sys.argv[1])
chat_template = sys.argv[2]
writer = GGUFWriter(path, "llama")
writer.add_name("tiny-llama3")
writer.add_token_list(["<unk>", "<|eot_id|>", "<|custom_stop|>"])
writer.add_eos_token_id(1)
writer.add_eot_token_id(2)
if chat_template:
    writer.add_chat_template(chat_template)
writer.add_tensor("token_embd.weight", np.zeros((1, 1), dtype=np.float32))
writer.write_header_to_file()
writer.write_kv_data_to_file()
writer.write_tensors_to_file()
writer.close()
`
	cmd := exec.Command(python311(), "-c", source, path, chatTemplate)
	cmd.Env = os.Environ()
	output, err := cmd.CombinedOutput()
	Expect(err).NotTo(HaveOccurred(), string(output))
}

func python311() string {
	if configured := strings.TrimSpace(os.Getenv("BIGHILL_MODEL_ARTIFACTS_PYTHON")); configured != "" {
		return configured
	}
	pyenv := filepath.Join(os.Getenv("HOME"), ".pyenv", "versions", "3.11.9", "bin", "python")
	if _, err := os.Stat(pyenv); err == nil {
		return pyenv
	}
	if found, err := exec.LookPath("python3.11"); err == nil {
		return found
	}
	return "python3"
}

func llama3JinjaChatTemplate() string {
	return "{% set loop_messages = messages %}{% for message in loop_messages %}{% set content = '<|start_header_id|>' + message['role'] + '<|end_header_id|>\\n\\n'+ message['content'] | trim + '<|eot_id|>' %}{% if loop.index0 == 0 %}{% set content = bos_token + content %}{% endif %}{{ content }}{% endfor %}{% if add_generation_prompt %}{{ '<|start_header_id|>assistant<|end_header_id|>\\n\\n' }}{% endif %}"
}

func requireRealOllama(endpoint string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"/api/version", nil)
	Expect(err).NotTo(HaveOccurred())
	resp, err := http.DefaultClient.Do(req)
	Expect(err).NotTo(HaveOccurred(), "real Ollama must be running at %s", endpoint)
	defer resp.Body.Close()
	Expect(resp.StatusCode).To(Equal(http.StatusOK))
}

type ollamaChatDefinition struct {
	Template   string
	Parameters string
}

func readOllamaChatDefinition(endpoint string, modelName string) ollamaChatDefinition {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	body, err := json.Marshal(map[string]any{"model": modelName})
	Expect(err).NotTo(HaveOccurred())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/api/show", bytes.NewReader(body))
	Expect(err).NotTo(HaveOccurred())
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	Expect(err).NotTo(HaveOccurred())
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	Expect(err).NotTo(HaveOccurred())
	Expect(resp.StatusCode).To(Equal(http.StatusOK), string(raw))
	var payload map[string]any
	Expect(json.Unmarshal(raw, &payload)).To(Succeed())
	parameters := ""
	switch typed := payload["parameters"].(type) {
	case string:
		parameters = typed
	default:
		rawParameters, err := json.Marshal(typed)
		Expect(err).NotTo(HaveOccurred())
		parameters = string(rawParameters)
	}
	return ollamaChatDefinition{
		Template:   strings.TrimSpace(fmt.Sprint(payload["template"])),
		Parameters: strings.TrimSpace(parameters),
	}
}

func deleteOllamaModel(endpoint string, modelName string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	body, _ := json.Marshal(map[string]any{"model": modelName})
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, endpoint+"/api/delete", bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err == nil && resp != nil {
		_ = resp.Body.Close()
	}
}

func fileSHA256(path string) string {
	file, err := os.Open(path)
	Expect(err).NotTo(HaveOccurred())
	defer file.Close()
	hash := sha256.New()
	_, err = io.Copy(hash, file)
	Expect(err).NotTo(HaveOccurred())
	return "sha256:" + hex.EncodeToString(hash.Sum(nil))
}

func repoRoot() string {
	wd, err := os.Getwd()
	Expect(err).NotTo(HaveOccurred())
	for dir := wd; dir != filepath.Dir(dir); dir = filepath.Dir(dir) {
		if _, err := os.Stat(filepath.Join(dir, "shared_py")); err == nil {
			return dir
		}
	}
	Fail("repository root not found")
	return ""
}

func envOrDefault(name string, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
