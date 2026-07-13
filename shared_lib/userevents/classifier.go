package userevents

import (
	"strings"

	log "github.com/sirupsen/logrus"
)

const (
	ErrorCodeModelServingMissingChatTemplate    = "model_serving_missing_chat_template"
	ErrorCodeModelServingChatDefinitionUnusable = "model_serving_chat_definition_unusable"
	ErrorCodeModelServingRuntimeUnavailable     = "model_serving_runtime_unavailable"
	ErrorCodeEmbeddingProviderUnavailable       = "embedding_provider_unavailable"
	ErrorCodeArtifactStoreUnavailable           = "artifact_store_unavailable"
	ErrorCodeUnknown                            = "unknown_async_failure"
)

type ClassificationInput struct {
	Service          string
	Operation        string
	ResourceType     string
	DomainErrorCode  string
	RawFailureReason string
}

func ClassifyError(input ClassificationInput) ErrorDetail {
	log.Trace("ClassifyError")

	raw := strings.ToLower(strings.TrimSpace(input.RawFailureReason))
	switch {
	case strings.Contains(raw, "tokenizer.chat_template"):
		return ErrorDetail{
			Code:            ErrorCodeModelServingMissingChatTemplate,
			Message:         "The model could not be exposed as a chat model.",
			TechnicalDetail: input.RawFailureReason,
			Retryable:       false,
			Remediation:     "Use a GGUF model with a supported chat template or create a valid Ollama Modelfile.",
		}
	case strings.Contains(raw, "usable chat") || strings.Contains(raw, "chat model"):
		return ErrorDetail{
			Code:            ErrorCodeModelServingChatDefinitionUnusable,
			Message:         "The model could not be exposed as a chat model.",
			TechnicalDetail: input.RawFailureReason,
			Retryable:       false,
			Remediation:     "Use a model with a supported chat template or serving definition.",
		}
	case strings.Contains(raw, "ollama") && (strings.Contains(raw, "connection refused") || strings.Contains(raw, "unavailable") || strings.Contains(raw, "deadline exceeded")):
		return ErrorDetail{
			Code:            ErrorCodeModelServingRuntimeUnavailable,
			Message:         "The model serving runtime is not available.",
			TechnicalDetail: input.RawFailureReason,
			Retryable:       true,
			Remediation:     "Retry after the model serving runtime is healthy.",
		}
	case strings.Contains(raw, "embedding") && (strings.Contains(raw, "failed") || strings.Contains(raw, "unavailable") || strings.Contains(raw, "deadline exceeded")):
		return ErrorDetail{
			Code:            ErrorCodeEmbeddingProviderUnavailable,
			Message:         "The embedding provider is not available.",
			TechnicalDetail: input.RawFailureReason,
			Retryable:       true,
			Remediation:     "Retry after the embedding provider is healthy.",
		}
	case strings.Contains(raw, "object store") || strings.Contains(raw, "s3"):
		return ErrorDetail{
			Code:            ErrorCodeArtifactStoreUnavailable,
			Message:         "The artifact store is not available.",
			TechnicalDetail: input.RawFailureReason,
			Retryable:       true,
			Remediation:     "Retry after the artifact store is healthy.",
		}
	default:
		code := strings.TrimSpace(input.DomainErrorCode)
		if code == "" {
			code = ErrorCodeUnknown
		}
		return ErrorDetail{
			Code:            code,
			Message:         "The background operation failed.",
			TechnicalDetail: input.RawFailureReason,
			Retryable:       false,
		}
	}
}
