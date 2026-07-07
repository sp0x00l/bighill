package model

import (
	"fmt"
	"strings"
)

type ServingProtocol int

const (
	ServingProtocolUnknown ServingProtocol = iota
	ServingProtocolOllamaGenerate
	ServingProtocolOpenAIChatCompletions
)

func (p ServingProtocol) String() string {
	if p < ServingProtocolOllamaGenerate || p > ServingProtocolOpenAIChatCompletions {
		return ""
	}
	return [...]string{"OLLAMA_GENERATE", "OPENAI_CHAT_COMPLETIONS"}[p-ServingProtocolOllamaGenerate]
}

func ToServingProtocol(value string) (ServingProtocol, error) {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "":
		return ServingProtocolUnknown, nil
	case ServingProtocolOllamaGenerate.String():
		return ServingProtocolOllamaGenerate, nil
	case ServingProtocolOpenAIChatCompletions.String():
		return ServingProtocolOpenAIChatCompletions, nil
	default:
		return ServingProtocolUnknown, fmt.Errorf("invalid serving protocol %q", value)
	}
}
