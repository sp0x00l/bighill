package model

import (
	"fmt"
	"strings"
)

type ProcessingProfile int

const (
	ProcessingProfileGenericParquet ProcessingProfile = iota
	ProcessingProfileTextRAG
	ProcessingProfileInstructionTuning
)

func (p ProcessingProfile) String() string {
	switch p {
	case ProcessingProfileGenericParquet:
		return "GENERIC_PARQUET_PROCESSING_PROFILE"
	case ProcessingProfileTextRAG:
		return "TEXT_RAG_PROCESSING_PROFILE"
	case ProcessingProfileInstructionTuning:
		return "INSTRUCTION_TUNING_PROCESSING_PROFILE"
	default:
		return "UNKNOWN"
	}
}

func (p ProcessingProfile) RequiresEmbeddings() bool {
	return p == ProcessingProfileTextRAG
}

func (p ProcessingProfile) RequiresGraph() bool {
	return p == ProcessingProfileTextRAG
}

func ToProcessingProfile(value string) (ProcessingProfile, error) {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "GENERIC_PARQUET_PROCESSING_PROFILE":
		return ProcessingProfileGenericParquet, nil
	case "TEXT_RAG_PROCESSING_PROFILE":
		return ProcessingProfileTextRAG, nil
	case "INSTRUCTION_TUNING_PROCESSING_PROFILE":
		return ProcessingProfileInstructionTuning, nil
	default:
		return 0, fmt.Errorf("invalid processing profile %q", value)
	}
}
