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
		return "GENERIC_PARQUET"
	case ProcessingProfileTextRAG:
		return "TEXT_RAG"
	case ProcessingProfileInstructionTuning:
		return "INSTRUCTION_TUNING"
	default:
		return "GENERIC_PARQUET"
	}
}

func (p ProcessingProfile) RequiresEmbeddings() bool {
	return p == ProcessingProfileTextRAG
}

func ToProcessingProfile(value string) (ProcessingProfile, error) {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "", "GENERIC_PARQUET":
		return ProcessingProfileGenericParquet, nil
	case "TEXT_RAG":
		return ProcessingProfileTextRAG, nil
	case "INSTRUCTION_TUNING":
		return ProcessingProfileInstructionTuning, nil
	default:
		return 0, fmt.Errorf("invalid processing profile %q", value)
	}
}
