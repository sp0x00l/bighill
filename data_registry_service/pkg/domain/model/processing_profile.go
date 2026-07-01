package model

import (
	"errors"
	"strings"
)

type ProcessingProfile int

const (
	GenericParquetProfile ProcessingProfile = iota
	TextRAGProfile
	InstructionTuningProfile
)

func (p ProcessingProfile) String() string {
	switch p {
	case GenericParquetProfile:
		return "GENERIC_PARQUET"
	case TextRAGProfile:
		return "TEXT_RAG"
	case InstructionTuningProfile:
		return "INSTRUCTION_TUNING"
	default:
		return "GENERIC_PARQUET"
	}
}

func ToProcessingProfile(value string) (ProcessingProfile, error) {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "", "GENERIC_PARQUET":
		return GenericParquetProfile, nil
	case "TEXT_RAG":
		return TextRAGProfile, nil
	case "INSTRUCTION_TUNING":
		return InstructionTuningProfile, nil
	default:
		return 0, errors.New("invalid ProcessingProfile")
	}
}
