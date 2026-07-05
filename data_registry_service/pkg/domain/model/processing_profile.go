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
		return "GENERIC_PARQUET_PROCESSING_PROFILE"
	case TextRAGProfile:
		return "TEXT_RAG_PROCESSING_PROFILE"
	case InstructionTuningProfile:
		return "INSTRUCTION_TUNING_PROCESSING_PROFILE"
	default:
		return "GENERIC_PARQUET_PROCESSING_PROFILE"
	}
}

func ToProcessingProfile(value string) (ProcessingProfile, error) {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "", "GENERIC_PARQUET_PROCESSING_PROFILE":
		return GenericParquetProfile, nil
	case "TEXT_RAG_PROCESSING_PROFILE":
		return TextRAGProfile, nil
	case "INSTRUCTION_TUNING_PROCESSING_PROFILE":
		return InstructionTuningProfile, nil
	default:
		return 0, errors.New("invalid ProcessingProfile")
	}
}
