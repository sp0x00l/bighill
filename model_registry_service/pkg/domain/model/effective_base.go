package model

import (
	"time"

	"github.com/google/uuid"
)

const EffectiveBaseDescriptorSchemaVersion = 1

type EffectiveBaseVersion struct {
	EffectiveBaseID         string
	FoundationModelID       uuid.UUID
	DescriptorSchemaVersion int
	FoundationChecksum      string
	Descriptor              string
	CreatedAt               time.Time
	UpdatedAt               time.Time
}

type EffectiveBaseDescriptor struct {
	DescriptorSchemaVersion int    `json:"descriptor_schema_version"`
	FoundationModelID       string `json:"foundation_model_id"`
	ArtifactURI             string `json:"artifact_uri"`
	ArtifactFormat          string `json:"artifact_format"`
	FoundationChecksum      string `json:"foundation_checksum"`
	ServingProtocol         string `json:"serving_protocol"`
	ServingModel            string `json:"serving_model"`
}
