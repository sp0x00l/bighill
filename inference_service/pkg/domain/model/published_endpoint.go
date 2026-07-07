package model

import "github.com/google/uuid"

type PublishedEndpointStatus string

const (
	PublishedEndpointStatusReady    PublishedEndpointStatus = "ready"
	PublishedEndpointStatusDisabled PublishedEndpointStatus = "disabled"
)

type PublishedEndpoint struct {
	EndpointID      uuid.UUID
	OrgID           uuid.UUID
	ModelID         uuid.UUID
	DatasetID       uuid.UUID
	Status          PublishedEndpointStatus
	DisplayName     string
	CreatedByUserID uuid.UUID
}

func (e PublishedEndpoint) IsReady() bool {
	return e.Status == PublishedEndpointStatusReady
}
