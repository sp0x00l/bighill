package domain

import (
	"time"

	"lib/shared_lib/authz"

	"github.com/google/uuid"
)

const (
	OrgMemberRoleConsumer     = authz.RoleConsumer
	OrgMemberRoleMLResearcher = authz.RoleMLResearcher
	OrgMemberRoleOrgAdmin     = authz.RoleOrgAdmin

	OrgMemberStatusActive   = "active"
	OrgMemberStatusInvited  = "invited"
	OrgMemberStatusDisabled = "disabled"
)

type Organization struct {
	ID              uuid.UUID
	DisplayName     string
	CreatedByUserID uuid.UUID
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

type OrganizationMembership struct {
	OrgID           uuid.UUID
	UserID          uuid.UUID
	Email           string
	Role            string
	Status          string
	CreatedByUserID uuid.UUID
	CreatedAt       time.Time
	UpdatedAt       time.Time
}
