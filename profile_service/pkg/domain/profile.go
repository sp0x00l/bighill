package domain

import (
	"time"

	"lib/shared_lib/authz"

	"github.com/google/uuid"
)

type ProfileAccount struct {
	ID                         uuid.UUID
	DefaultOrgID               uuid.UUID
	Email                      string
	PhoneNumber                string
	CountryCode                string
	Password                   string
	EmailVerified              bool
	HuggingFaceTokenCiphertext string
	EmailVerifyToken           string
	EmailVerifyExpiresAt       time.Time
}

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

type Profile struct {
	ProfileAccount
	FirstName    string
	LastName     string
	DateOfBirth  time.Time
	AddressLine1 string
	AddressLine2 string
	City         string
	State        string
	PostalCode   string
	Country      string
}

type OAuthIdentity struct {
	Provider      string
	Subject       string
	Email         string
	EmailVerified bool
	FirstName     string
	LastName      string
}

type OAuthState struct {
	State         string
	Provider      string
	RedirectURI   string
	CodeChallenge string
}
