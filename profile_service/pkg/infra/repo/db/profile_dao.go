package db

import (
	"strings"

	"lib/shared_lib/idem"

	"profile_service/pkg/domain"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type ProfileDAO struct {
	ID                         pgtype.UUID
	DefaultOrgID               pgtype.UUID
	Email                      pgtype.Text
	EmailVerified              pgtype.Bool
	FirstName                  pgtype.Text
	LastName                   pgtype.Text
	PhoneNumber                pgtype.Text
	DateOfBirth                pgtype.Date
	CountryCode                pgtype.Text
	AddressLine1               pgtype.Text
	AddressLine2               pgtype.Text
	City                       pgtype.Text
	State                      pgtype.Text
	PostalCode                 pgtype.Text
	Country                    pgtype.Text
	HuggingFaceTokenCiphertext pgtype.Text
}

type ProfileAccountDAO struct {
	ID                   pgtype.UUID
	DefaultOrgID         pgtype.UUID
	Email                pgtype.Text
	PhoneNumber          pgtype.Text
	CountryCode          pgtype.Text
	Password             pgtype.Text
	EmailVerified        pgtype.Bool
	EmailVerifyTokenHash pgtype.Text
	EmailVerifyExpiresAt pgtype.Timestamp
}

type OAuthIdentityDAO struct {
	ProfileID     pgtype.UUID
	Provider      pgtype.Text
	ProviderSub   pgtype.Text
	Email         pgtype.Text
	EmailVerified pgtype.Bool
}

type OAuthProfileDAO struct {
	Email        pgtype.Text
	PhoneNumber  pgtype.Text
	CountryCode  pgtype.Text
	PasswordHash pgtype.Text
	FirstName    pgtype.Text
	LastName     pgtype.Text
}

type ProfileIDDAO struct {
	ID pgtype.UUID
}

type OrganizationDAO struct {
	ID              pgtype.UUID
	DisplayName     pgtype.Text
	CreatedByUserID pgtype.UUID
	CreatedAt       pgtype.Timestamptz
	UpdatedAt       pgtype.Timestamptz
}

type OrganizationMembershipDAO struct {
	OrgID           pgtype.UUID
	UserID          pgtype.UUID
	Email           pgtype.Text
	Role            pgtype.Text
	Status          pgtype.Text
	CreatedByUserID pgtype.UUID
	CreatedAt       pgtype.Timestamptz
	UpdatedAt       pgtype.Timestamptz
}

type OAuthProfileIDDAO struct {
	ProfileID pgtype.UUID
}

func ToDAO(profile *domain.Profile, userID uuid.UUID) pgx.NamedArgs {
	args := pgx.NamedArgs{
		"id":             pgtype.UUID{Bytes: userID, Valid: true},
		"email":          pgtype.Text{String: profile.Email, Valid: true},
		"email_verified": pgtype.Bool{Bool: profile.EmailVerified, Valid: true},
		"first_name":     pgtype.Text{String: profile.FirstName, Valid: true},
		"last_name":      pgtype.Text{String: profile.LastName, Valid: true},
		"phone_number":   pgtype.Text{String: profile.PhoneNumber, Valid: true},
		"date_of_birth":  pgtype.Date{Time: profile.DateOfBirth, Valid: true},
		"country_code":   pgtype.Text{String: profile.CountryCode, Valid: true},
		"address_line_1": pgtype.Text{String: profile.AddressLine1, Valid: true},
		"address_line_2": pgtype.Text{String: profile.AddressLine2, Valid: true},
		"city":           pgtype.Text{String: profile.City, Valid: true},
		"state":          pgtype.Text{String: profile.State, Valid: true},
		"postal_code":    pgtype.Text{String: profile.PostalCode, Valid: true},
		"country":        pgtype.Text{String: profile.Country, Valid: true},
	}

	// email, phone must be unique, if empty, they should be nullable due to the unique constraint
	if profile.Email != "" {
		args["email"] = pgtype.Text{String: profile.Email, Valid: true}
	} else {
		args["email"] = pgtype.Text{Valid: false}
	}

	if profile.PhoneNumber != "" {
		args["phone_number"] = pgtype.Text{String: profile.PhoneNumber, Valid: true}
	} else {
		args["phone_number"] = pgtype.Text{Valid: false}
	}

	return args
}

func ToDAOProfileAccount(profileAccount *domain.ProfileAccount) pgx.NamedArgs {
	return pgx.NamedArgs{
		"email":                   pgtype.Text{String: profileAccount.Email, Valid: true},
		"phone_number":            pgtype.Text{String: profileAccount.PhoneNumber, Valid: true},
		"country_code":            pgtype.Text{String: profileAccount.CountryCode, Valid: true},
		"password_hash":           pgtype.Text{String: profileAccount.Password, Valid: true},
		"email_verified":          pgtype.Bool{Bool: profileAccount.EmailVerified, Valid: true},
		"email_verify_token_hash": pgtype.Text{String: hashVerificationToken(profileAccount.EmailVerifyToken), Valid: profileAccount.EmailVerifyToken != ""},
		"email_verify_expires_at": pgtype.Timestamp{Time: profileAccount.EmailVerifyExpiresAt, Valid: !profileAccount.EmailVerifyExpiresAt.IsZero()},
	}
}

func ToDAOOAuthIdentity(userID uuid.UUID, identity domain.OAuthIdentity) pgx.NamedArgs {
	return pgx.NamedArgs{
		"profile_id":       pgtype.UUID{Bytes: userID, Valid: true},
		"provider":         pgtype.Text{String: identity.Provider, Valid: true},
		"provider_subject": pgtype.Text{String: identity.Subject, Valid: true},
		"email":            pgtype.Text{String: identity.Email, Valid: true},
		"email_verified":   pgtype.Bool{Bool: identity.EmailVerified, Valid: true},
	}
}

func ToDAOOAuthProfile(identity domain.OAuthIdentity, passwordHash string) pgx.NamedArgs {
	idempotencyKey := idem.FromParts(
		idem.OAuthProfile,
		strings.ToLower(strings.TrimSpace(identity.Provider)),
		strings.TrimSpace(identity.Subject),
	)
	return pgx.NamedArgs{
		"idempotency_key": pgtype.UUID{Bytes: idempotencyKey, Valid: true},
		"email":           pgtype.Text{String: identity.Email, Valid: true},
		"phone_number":    pgtype.Text{Valid: false},
		"country_code":    pgtype.Text{Valid: false},
		"password_hash":   pgtype.Text{String: passwordHash, Valid: true},
		"first_name":      pgtype.Text{String: identity.FirstName, Valid: identity.FirstName != ""},
		"last_name":       pgtype.Text{String: identity.LastName, Valid: identity.LastName != ""},
	}
}

func FromDAOProfileID(dao *ProfileIDDAO) uuid.UUID {
	return dao.ID.Bytes
}

func FromDAOOAuthProfileID(dao *OAuthProfileIDDAO) uuid.UUID {
	return dao.ProfileID.Bytes
}

func FromDAO(dao *ProfileDAO) (*domain.Profile, error) {
	return &domain.Profile{
		ProfileAccount: domain.ProfileAccount{
			ID:                         dao.ID.Bytes,
			DefaultOrgID:               dao.DefaultOrgID.Bytes,
			Email:                      dao.Email.String,
			PhoneNumber:                dao.PhoneNumber.String,
			CountryCode:                dao.CountryCode.String,
			HuggingFaceTokenCiphertext: dao.HuggingFaceTokenCiphertext.String,
			EmailVerified:              dao.EmailVerified.Bool,
		},
		FirstName:    dao.FirstName.String,
		LastName:     dao.LastName.String,
		DateOfBirth:  dao.DateOfBirth.Time,
		AddressLine1: dao.AddressLine1.String,
		AddressLine2: dao.AddressLine2.String,
		City:         dao.City.String,
		State:        dao.State.String,
		PostalCode:   dao.PostalCode.String,
		Country:      dao.Country.String,
	}, nil
}

func FromDAOProfileAccount(dao *ProfileAccountDAO) *domain.ProfileAccount {
	return &domain.ProfileAccount{
		ID:                   dao.ID.Bytes,
		DefaultOrgID:         dao.DefaultOrgID.Bytes,
		Email:                dao.Email.String,
		PhoneNumber:          dao.PhoneNumber.String,
		CountryCode:          dao.CountryCode.String,
		EmailVerified:        dao.EmailVerified.Bool,
		EmailVerifyExpiresAt: dao.EmailVerifyExpiresAt.Time,
	}
}

func FromDAOOrganization(dao *OrganizationDAO) *domain.Organization {
	return &domain.Organization{
		ID:              dao.ID.Bytes,
		DisplayName:     dao.DisplayName.String,
		CreatedByUserID: dao.CreatedByUserID.Bytes,
		CreatedAt:       dao.CreatedAt.Time,
		UpdatedAt:       dao.UpdatedAt.Time,
	}
}

func FromDAOOrganizationMembership(dao *OrganizationMembershipDAO) *domain.OrganizationMembership {
	return &domain.OrganizationMembership{
		OrgID:           dao.OrgID.Bytes,
		UserID:          dao.UserID.Bytes,
		Email:           dao.Email.String,
		Role:            dao.Role.String,
		Status:          dao.Status.String,
		CreatedByUserID: dao.CreatedByUserID.Bytes,
		CreatedAt:       dao.CreatedAt.Time,
		UpdatedAt:       dao.UpdatedAt.Time,
	}
}
