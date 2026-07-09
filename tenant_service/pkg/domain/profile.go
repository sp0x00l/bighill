package domain

import (
	"time"

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
