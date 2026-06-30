package domain

import (
	"time"

	"github.com/google/uuid"
)

type ProfileAccount struct {
	ID                   uuid.UUID
	Email                string
	PhoneNumber          string
	CountryCode          string
	Password             string
	EmailVerified        bool
	EmailVerifyToken     string
	EmailVerifyExpiresAt time.Time
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
