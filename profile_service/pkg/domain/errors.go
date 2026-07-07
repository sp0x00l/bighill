package domain

import "errors"

var (
	ErrProfileNotFound          = errors.New("profile not found")
	ErrProfileAlreadyExists     = errors.New("profile already exists")
	ErrOAuthIdentityNotFound    = errors.New("oauth identity not found")
	ErrEmailNotVerified         = errors.New("email not verified")
	ErrUnsupportedOAuthProvider = errors.New("unsupported oauth provider")
	ErrInvalidOAuthState        = errors.New("invalid oauth state")
	ErrInvalidOAuthCode         = errors.New("invalid oauth code")
	ErrOAuthEmailRequired       = errors.New("oauth provider did not return an email")
	ErrOAuthEmailUnverified     = errors.New("oauth provider email is not verified")
	ErrValidationFailed         = errors.New("validation failed")
	ErrUnauthorized             = errors.New("unauthorized")
)
