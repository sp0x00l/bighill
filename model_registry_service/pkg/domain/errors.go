package domain

import "errors"

var (
	ErrValidationFailed = errors.New("validation failed")
	ErrModelNotFound    = errors.New("model not found")
	ErrModelExists      = errors.New("model already exists")
)
