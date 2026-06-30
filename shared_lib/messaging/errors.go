package messaging

import "errors"

var (
	ErrEnvelopeInvalid     = errors.New("message envelope invalid")
	ErrDispatchKeyRequired = errors.New("dispatch key required")
)
