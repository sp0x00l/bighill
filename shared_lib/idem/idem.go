package idem

import (
	"strings"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

const (
	OAuthProfile = "oauth-profile"
	Outbox       = "outbox"
)

// FromParts derives a deterministic idempotency key from free-form key parts.
// Use this when the owner is not a single primary UUID, such as an outbox event
// identified by aggregate type plus aggregate ID plus sequence.
func FromParts(parts ...string) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(Join(parts...)))
}

// Key derives a deterministic idempotency key rooted at one owning UUID. Use it
// for child operations of an existing order, transaction, provision, or workflow.
func Key(seed uuid.UUID, parts ...string) uuid.UUID {
	return FromParts(append([]string{seed.String()}, parts...)...)
}

// Join returns the canonical SHA-1 seed. Empty parts are programmer errors:
// dropping one would make Key(seed, "a", "") collide with Key(seed, "a").
func Join(parts ...string) string {
	for _, part := range parts {
		if part == "" {
			log.Fatal("idem: empty key part")
		}
	}
	return strings.Join(parts, ":")
}
