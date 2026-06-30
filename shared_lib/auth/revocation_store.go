package provider

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/redis/rueidis"
	log "github.com/sirupsen/logrus"
)

type RevocationStore interface {
	RevokeToken(ctx context.Context, jti string, expUnix int64) error
	IsRevoked(ctx context.Context, jti string) (bool, error)

	SetUserRevokedAfter(ctx context.Context, userID string, unixTs int64) error
	GetUserRevokedAfter(ctx context.Context, userID string) (int64, error)
	ClearUserRevokedAfter(ctx context.Context, userID string) error

	// Optional per-session allowlist (device-specific control)
	CreateSession(ctx context.Context, sid string, expUnix int64) error
	SessionExists(ctx context.Context, sid string) (bool, error)
	DeleteSession(ctx context.Context, sid string) error
}

func NewRevocationStore(c rueidis.Client, opts ...AuthRevocationStoreOption) RevocationStore {
	log.Trace("NewRevocationStore")
	return NewAuthRevocationStore(c, opts...)
}

type authRevocationStore struct {
	c      rueidis.Client
	prefix string
	now    func() time.Time
}

type AuthRevocationStoreOption func(*authRevocationStore)

func WithKeyPrefix(prefix string) AuthRevocationStoreOption {
	log.Trace("WithKeyPrefix")
	return func(s *authRevocationStore) { s.prefix = prefix }
}

func WithClock(now func() time.Time) AuthRevocationStoreOption {
	log.Trace("WithClock")
	return func(s *authRevocationStore) { s.now = now }
}

func NewAuthRevocationStore(c rueidis.Client, opts ...AuthRevocationStoreOption) *authRevocationStore {
	log.Trace("NewAuthRevocationStore")

	s := &authRevocationStore{
		c:      c,
		prefix: "auth:",
		now:    func() time.Time { return time.Now() },
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

func (s *authRevocationStore) RevokeToken(ctx context.Context, jti string, expUnix int64) error {
	log.Trace("authRevocationStore RevokeToken")

	if jti == "" {
		return errors.New("jti is empty")
	}
	ttl := s.ttlFromExp(expUnix)
	if ttl <= 0 {
		// Token already expired; setting a 1s TTL keeps behavior predictable and avoids a permanent key.
		ttl = time.Second
	}
	cmd := s.c.B().Set().Key(s.keyJTI(jti)).Value("1").Ex(ttl).Build()
	return s.c.Do(ctx, cmd).Error()
}

func (s *authRevocationStore) IsRevoked(ctx context.Context, jti string) (bool, error) {
	log.Trace("authRevocationStore IsRevoked")

	if jti == "" {
		return false, errors.New("jti is empty")
	}
	cmd := s.c.B().Exists().Key(s.keyJTI(jti)).Build()
	n, err := s.c.Do(ctx, cmd).AsInt64()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

func (s *authRevocationStore) SetUserRevokedAfter(ctx context.Context, userID string, unixTs int64) error {
	log.Trace("authRevocationStore SetUserRevokedAfter")

	if userID == "" {
		return errors.New("userID is empty")
	}
	cmd := s.c.B().Set().Key(s.keyUserRevokedAfter(userID)).Value(strconv.FormatInt(unixTs, 10)).Build()
	return s.c.Do(ctx, cmd).Error()
}

func (s *authRevocationStore) GetUserRevokedAfter(ctx context.Context, userID string) (int64, error) {
	log.Trace("authRevocationStore GetUserRevokedAfter")

	if userID == "" {
		return 0, errors.New("userID is empty")
	}
	cmd := s.c.B().Get().Key(s.keyUserRevokedAfter(userID)).Build()
	res := s.c.Do(ctx, cmd)
	if res.Error() != nil {
		if errors.Is(res.Error(), rueidis.Nil) {
			return 0, nil
		}
		return 0, res.Error()
	}
	str, err := res.ToString()
	if err != nil {
		return 0, err
	}
	ts, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		return 0, err
	}
	return ts, nil
}

func (s *authRevocationStore) ClearUserRevokedAfter(ctx context.Context, userID string) error {
	log.Trace("authRevocationStore ClearUserRevokedAfter")

	if userID == "" {
		return errors.New("userID is empty")
	}
	cmd := s.c.B().Del().Key(s.keyUserRevokedAfter(userID)).Build()
	return s.c.Do(ctx, cmd).Error()
}

func (s *authRevocationStore) CreateSession(ctx context.Context, sid string, expUnix int64) error {
	log.Trace("authRevocationStore CreateSession")

	if sid == "" {
		return errors.New("sid is empty")
	}
	ttl := s.ttlFromExp(expUnix)
	if ttl <= 0 {
		ttl = time.Second
	}
	cmd := s.c.B().Set().Key(s.keySession(sid)).Value("1").Ex(ttl).Build()
	return s.c.Do(ctx, cmd).Error()
}

func (s *authRevocationStore) SessionExists(ctx context.Context, sid string) (bool, error) {
	log.Trace("authRevocationStore SessionExists")

	if sid == "" {
		return false, errors.New("sid is empty")
	}
	cmd := s.c.B().Exists().Key(s.keySession(sid)).Build()
	n, err := s.c.Do(ctx, cmd).AsInt64()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

func (s *authRevocationStore) DeleteSession(ctx context.Context, sid string) error {
	log.Trace("authRevocationStore DeleteSession")

	if sid == "" {
		return errors.New("sid is empty")
	}
	cmd := s.c.B().Del().Key(s.keySession(sid)).Build()
	return s.c.Do(ctx, cmd).Error()
}

func (s *authRevocationStore) ttlFromExp(expUnix int64) time.Duration {
	log.Trace("authRevocationStore ttlFromExp")

	if expUnix <= 0 {
		return 0
	}
	now := s.now().Unix()
	sec := expUnix - now
	if sec <= 0 {
		return 0
	}
	return time.Duration(sec) * time.Second
}

func (s *authRevocationStore) keyJTI(jti string) string {
	log.Trace("authRevocationStore keyJTI")

	return s.prefix + "revoked:jti:" + jti
}

func (s *authRevocationStore) keyUserRevokedAfter(userID string) string {
	log.Trace("authRevocationStore keyUserRevokedAfter")

	return s.prefix + "user:revoked_after:" + userID
}

func (s *authRevocationStore) keySession(sid string) string {
	log.Trace("authRevocationStore keySession")

	return s.prefix + "session:" + sid
}
