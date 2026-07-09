package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"tenant_service/pkg/domain"

	"github.com/redis/rueidis"
)

type OAuthStateStore struct {
	client rueidis.Client
	prefix string
}

func NewOAuthStateStore(client rueidis.Client, prefix string) *OAuthStateStore {
	if prefix == "" {
		prefix = "auth:oauth:"
	}
	return &OAuthStateStore{
		client: client,
		prefix: prefix,
	}
}

func (s *OAuthStateStore) Save(ctx context.Context, state domain.OAuthState, ttl time.Duration) error {
	payload, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to marshal oauth state: %w", err)
	}
	cmd := s.client.B().Set().Key(s.key(state.State)).Value(string(payload)).Ex(ttl).Build()
	return s.client.Do(ctx, cmd).Error()
}

func (s *OAuthStateStore) Load(ctx context.Context, state string) (*domain.OAuthState, error) {
	cmd := s.client.B().Get().Key(s.key(state)).Build()
	res := s.client.Do(ctx, cmd)
	if res.Error() != nil {
		if errors.Is(res.Error(), rueidis.Nil) {
			return nil, nil
		}
		return nil, res.Error()
	}
	payload, err := res.ToString()
	if err != nil {
		return nil, err
	}
	var record domain.OAuthState
	if err := json.Unmarshal([]byte(payload), &record); err != nil {
		return nil, fmt.Errorf("failed to unmarshal oauth state: %w", err)
	}
	return &record, nil
}

func (s *OAuthStateStore) Delete(ctx context.Context, state string) error {
	cmd := s.client.B().Del().Key(s.key(state)).Build()
	return s.client.Do(ctx, cmd).Error()
}

func (s *OAuthStateStore) key(state string) string {
	return s.prefix + "state:" + state
}
