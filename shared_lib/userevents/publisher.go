package userevents

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/redis/rueidis"
	log "github.com/sirupsen/logrus"
)

type NoopPublisher struct{}

func NewNoopPublisher() *NoopPublisher {
	log.Trace("NewNoopPublisher")

	return &NoopPublisher{}
}

func (p *NoopPublisher) Publish(context.Context, Event) error {
	log.Trace("NoopPublisher Publish")

	return nil
}

func (p *NoopPublisher) Close() {
	log.Trace("NoopPublisher Close")
}

type RecordingPublisher struct {
	mu     sync.Mutex
	events []Event
	err    error
}

func NewRecordingPublisher() *RecordingPublisher {
	log.Trace("NewRecordingPublisher")

	return &RecordingPublisher{}
}

func (p *RecordingPublisher) Publish(ctx context.Context, event Event) error {
	log.Trace("RecordingPublisher Publish")

	event = EnsureEventDefaults(ctx, SanitizeEvent(event))
	if err := event.Validate(); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return p.err
	}
	p.events = append(p.events, event)
	return nil
}

func (p *RecordingPublisher) Close() {
	log.Trace("RecordingPublisher Close")
}

func (p *RecordingPublisher) Events() []Event {
	log.Trace("RecordingPublisher Events")

	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]Event, len(p.events))
	copy(out, p.events)
	return out
}

func (p *RecordingPublisher) SetError(err error) {
	log.Trace("RecordingPublisher SetError")

	p.mu.Lock()
	defer p.mu.Unlock()
	p.err = err
}

type RedisPublisher struct {
	client     rueidis.Client
	config     Config
	ownsClient bool
}

func NewRedisPublisher(cfg Config) (*RedisPublisher, error) {
	log.Trace("NewRedisPublisher")

	cfg = cfg.Normalized()
	if cfg.RedisAddress == "" {
		return nil, errors.New("user events redis address is required")
	}
	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{cfg.RedisAddress},
		Username:    cfg.RedisUsername,
		Password:    cfg.RedisPassword,
		TLSConfig:   cfg.TLSConfig(),
	})
	if err != nil {
		return nil, fmt.Errorf("create user events redis client: %w", err)
	}
	return NewRedisPublisherWithClient(client, cfg, true), nil
}

func NewRedisPublisherWithClient(client rueidis.Client, cfg Config, ownsClient bool) *RedisPublisher {
	log.Trace("NewRedisPublisherWithClient")

	return &RedisPublisher{
		client:     client,
		config:     cfg.Normalized(),
		ownsClient: ownsClient,
	}
}

func (p *RedisPublisher) Publish(ctx context.Context, event Event) error {
	log.Trace("RedisPublisher Publish")

	if p == nil || p.client == nil {
		return errors.New("user event redis publisher is not initialized")
	}
	event = EnsureEventDefaults(ctx, SanitizeEvent(event))
	payload, err := event.MarshalJSONPayload()
	if err != nil {
		return err
	}
	rooms := EventRooms(p.config.ChannelPrefix, event)
	if len(rooms) == 0 {
		return fmt.Errorf("%w: no event rooms resolved", ErrInvalidEvent)
	}
	publishCtx, cancel := context.WithTimeout(ctx, p.config.PublishTimeout)
	defer cancel()
	for _, room := range rooms {
		if err := p.xadd(publishCtx, room.Key, payload); err != nil {
			return err
		}
		if err := p.publish(publishCtx, room.Key, payload); err != nil {
			return err
		}
		log.WithContext(ctx).WithField("room", room.Key).WithField("event_id", event.EventID).Trace("published user event")
	}
	return nil
}

func (p *RedisPublisher) Close() {
	log.Trace("RedisPublisher Close")

	if p != nil && p.ownsClient && p.client != nil {
		p.client.Close()
	}
}

func (p *RedisPublisher) xadd(ctx context.Context, key string, payload []byte) error {
	log.Trace("RedisPublisher xadd")

	cmd := p.client.B().
		Xadd().
		Key(key).
		Maxlen().
		Almost().
		Threshold(fmt.Sprintf("%d", p.config.StreamMaxLen)).
		Id("*").
		FieldValue().
		FieldValue("payload", string(payload)).
		Build()
	if err := p.client.Do(ctx, cmd).Error(); err != nil {
		return fmt.Errorf("xadd user event %s: %w", key, err)
	}
	return nil
}

func (p *RedisPublisher) publish(ctx context.Context, key string, payload []byte) error {
	log.Trace("RedisPublisher publish")

	cmd := p.client.B().Publish().Channel(key).Message(string(payload)).Build()
	if err := p.client.Do(ctx, cmd).Error(); err != nil {
		return fmt.Errorf("publish user event %s: %w", key, err)
	}
	return nil
}
