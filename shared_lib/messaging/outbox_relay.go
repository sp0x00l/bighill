package messaging

import (
	"context"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
	metrics "lib/shared_lib/metrics"
)

type OutboxRelayConfig struct {
	PollInterval   time.Duration
	FailureBackoff time.Duration
	BatchSize      int32
	Signal         <-chan struct{}
	InstanceID     string
	LeaseDuration  time.Duration
}

type OutboxRelay struct {
	outbox    RelayOutbox
	publisher RelayPublisher
	cfg       OutboxRelayConfig
	retryCh   chan struct{}
}

func NewOutboxRelay(outbox RelayOutbox, publisher RelayPublisher, cfg OutboxRelayConfig) *OutboxRelay {
	log.Trace("NewOutboxRelay")

	if configurable, ok := outbox.(interface {
		ConfigureRelayIdentity(ownerID string, leaseDuration time.Duration)
	}); ok {
		configurable.ConfigureRelayIdentity(cfg.InstanceID, cfg.LeaseDuration)
	}

	return &OutboxRelay{
		outbox:    outbox,
		publisher: publisher,
		cfg:       cfg,
		retryCh:   make(chan struct{}, 1),
	}
}

func (r *OutboxRelay) Run(ctx context.Context) error {
	log.Trace("OutboxRelay Run")

	// If a signal channel is configured, run in signal-driven mode to avoid
	// continuous polling in idle periods.
	if r.cfg.Signal != nil {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-r.cfg.Signal:
				if _, err := r.processBatch(ctx); err != nil {
					log.WithContext(ctx).WithError(err).Warn("outbox relay batch failed")
				}
			case <-r.retryCh:
				if _, err := r.processBatch(ctx); err != nil {
					log.WithContext(ctx).WithError(err).Warn("outbox relay retry batch failed")
				}
			}
		}
	}

	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if _, err := r.processBatch(ctx); err != nil {
				log.WithContext(ctx).WithError(err).Warn("outbox relay batch failed")
			}
		}
	}
}

func (r *OutboxRelay) processBatch(ctx context.Context) (bool, error) {
	log.Trace("OutboxRelay processBatch")

	pending, err := r.outbox.ReadPending(ctx, r.cfg.BatchSize)
	if err != nil {
		metrics.Default().RecordError(ctx, metrics.BoundaryDB, "outbox_relay_read", metrics.ClassifyDB(err), "")
		return false, fmt.Errorf("read pending outbox messages: %w", err)
	}

	hadFailure := false
	for _, item := range pending {
		if err := r.publisher.PublishOutboxMessage(ctx, item.Topic, item.Payload, item.Headers); err != nil {
			metrics.Default().RecordError(ctx, metrics.BoundaryKafka, "outbox_relay_publish", metrics.ClassifyKafka(err), "")
			nextAttemptAt := time.Now().UTC().Add(r.cfg.FailureBackoff)
			hadFailure = true
			if markErr := r.outbox.MarkFailed(ctx, item, err.Error(), nextAttemptAt); markErr != nil {
				log.WithContext(ctx).WithError(markErr).Error("failed to mark outbox message as failed")
			}
			continue
		}

		if err := r.outbox.MarkSent(ctx, item); err != nil {
			metrics.Default().RecordError(ctx, metrics.BoundaryDB, "outbox_relay_mark_sent", metrics.ClassifyDB(err), "")
			log.WithContext(ctx).WithError(err).Error("failed to mark outbox message as sent")
		}
	}

	if hadFailure && r.cfg.Signal != nil {
		time.AfterFunc(r.cfg.FailureBackoff, func() {
			select {
			case r.retryCh <- struct{}{}:
			default:
			}
		})
	}

	return hadFailure, nil
}
