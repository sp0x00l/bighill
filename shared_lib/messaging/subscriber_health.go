package messaging

import (
	"context"
	"fmt"
	"strings"
	"time"

	"lib/shared_lib/healthcheck"
)

type SubscriberHealth struct {
	GroupID              string
	Topics               []string
	Started              bool
	Closed               bool
	AssignedPartitions   int
	PausedPartitions     int
	QueueDepth           int
	QueueCapacity        int
	BacklogDepth         int
	PollAttempts         uint64
	MessagesCommitted    uint64
	MessagesDLQ          uint64
	RetryCount           uint64
	BackpressurePauses   uint64
	MaxReplayDLQ         uint64
	LastPollAt           time.Time
	LastMessageAt        time.Time
	LastCommitAt         time.Time
	LastProgressAt       time.Time
	LastErrorAt          time.Time
	LastError            string
	LastTransientErrorAt time.Time
	LastTransientError   string
	MaxLag               int64
	LagByTopic           map[string]int64
	NonRetryableFailures uint64
}

type SubscriberHealthReporter interface {
	Health() SubscriberHealth
}

type SubscriberHealthCheckConfig struct {
	RequireAssignment  bool
	MaxPollSilence     time.Duration
	MaxProgressSilence time.Duration
	MaxLag             int64
}

func CheckSubscriberHealth(ctx context.Context, sub Subscriber, cfg SubscriberHealthCheckConfig) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	reporter, ok := sub.(SubscriberHealthReporter)
	if !ok {
		return fmt.Errorf("subscriber does not expose health")
	}
	health := reporter.Health()
	if err := ctx.Err(); err != nil {
		return err
	}
	return health.Check(ctx, cfg)
}

func (h SubscriberHealth) Check(ctx context.Context, cfg SubscriberHealthCheckConfig) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	now := time.Now()
	label := h.GroupID
	if label == "" {
		label = strings.Join(h.Topics, ",")
	}

	if !h.Started {
		return fmt.Errorf("subscriber %s has not started", label)
	}
	if h.Closed {
		return fmt.Errorf("subscriber %s is closed", label)
	}
	if cfg.RequireAssignment && h.AssignedPartitions == 0 {
		return fmt.Errorf("subscriber %s has no active message broker assignment for topics %v", label, h.Topics)
	}
	if h.LastPollAt.IsZero() {
		return fmt.Errorf("subscriber %s has not polled message broker yet", label)
	}
	if silence := now.Sub(h.LastPollAt); cfg.MaxPollSilence > 0 && silence > cfg.MaxPollSilence {
		return fmt.Errorf("subscriber %s has not polled message broker for %s", label, silence.Truncate(time.Second))
	}

	if cfg.MaxLag > 0 && h.MaxLag > cfg.MaxLag {
		return fmt.Errorf("subscriber %s lag %d exceeds threshold %d", label, h.MaxLag, cfg.MaxLag)
	}
	if h.MaxLag > 0 && cfg.MaxProgressSilence > 0 {
		progressAt := h.LastProgressAt
		if progressAt.IsZero() {
			progressAt = h.LastPollAt
		}
		if silence := now.Sub(progressAt); silence > cfg.MaxProgressSilence {
			return fmt.Errorf("subscriber %s has lag %d and no offset progress for %s", label, h.MaxLag, silence.Truncate(time.Second))
		}
	}

	return nil
}

type SubscriberHealthCheck struct {
	subscriber Subscriber
}

type SubscriberHealthRegistration struct {
	Name       string
	Subscriber Subscriber
}

func NewSubscriberHealthCheck(subscriber Subscriber) *SubscriberHealthCheck {
	return &SubscriberHealthCheck{subscriber: subscriber}
}

func (c *SubscriberHealthCheck) Check(ctx context.Context, config healthcheck.HealthCheckConfig) error {
	return CheckSubscriberHealth(ctx, c.subscriber, SubscriberHealthCheckConfig{
		RequireAssignment:  true,
		MaxPollSilence:     config.MessageBrokerSubscriberMaxPollSilenceSec,
		MaxProgressSilence: config.MessageBrokerSubscriberMaxProgressSilenceSec,
		MaxLag:             config.MessageBrokerSubscriberMaxLag,
	})
}

func RegisterSubscriberHealthChecks(monitor *healthcheck.Monitor, registrations ...SubscriberHealthRegistration) *healthcheck.Monitor {
	for _, registration := range registrations {
		if registration.Subscriber == nil {
			continue
		}
		name := strings.TrimSpace(registration.Name)
		if name == "" {
			continue
		}
		if !strings.HasPrefix(name, "message_broker_subscriber_") {
			name = "message_broker_subscriber_" + name
		}
		monitor = monitor.Register(name, NewSubscriberHealthCheck(registration.Subscriber).Check)
	}
	return monitor
}
