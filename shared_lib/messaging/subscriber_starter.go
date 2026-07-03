package messaging

import (
	"context"
	"errors"
	"strings"

	"lib/shared_lib/healthcheck"

	log "github.com/sirupsen/logrus"
)

type StreamSubscriberConfig struct {
	Brokers          string
	DLQURL           string
	BaseGroupID      string
	AutoOffsetReset  string
	Cancel           context.CancelFunc
	Monitor          *healthcheck.Monitor
	OnUnexpectedStop func()
}

func StreamGroupID(baseGroupID string, stream string) string {
	baseGroupID = strings.TrimSpace(baseGroupID)
	stream = strings.TrimSpace(stream)
	if stream == "" {
		return baseGroupID
	}
	if baseGroupID == "" {
		return stream
	}
	return baseGroupID + "-" + stream
}

func StartStreamSubscriber(
	ctx context.Context,
	cfg StreamSubscriberConfig,
	name string,
	topics []string,
	configure func(Subscriber),
) (Messenger, *healthcheck.Monitor, error) {
	log.Trace("StartStreamSubscriber")

	groupID := StreamGroupID(cfg.BaseGroupID, name)
	factory := NewMessenger(MessengerConfig{
		Brokers:         cfg.Brokers,
		GroupID:         groupID,
		DlqURL:          cfg.DLQURL,
		AutoOffsetReset: cfg.AutoOffsetReset,
	}, cfg.Cancel)
	subscriber, err := factory.Subscriber(ctx)
	if err != nil {
		return nil, cfg.Monitor, err
	}
	if configure != nil {
		configure(subscriber)
	}
	monitor := cfg.Monitor
	if monitor != nil {
		monitor = RegisterSubscriberHealthChecks(
			monitor,
			SubscriberHealthRegistration{Name: name, Subscriber: subscriber},
		)
	}
	go func() {
		if err := subscriber.Subscribe(ctx, topics); err != nil && !errors.Is(err, context.Canceled) {
			log.WithContext(ctx).WithError(err).WithFields(log.Fields{
				"subscriber": name,
				"group_id":   groupID,
				"topics":     topics,
			}).Error("subscriber stopped unexpectedly")
			if cfg.OnUnexpectedStop != nil {
				cfg.OnUnexpectedStop()
			}
		}
	}()
	return factory, monitor, nil
}
