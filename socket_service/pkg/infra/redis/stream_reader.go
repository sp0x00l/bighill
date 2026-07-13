package redis

import (
	"context"
	"errors"
	"sort"

	"socket_service/pkg/domain"

	"github.com/redis/rueidis"
	log "github.com/sirupsen/logrus"
	"lib/shared_lib/userevents"
)

const (
	payloadField       = "payload"
	defaultStreamStart = "0-0"
)

type StreamReader struct {
	client                rueidis.Client
	liveBlockMilliseconds int64
}

func NewStreamReader(client rueidis.Client, liveBlockMilliseconds int64) *StreamReader {
	log.Trace("NewStreamReader")

	return &StreamReader{
		client:                client,
		liveBlockMilliseconds: liveBlockMilliseconds,
	}
}

func (r *StreamReader) Replay(ctx context.Context, subscription domain.Subscription, cursors map[string]string, limit int) ([]domain.StreamEvent, error) {
	log.Trace("StreamReader Replay")

	return r.read(ctx, subscription.Rooms, cursors, limit, 0)
}

func (r *StreamReader) ReadLive(ctx context.Context, subscription domain.Subscription, cursors map[string]string, limit int) ([]domain.StreamEvent, error) {
	log.Trace("StreamReader ReadLive")

	return r.read(ctx, subscription.Rooms, cursors, limit, r.liveBlockMilliseconds)
}

func (r *StreamReader) read(ctx context.Context, rooms []domain.Room, cursors map[string]string, limit int, blockMilliseconds int64) ([]domain.StreamEvent, error) {
	log.Trace("StreamReader read")

	if len(rooms) == 0 {
		return nil, nil
	}
	if limit <= 0 {
		return nil, domain.ErrValidationFailed.Extend("stream read limit must be positive")
	}

	keys := make([]string, 0, len(rooms))
	ids := make([]string, 0, len(rooms))
	roomByKey := map[string]domain.Room{}
	for _, room := range rooms {
		keys = append(keys, room.Key)
		roomByKey[room.Key] = room
		cursor := cursors[room.Key]
		if cursor == "" {
			cursor = defaultStreamStart
		}
		ids = append(ids, cursor)
	}

	var cmd rueidis.Completed
	if blockMilliseconds > 0 {
		cmd = r.client.B().Xread().Count(int64(limit)).Block(blockMilliseconds).Streams().Key(keys...).Id(ids...).Build()
	} else {
		cmd = r.client.B().Xread().Count(int64(limit)).Streams().Key(keys...).Id(ids...).Build()
	}
	result, err := r.client.Do(ctx, cmd).AsXRead()
	if err != nil {
		if errors.Is(err, rueidis.Nil) {
			return nil, nil
		}
		log.WithContext(ctx).WithError(err).Warn("redis user event stream read failed")
		return nil, domain.ErrDependencyFailed.Extend("redis stream read failed")
	}

	events := make([]domain.StreamEvent, 0)
	for key, entries := range result {
		room := roomByKey[key]
		for _, entry := range entries {
			payload := entry.FieldValues[payloadField]
			if payload == "" {
				continue
			}
			event, err := userevents.ParseEventPayload([]byte(payload))
			if err != nil {
				log.WithContext(ctx).WithError(err).WithField("stream", key).Warn("discarding invalid user event payload")
				continue
			}
			events = append(events, domain.StreamEvent{
				Room:     room,
				StreamID: entry.ID,
				Event:    event,
			})
		}
	}
	sort.SliceStable(events, func(i, j int) bool {
		return events[i].Event.OccurredAt.Before(events[j].Event.OccurredAt)
	})
	return events, nil
}
