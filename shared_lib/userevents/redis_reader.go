package userevents

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/redis/rueidis"
	log "github.com/sirupsen/logrus"
)

const (
	InitialStreamCursor = "0-0"
	LatestStreamCursor  = "$"
)

type StreamEvent struct {
	Room     string
	StreamID string
	Event    Event
}

type RedisStreamReader struct {
	client rueidis.Client
	limit  int64
}

func NewRedisStreamReader(client rueidis.Client, limit int64) *RedisStreamReader {
	log.Trace("NewRedisStreamReader")

	return &RedisStreamReader{
		client: client,
		limit:  limit,
	}
}

func (r *RedisStreamReader) Read(ctx context.Context, room string, cursor string) ([]StreamEvent, error) {
	log.Trace("RedisStreamReader Read")

	cursor = strings.TrimSpace(cursor)
	if cursor == "" {
		cursor = InitialStreamCursor
	}
	count := r.limit
	if count <= 0 {
		count = 200
	}
	result := r.client.Do(ctx, r.client.B().
		Xread().
		Count(count).
		Streams().
		Key(room).
		Id(cursor).
		Build())
	if err := result.Error(); err != nil {
		if err == rueidis.Nil {
			return nil, nil
		}
		return nil, fmt.Errorf("read user event stream %s: %w", room, err)
	}
	return parseXReadResult(room, result)
}

func parseXReadResult(expectedRoom string, result rueidis.RedisResult) ([]StreamEvent, error) {
	log.Trace("parseXReadResult")

	streams, err := result.AsXRead()
	if err != nil {
		return nil, fmt.Errorf("parse user event stream response: %w", err)
	}
	events := []StreamEvent{}
	for room, messages := range streams {
		if room == "" {
			room = expectedRoom
		}
		for _, message := range messages {
			payload := message.FieldValues["payload"]
			if payload == "" {
				continue
			}
			var event Event
			if err := json.Unmarshal([]byte(payload), &event); err != nil {
				return nil, fmt.Errorf("decode user event payload %s: %w", message.ID, err)
			}
			events = append(events, StreamEvent{
				Room:     room,
				StreamID: message.ID,
				Event:    event,
			})
		}
	}
	return events, nil
}

func StreamIDTooOld(cursor string, firstID string) bool {
	log.Trace("StreamIDTooOld")

	cursor = strings.TrimSpace(cursor)
	firstID = strings.TrimSpace(firstID)
	if cursor == "" || cursor == InitialStreamCursor || firstID == "" {
		return false
	}
	return compareRedisStreamIDs(cursor, firstID) < 0
}

func compareRedisStreamIDs(left string, right string) int {
	log.Trace("compareRedisStreamIDs")

	leftMs, leftSeq := splitStreamID(left)
	rightMs, rightSeq := splitStreamID(right)
	if leftMs < rightMs {
		return -1
	}
	if leftMs > rightMs {
		return 1
	}
	if leftSeq < rightSeq {
		return -1
	}
	if leftSeq > rightSeq {
		return 1
	}
	return 0
}

func splitStreamID(value string) (int64, int64) {
	log.Trace("splitStreamID")

	parts := strings.SplitN(strings.TrimSpace(value), "-", 2)
	if len(parts) != 2 {
		return 0, 0
	}
	ms, _ := strconv.ParseInt(parts[0], 10, 64)
	seq, _ := strconv.ParseInt(parts[1], 10, 64)
	return ms, seq
}
