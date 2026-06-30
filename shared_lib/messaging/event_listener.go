package messaging

import (
	"context"
	"fmt"
	"runtime/debug"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

type contextKey string

type EventListener[T proto.Message] interface {
	MsgType() MsgType
	NewMessage() T // Factory for T
	Handle(ctx context.Context, resourceKey uuid.UUID, payload T) error
}

// AddListener registers a typed listener with the subscriber. The wrapper
// enforces two invariants on behalf of the service:
//
//  1. A payload that fails to deserialize is a deterministic wire-format
//     failure. The error is wrapped as NonRetryable so the subscriber routes
//     the message to the DLQ instead of replaying it.
//  2. A panic inside Handle (or its transitive dependencies) is recovered and
//     converted to a NonRetryable error. This prevents one malformed message
//     from killing the shard worker — the offending message is sent to the
//     DLQ and the worker keeps draining its shard.
func AddListener[T proto.Message](s Subscriber, handler EventListener[T]) {
	msgType := handler.MsgType()

	s.RegisterListener(msgType, func(ctx context.Context, msg Message) (err error) {
		defer func() {
			if r := recover(); r != nil {
				stack := debug.Stack()
				log.WithContext(ctx).
					WithField("msg_type", msgType.String()).
					WithField("resource_key", msg.ResourceKey).
					WithField("panic", r).
					WithField("stack", string(stack)).
					Error("listener panic recovered")
				err = NonRetryable(fmt.Errorf("listener panic for msg_type=%s: %v", msgType.String(), r))
			}
		}()

		payload := handler.NewMessage() // new instance of T
		if derr := msg.DeserializePayload(payload); derr != nil {
			return NonRetryable(fmt.Errorf("deserialize %s: %w", msgType.String(), derr))
		}
		newCtx := context.WithValue(ctx, contextKey("resource_key"), msg.ResourceKey)
		newCtx = context.WithValue(newCtx, contextKey("msg_type"), msgType.String())

		return handler.Handle(newCtx, msg.ResourceKey, payload)
	})
	log.Info("Registered kafka event listener for message type:", msgType)
}
