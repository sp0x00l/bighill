package domain

import "lib/shared_lib/userevents"

type StreamEvent struct {
	Room     Room
	StreamID string
	Event    userevents.Event
}
