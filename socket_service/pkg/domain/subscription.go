package domain

import log "github.com/sirupsen/logrus"

type Subscription struct {
	ConnectionID string
	Session      Session
	Rooms        []Room
}

func (s Subscription) RoomKeys() []string {
	log.Trace("Subscription RoomKeys")

	keys := make([]string, 0, len(s.Rooms))
	for _, room := range s.Rooms {
		keys = append(keys, room.Key)
	}
	return keys
}
