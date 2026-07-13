package app

import (
	"socket_service/pkg/domain"

	log "github.com/sirupsen/logrus"
	"lib/shared_lib/userevents"
)

type RoomResolver struct {
	prefix string
}

func NewRoomResolver(prefix string) *RoomResolver {
	log.Trace("NewRoomResolver")

	return &RoomResolver{prefix: prefix}
}

func (r *RoomResolver) Resolve(session domain.Session) []domain.Room {
	log.Trace("RoomResolver Resolve")

	rooms := []domain.Room{}
	if session.UserID != "" {
		room := userevents.UserRoom(r.prefix, session.UserID)
		rooms = append(rooms, domain.Room{Key: room.Key, Type: domain.RoomTypeUser})
	}
	if session.OrgID != "" {
		room := userevents.OrgRoom(r.prefix, session.OrgID)
		rooms = append(rooms, domain.Room{Key: room.Key, Type: domain.RoomTypeOrg})
	}
	return rooms
}
