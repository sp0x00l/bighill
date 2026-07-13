package userevents

import (
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"
)

const (
	roomKindUser = "user"
	roomKindOrg  = "org"
)

type Room struct {
	Key string
}

func UserRoom(prefix string, userID string) Room {
	log.Trace("UserRoom")

	prefix = normalizeRoomPrefix(prefix)
	return Room{Key: roomKey(prefix, roomKindUser, strings.TrimSpace(userID), "events")}
}

func OrgRoom(prefix string, orgID string) Room {
	log.Trace("OrgRoom")

	prefix = normalizeRoomPrefix(prefix)
	return Room{Key: roomKey(prefix, roomKindOrg, strings.TrimSpace(orgID), "events")}
}

func EventRooms(prefix string, event Event) []Room {
	log.Trace("EventRooms")

	prefix = normalizeRoomPrefix(prefix)
	rooms := make([]Room, 0, 2)
	if strings.TrimSpace(event.UserID) != "" {
		rooms = append(rooms, UserRoom(prefix, event.UserID))
	}
	if strings.TrimSpace(event.OrgID) != "" && strings.TrimSpace(event.RequiredPermission) != "" {
		rooms = append(rooms, OrgRoom(prefix, event.OrgID))
	}
	return dedupeRooms(rooms)
}

func normalizeRoomPrefix(prefix string) string {
	log.Trace("normalizeRoomPrefix")

	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return DefaultChannelPrefix
	}
	return prefix
}

func roomKey(parts ...string) string {
	log.Trace("roomKey")

	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.Trim(strings.TrimSpace(part), ":")
		if part != "" {
			out = append(out, part)
		}
	}
	return strings.Join(out, ":")
}

func dedupeRooms(rooms []Room) []Room {
	log.Trace("dedupeRooms")

	seen := map[string]struct{}{}
	out := make([]Room, 0, len(rooms))
	for _, room := range rooms {
		if strings.TrimSpace(room.Key) == "" {
			continue
		}
		if _, ok := seen[room.Key]; ok {
			continue
		}
		seen[room.Key] = struct{}{}
		out = append(out, room)
	}
	return out
}

func (r Room) String() string {
	log.Trace("Room String")

	return fmt.Sprintf("%s", r.Key)
}
