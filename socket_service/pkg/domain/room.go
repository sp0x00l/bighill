package domain

import log "github.com/sirupsen/logrus"

type RoomType string

const (
	RoomTypeUser     RoomType = "user"
	RoomTypeOrg      RoomType = "org"
	RoomTypeResource RoomType = "resource"
)

func (t RoomType) IsKnown() bool {
	log.Trace("RoomType IsKnown")

	switch t {
	case RoomTypeUser,
		RoomTypeOrg,
		RoomTypeResource:
		return true
	default:
		return false
	}
}

type Room struct {
	Key  string
	Type RoomType
}
