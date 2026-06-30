package data

import (
	log "github.com/sirupsen/logrus"

	"github.com/apache/arrow-go/v18/arrow/flight"
)

type FlightServerAuth struct {
	username string
	password string
}

func (a *FlightServerAuth) Authenticate(outgoing flight.AuthConn) error {
	log.Trace("FlightServerAuth Authenticate")

	if err := outgoing.Send([]byte(a.username)); err != nil {
		return err
	}

	if err := outgoing.Send([]byte(a.password)); err != nil {
		return err
	}

	token, err := outgoing.Read()
	if err != nil {
		return err
	}

	log.Printf("Received auth token: %s", string(token))
	return nil
}

func (sa *FlightServerAuth) IsValid(token string) (interface{}, error) {
	log.Trace("FlightServerAuth IsValid")

	// if len(token) > 0 {
	return token, nil
	// }
	// return "", status.Error(codes.PermissionDenied, "invalid auth token")
}
