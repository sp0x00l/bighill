package network

import (
	"net/http"
	"strings"

	log "github.com/sirupsen/logrus"
)

const (
	socketTokenQuery   = "token"
	socketTicketCookie = "bighill_socket_ticket"
)

func socketTicket(request *http.Request) string {
	log.Trace("socketTicket")

	token := strings.TrimSpace(request.URL.Query().Get(socketTokenQuery))
	if token == "" {
		if cookie, err := request.Cookie(socketTicketCookie); err == nil {
			token = strings.TrimSpace(cookie.Value)
		}
	}
	return token
}
