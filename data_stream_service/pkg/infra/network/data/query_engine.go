package data

import (
	streamapp "data_stream_service/pkg/app"
	domainErrors "data_stream_service/pkg/domain"

	"github.com/apache/arrow-go/v18/arrow/flight"
	log "github.com/sirupsen/logrus"
)

type QueryResult = streamapp.QueryResult

type QueryEngine = streamapp.QueryEngineAdapter

func descriptorCommand(descriptor *flight.FlightDescriptor) string {
	log.Trace("descriptorCommand")

	if descriptor == nil {
		return ""
	}
	if len(descriptor.Cmd) > 0 {
		return string(descriptor.Cmd)
	}
	if len(descriptor.Path) > 0 {
		return descriptor.Path[len(descriptor.Path)-1]
	}
	return ""
}

func ticketCommand(ticket *flight.Ticket) string {
	log.Trace("ticketCommand")

	if ticket == nil {
		return ""
	}
	return string(ticket.GetTicket())
}

func validateDescriptor(descriptor *flight.FlightDescriptor) error {
	log.Trace("validateDescriptor")

	if descriptorCommand(descriptor) == "" {
		return domainErrors.ErrValidationFailed.Extend("flight descriptor requires command or path")
	}
	return nil
}
