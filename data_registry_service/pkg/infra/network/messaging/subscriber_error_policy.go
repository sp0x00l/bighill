package messaging

import (
	domainErrors "data_registry_service/pkg/domain"
	msgConn "lib/shared_lib/messaging"

	log "github.com/sirupsen/logrus"
)

type errorPolicy struct{}

func configureErrorPolicy(subscriber msgConn.Subscriber) {
	log.Trace("configureErrorPolicy")

	if err := msgConn.ConfigureErrorPolicy(subscriber, errorPolicy{}); err != nil {
		log.WithError(err).Trace("subscriber does not support error policy")
	}
}

func (errorPolicy) IsNonRetryableError(err error) bool {
	log.Trace("errorPolicy IsNonRetryableError")

	return msgConn.IsNonRetryable(err) ||
		msgConn.IsAlreadyProcessed(err) ||
		domainErrors.IsServiceError(err, domainErrors.ErrValidationFailed)
}
