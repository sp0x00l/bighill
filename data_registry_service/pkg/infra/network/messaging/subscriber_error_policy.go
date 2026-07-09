package messaging

import (
	"errors"

	domainErrors "data_registry_service/pkg/domain"
	msgConn "lib/shared_lib/messaging"

	log "github.com/sirupsen/logrus"
)

type errorPolicy struct{}

func configureErrorPolicy(subscriber msgConn.Subscriber) {
	log.Trace("configureErrorPolicy")

	if err := msgConn.ConfigureErrorPolicy(subscriber, errorPolicy{}); err != nil {
		log.Trace("configureErrorPolicy unsupported subscriber")
	}
}

func ConfigureSubscriberErrorPolicy(subscriber msgConn.Subscriber) {
	log.Trace("ConfigureSubscriberErrorPolicy")

	configureErrorPolicy(subscriber)
}

func (errorPolicy) IsNonRetryableError(err error) bool {
	log.Trace("errorPolicy IsNonRetryableError")

	return msgConn.IsNonRetryable(err) ||
		msgConn.IsAlreadyProcessed(err) ||
		domainErrors.IsServiceError(err, domainErrors.ErrValidationFailed) ||
		errors.Is(err, domainErrors.ErrMaterializationEventSequenceInvalid)
}
