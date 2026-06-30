//go:build cgo

package metrics

import (
	"errors"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func ClassifyKafka(err error) ErrorClass {
	if err == nil {
		return ErrorClassUnknown
	}
	var kErr kafka.Error
	if errors.As(err, &kErr) {
		if kErr.IsTimeout() || kErr.Code() == kafka.ErrTimedOut {
			return ErrorClassTimeout
		}
		if kErr.Code() == kafka.ErrAllBrokersDown {
			return ErrorClassUnavailable
		}
		if kErr.Code() == kafka.ErrTransport {
			return ErrorClassNetwork
		}
		return ErrorClassInternal
	}
	return ErrorClassUnknown
}
