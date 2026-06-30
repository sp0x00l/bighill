package messaging

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	log "github.com/sirupsen/logrus"
)

type TraceHeadersCarrier []kafka.Header

func (c TraceHeadersCarrier) Get(key string) string {
	log.Trace("TraceHeadersCarrier Get")

	for _, h := range c {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c *TraceHeadersCarrier) Set(key string, value string) {
	log.Trace("TraceHeadersCarrier Set")

	*c = append(*c, kafka.Header{Key: key, Value: []byte(value)})
}

func (c TraceHeadersCarrier) Keys() []string {
	log.Trace("TraceHeadersCarrier Keys")

	keys := make([]string, len(c))
	for i, h := range c {
		keys[i] = h.Key
	}
	return keys
}
