package userevents

import (
	"crypto/tls"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	DefaultChannelPrefix  = "mlops"
	DefaultPublishTimeout = 500 * time.Millisecond
	DefaultStreamMaxLen   = 1000
)

type Config struct {
	Enabled        bool
	RedisAddress   string
	RedisUsername  string
	RedisPassword  string
	RedisTLS       bool
	ChannelPrefix  string
	PublishTimeout time.Duration
	StreamMaxLen   int64
}

func (c Config) Normalized() Config {
	log.Trace("Config Normalized")

	out := c
	out.ChannelPrefix = strings.TrimSpace(out.ChannelPrefix)
	if out.ChannelPrefix == "" {
		out.ChannelPrefix = DefaultChannelPrefix
	}
	if out.PublishTimeout <= 0 {
		out.PublishTimeout = DefaultPublishTimeout
	}
	if out.StreamMaxLen <= 0 {
		out.StreamMaxLen = DefaultStreamMaxLen
	}
	return out
}

func (c Config) TLSConfig() *tls.Config {
	log.Trace("Config TLSConfig")

	if !c.RedisTLS {
		return nil
	}
	return &tls.Config{MinVersion: tls.VersionTLS12}
}
