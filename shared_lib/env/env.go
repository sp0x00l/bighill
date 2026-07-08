package config

import (
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	env          = "ENVIRONMENT"
	devbuild     = "LOCAL-DEV"
	cicdBuild    = "CICD"
	stagingBuild = "STAGING"
	prodBuild    = "PROD"
)

var (
	envValue atomic.Pointer[string]
)

func WithDefaultString(key, defaultValue string) string {
	log.Trace("Env WithDefaultString")

	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

func MustString(key string) string {
	log.Trace("Env MustString")
	value, exists := os.LookupEnv(key)
	if !exists {
		log.Fatalf("environment variable %s is required", key)
	}
	return value
}

func WithDefaultInt(key string, defaultValue string) int {
	log.Trace("Env WithDefaultInt")

	if value, exists := os.LookupEnv(key); exists {
		result, err := strconv.Atoi(value)
		if err != nil {
			log.Fatalf("could not load environment variable %s=%q, expected integer: %v", key, value, err)
		}
		return result
	}
	defaultIntValue, err := strconv.Atoi(defaultValue)
	if err != nil {
		log.Fatalf("could not convert default value for environment variable %s=%q, expected integer: %v", key, defaultValue, err)
	}
	return defaultIntValue
}

func MustInt(key string) int {
	log.Trace("Env MustInt")
	value := MustString(key)
	result, err := strconv.Atoi(value)
	if err != nil {
		log.Fatalf("could not load environment variable %s=%q, expected integer: %v", key, value, err)
	}
	return result
}

func WithDefaultInt64(key string, defaultValue string) int64 {
	log.Trace("Env WithDefaultInt64")

	if value, exists := os.LookupEnv(key); exists {
		i64, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			log.Fatalf("could not load environment variable %s=%q, expected int64: %v", key, value, err)
		}
		return i64
	}
	defaultInt64Value, err := strconv.ParseInt(defaultValue, 10, 64)
	if err != nil {
		log.Fatalf("could not convert default value for environment variable %s=%q, expected int64: %v", key, defaultValue, err)
	}
	return defaultInt64Value
}

func MustInt64(key string) int64 {
	log.Trace("Env MustInt64")
	value := MustString(key)
	result, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		log.Fatalf("could not load environment variable %s=%q, expected int64: %v", key, value, err)
	}
	return result
}

func MustDuration(key string) time.Duration {
	log.Trace("Env MustDuration")
	value := MustString(key)
	result, err := time.ParseDuration(value)
	if err != nil {
		log.Fatalf("could not load environment variable %s=%q, expected duration: %v", key, value, err)
	}
	return result
}

func WithDefaultBool(key string, defaultValue bool) bool {
	log.Trace("Env WithDefaultBool")

	if value, exists := os.LookupEnv(key); exists {
		b, err := strconv.ParseBool(value)
		if err != nil {
			log.Fatalf("could not load environment variable %s=%q, expected bool: %v", key, value, err)
		}
		return b
	}
	return defaultValue
}

func MustBool(key string) bool {
	log.Trace("Env MustBool")
	value := MustString(key)
	result, err := strconv.ParseBool(value)
	if err != nil {
		log.Fatalf("could not load environment variable %s=%q, expected bool: %v", key, value, err)
	}
	return result
}

func WithDefaultStringSlice(key string, defaultValue string) []string {
	log.Trace("Env WithDefaultStringSlice")

	if value, exists := os.LookupEnv(key); exists {
		return parseStringSlice(key, value)
	}
	return parseStringSlice(key, defaultValue)
}

func MustStringSlice(key string) []string {
	log.Trace("Env MustStringSlice")
	return parseStringSlice(key, MustString(key))
}

func parseStringSlice(key string, value string) []string {
	if value == "" {
		log.Fatalf("could not load environment variable %s, expected non-empty comma-separated string list", key)
	}

	result := strings.Split(value, ",")
	for _, item := range result {
		if item == "" {
			log.Fatalf("could not load environment variable %s=%q, expected non-empty comma-separated string list items", key, value)
		}
	}
	return result
}

func WithDefaultMap(key string, defaultValue string) map[string]int64 {
	log.Trace("Env WithDefaultMap")

	result := make(map[string]int64)
	if value, exists := os.LookupEnv(key); exists {
		pairs := strings.SplitSeq(value, ",")
		for pair := range pairs {
			kv := strings.Split(pair, ":")
			if len(kv) != 2 {
				log.Fatalf("could not parse environment variable map for key %s, got: %s", key, pair)
			}
			intValue, err := strconv.ParseInt(kv[1], 10, 64)
			if err != nil {
				log.Fatalf("could not convert value to int64 for key %s, got: %s", key, kv[1])
			}
			result[kv[0]] = intValue
		}
		return result
	}

	pairs := strings.SplitSeq(defaultValue, ",")
	for pair := range pairs {
		kv := strings.Split(pair, ":")
		if len(kv) != 2 {
			log.Fatalf("could not parse default environment variable map for key %s, got: %s", key, pair)
		}
		intValue, err := strconv.ParseInt(kv[1], 10, 64)
		if err != nil {
			log.Fatalf("could not convert default value to int64 for key %s, got: %s", key, kv[1])
		}
		result[kv[0]] = intValue
	}
	return result
}

func IsLocalDev() bool {
	return environment() == devbuild
}

func IsDevEnv() bool {
	buildType := environment()
	return buildType == devbuild || buildType == cicdBuild
}

func IsStaging() bool {
	return environment() == stagingBuild
}

func IsProduction() bool {
	return environment() == prodBuild
}

func RequireKnownEnvironment() {
	log.Trace("Env RequireKnownEnvironment")

	if !IsKnownEnvironment() {
		log.Fatalf("environment variable %s must be one of %s, %s, %s, %s", env, devbuild, cicdBuild, stagingBuild, prodBuild)
	}
}

func RequireServiceEnvironment() {
	log.Trace("Env RequireServiceEnvironment")

	RequireKnownEnvironment()
}

func IsKnownEnvironment() bool {
	log.Trace("Env IsKnownEnvironment")

	switch environment() {
	case devbuild, cicdBuild, stagingBuild, prodBuild:
		return true
	default:
		return false
	}
}

// ResetEnvironmentCache clears the cached environment value (primarily for tests).
func ResetEnvironmentCache() {
	envValue.Store(nil)
}

func environment() string {
	for {
		if value := envValue.Load(); value != nil {
			return *value
		}
		next := strings.ToUpper(strings.TrimSpace(WithDefaultString(env, "")))
		stored := &next
		if envValue.CompareAndSwap(nil, stored) {
			return next
		}
	}
}
