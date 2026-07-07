package provider

import (
	"encoding/json"
	"math"
)

func ClaimUnixSeconds(claims map[string]any, name string) (int64, bool) {
	switch value := claims[name].(type) {
	case int64:
		return value, value > 0
	case int:
		return int64(value), value > 0
	case int32:
		return int64(value), value > 0
	case float64:
		if value <= 0 || math.Trunc(value) != value || value > float64(math.MaxInt64) {
			return 0, false
		}
		return int64(value), true
	case json.Number:
		parsed, err := value.Int64()
		return parsed, err == nil && parsed > 0
	default:
		return 0, false
	}
}

func ClaimIssuedBefore(claims map[string]any, name string, unixSeconds int64) bool {
	claimSeconds, ok := ClaimUnixSeconds(claims, name)
	return ok && unixSeconds > 0 && claimSeconds < unixSeconds
}
