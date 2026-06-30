package transport

import (
	"html"
	"regexp"
	"strings"
	"unicode"

	"github.com/microcosm-cc/bluemonday"
)

// using a sqllib introduces cgo dependencies for all http services
// this is a best case scenario regex for catching basic SQL patterns
var sqlLikeRE = regexp.MustCompile(`(?is)^\s*(?:/\*.*?\*/\s*|--[^\n]*(?:\n|$)\s*)*(?:WITH\b.*?\bSELECT\b|SELECT\b.*?\bFROM\b|INSERT\b\s+INTO\b.*?\b(?:VALUES|SELECT)\b|UPDATE\b.*?\bSET\b|DELETE\b\s+FROM\b|TRUNCATE\b\s+(?:TABLE\s+)?\S+|CREATE\b\s+(?:TABLE|DATABASE|SCHEMA|INDEX|VIEW|FUNCTION|PROCEDURE)\b|ALTER\b\s+(?:TABLE|DATABASE|SCHEMA|INDEX|VIEW|FUNCTION|PROCEDURE)\b|DROP\b\s+(?:TABLE|DATABASE|SCHEMA|INDEX|VIEW|FUNCTION|PROCEDURE)\b)`)

// Note: This should not replace parameterized queries. It is just an additional layer of defense for upstream UGC ingestion.
var sanitizer = bluemonday.StrictPolicy()

func SanitizeUGC(input string) string {
	s := sanitizer.Sanitize(input)
	if looksLikeSQL(s) {
		return ""
	}
	if !hasMinNonSpaceRunes(s, 2) {
		return ""
	}
	return s
}

func looksLikeSQL(s string) bool {
	low := strings.ToLower(s)
	if !(strings.Contains(low, "select ") ||
		strings.Contains(low, "insert ") ||
		strings.Contains(low, "update ") ||
		strings.Contains(low, "delete ") ||
		strings.Contains(low, "drop ") ||
		strings.Contains(low, "create ") ||
		strings.Contains(low, "alter ") ||
		strings.Contains(low, "truncate ") ||
		strings.Contains(low, "union ") ||
		strings.Contains(low, "with ")) {
		return false
	}
	return sqlLikeRE.MatchString(s)
}

func hasMinNonSpaceRunes(s string, min int) bool {
	s = html.UnescapeString(s)

	count := 0
	for _, r := range s {
		switch r {
		// strip zero-width
		case '\u200B', '\u200C', '\u200D', '\u2060', '\uFEFF':
			continue
		default:
			if unicode.IsSpace(r) {
				continue
			}
			count++
			if count > min {
				return true
			}
		}
	}
	return false
}
