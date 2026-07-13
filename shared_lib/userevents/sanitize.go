package userevents

import (
	"regexp"
	"strings"

	log "github.com/sirupsen/logrus"
)

const defaultTechnicalDetailLimit = 512

var (
	credentialPattern = regexp.MustCompile(`(?i)(token|password|secret|signature|x-amz-signature|credential)=([^&\s]+)`)
	filePathPattern   = regexp.MustCompile(`(/[A-Za-z0-9._@%+\-]+)+`)
)

func SanitizeEvent(event Event) Event {
	log.Trace("SanitizeEvent")

	event.Message = SanitizeUserText(event.Message)
	event.Title = SanitizeUserText(event.Title)
	if event.Error != nil {
		errCopy := *event.Error
		errCopy.Message = SanitizeUserText(errCopy.Message)
		errCopy.TechnicalDetail = LimitTechnicalDetail(SanitizeTechnicalDetail(errCopy.TechnicalDetail), defaultTechnicalDetailLimit)
		errCopy.Remediation = SanitizeUserText(errCopy.Remediation)
		event.Error = &errCopy
	}
	return event
}

func SanitizeUserText(value string) string {
	log.Trace("SanitizeUserText")

	return strings.TrimSpace(strings.ReplaceAll(value, "\x00", ""))
}

func SanitizeTechnicalDetail(value string) string {
	log.Trace("SanitizeTechnicalDetail")

	value = SanitizeUserText(value)
	value = credentialPattern.ReplaceAllString(value, "$1=<redacted>")
	value = filePathPattern.ReplaceAllStringFunc(value, func(match string) string {
		if strings.HasPrefix(match, "/v1/") || strings.HasPrefix(match, "/models/") || strings.HasPrefix(match, "/datasets/") {
			return match
		}
		return "<path>"
	})
	return value
}

func LimitTechnicalDetail(value string, limit int) string {
	log.Trace("LimitTechnicalDetail")

	value = strings.TrimSpace(value)
	if limit <= 0 || len(value) <= limit {
		return value
	}
	return value[:limit] + "..."
}
