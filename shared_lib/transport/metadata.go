package transport

import (
	"context"
	"fmt"

	"net/url"
	"strings"

	log "github.com/sirupsen/logrus"
)

type Self struct {
	Href string `json:"href"`
}

type ResourceLinks struct {
	Self Self `json:"self"`
}

type Metadata struct {
	TotalCount int      `json:"total"`
	Page       int      `json:"page"`
	Limit      int      `json:"limit"`
	Filter     []Filter `json:"filter,omitempty"`
	NextURL    string   `json:"next,omitempty"`
}

func NewMetadata(ctx context.Context, totalCount int, pagination Pagination, filter []Filter, originalURL string) (Metadata, error) {
	log.Trace("metadata NewMetadata")

	res := Metadata{
		TotalCount: totalCount,
		Page:       pagination.Page,
		Limit:      pagination.Limit,
		Filter:     filter,
	}

	pagination.Page += 1

	if pagination.IsValidForCount(totalCount) {
		nextURL, err := nextURL(ctx, originalURL, pagination, filter)
		if err != nil {
			return res, err
		}
		res.NextURL = nextURL
	}

	return res, nil
}

func nextURL(ctx context.Context, originalURL string, pagination Pagination, filters []Filter) (string, error) {
	log.Trace("metadata GenerateNextURL")

	parsedURL, err := url.Parse(originalURL)
	if err != nil {
		log.WithContext(ctx).Errorf("parse original URL failed: %v", err)
		return "", fmt.Errorf("parse original URL failed: %w", err)
	}

	queryParts := []string{
		fmt.Sprintf("page=%d", pagination.Page),
		fmt.Sprintf("limit=%d", pagination.Limit),
	}

	for _, filter := range filters {
		queryParts = append(queryParts, fmt.Sprintf("filter=%s:%s:(%s)", filter.Field, filter.Operation, joinFilterValues(filter.Values)))
	}

	for _, part := range preserveQueryParts(parsedURL.RawQuery) {
		queryParts = append(queryParts, part)
	}

	return fmt.Sprintf("%s?%s", parsedURL.Path, strings.Join(queryParts, "&")), nil
}

func joinFilterValues(values []string) string {
	return url.QueryEscape(join(values, ","))
}

func preserveQueryParts(rawQuery string) []string {
	if rawQuery == "" {
		return nil
	}

	parts := strings.Split(rawQuery, "&")
	kept := make([]string, 0, len(parts))
	for _, part := range parts {
		if part == "" {
			continue
		}

		key := part
		if idx := strings.Index(part, "="); idx >= 0 {
			key = part[:idx]
		}
		unescapedKey, err := url.QueryUnescape(key)
		if err != nil {
			unescapedKey = key
		}

		switch unescapedKey {
		case "page", "limit", "offset", "filter":
			continue
		default:
			kept = append(kept, part)
		}
	}

	return kept
}

func join(elems []string, sep string) string {
	if len(elems) == 0 {
		return ""
	}
	result := elems[0]
	for _, s := range elems[1:] {
		result += sep + s
	}
	return result
}
