package transport

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/gorilla/schema"
	log "github.com/sirupsen/logrus"
)

type FilterOperation string

type FilterParams struct {
	Filters []string `schema:"filter"`
}

// Add more operations as needed
const (
	FilterOpInvalid FilterOperation = "invalid"
	FilterOpIn      FilterOperation = "in"
	FilterOpNotIn   FilterOperation = "notin"
	FilterOpIs      FilterOperation = "is"
	FilterOpIsNot   FilterOperation = "isnot"
)

func StringToFilterOperation(s string) FilterOperation {
	switch s {
	case "in":
		return FilterOpIn
	case "notin":
		return FilterOpNotIn
	case "is":
		return FilterOpIs
	case "isnot":
		return FilterOpIsNot
	default:
		return FilterOpInvalid
	}
}

func FiltersFromURL(parsedURL *url.URL) ([]string, error) {
	decoder := schema.NewDecoder()
	queryValues := parsedURL.Query()

	var queryParams FilterParams
	if err := decoder.Decode(&queryParams, queryValues); err != nil {
		return queryParams.Filters, fmt.Errorf("decode filter parameters failed: %w", err)
	}

	return queryParams.Filters, nil
}

type Filter struct {
	Field     string
	Operation FilterOperation
	Values    []string
}

func ParseFilterParams(filterParams []string) ([]Filter, error) {
	log.Trace("ParseFilterParams")

	filters := make([]Filter, 0)

	var filterErrors []string
	for _, f := range filterParams {
		filterParts := strings.Split(f, ":")
		if len(filterParts) != 3 {
			filterErrors = append(filterErrors, fmt.Sprintf("validation error, invalid filter format: `%s`", f))
			continue
		}

		field := strings.TrimSpace(filterParts[0])
		operation := strings.TrimSpace(filterParts[1])
		value := strings.TrimSpace(filterParts[2])

		if field == "" || operation == "" || value == "" {
			filterErrors = append(filterErrors, fmt.Sprintf("validation error, filter field, operation and value must not be empty: `%s`", f))
			continue
		}

		filterOperation := StringToFilterOperation(operation)
		if filterOperation == FilterOpInvalid {
			filterErrors = append(filterErrors, fmt.Sprintf("validation error, invalid filter operation: `%s`", f))
			continue
		}

		filter := Filter{
			Field:     field,
			Operation: filterOperation,
			Values:    strings.Split(value, ","),
		}
		filters = append(filters, filter)
	}

	if len(filterErrors) > 0 {
		return nil, errors.New(strings.Join(filterErrors, ", \n "))
	}

	return filters, nil
}
