package model

import (
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
)

type FilterBy string

const (
	FilterByInvalid  FilterBy = "invalid"
	FilterByCategory FilterBy = "category"
)

func (f FilterBy) String() string {
	return string(f)
}

type Filter interface {
	GetType() FilterBy
	GetFilterAndFillArguments(string, map[string]any) string
}

type CategoryFilter struct {
	Values []string
}

func (f CategoryFilter) GetType() FilterBy {
	return FilterByCategory
}

func (f CategoryFilter) GetFilterAndFillArguments(field string, args map[string]any) string {
	log.Trace("CategoryFilter GetFilterAndFillArguments")

	var params []string
	for i, v := range f.Values {
		argName := "value" + "_" + strconv.Itoa(i)
		args[argName] = v

		params = append(params, "@"+argName)
	}
	values := strings.Join(params, ",")

	return field + " IN (" + values + ")"
}
