package adapter

import (
	"context"
	domainErrors "data_registry_service/pkg/domain"
	"data_registry_service/pkg/domain/model"
	"fmt"
	pagination "lib/shared_lib/transport"

	log "github.com/sirupsen/logrus"
)

type FilterDTOAdapter interface {
	QueryFiltersToDatasetsFilters(context.Context, []pagination.Filter) ([]model.Filter, error)
}

func NewFilterDTOAdapter() FilterDTOAdapter {
	return &filterDTOAdapter{}
}

type filterDTOAdapter struct{}

func (a *filterDTOAdapter) QueryFiltersToDatasetsFilters(ctx context.Context, queryFilters []pagination.Filter) ([]model.Filter, error) {
	log.Trace("filterDTOAdapter QueryFiltersToDatasetsFilters")

	filters := make([]model.Filter, 0)
	for _, queryFilter := range queryFilters {
		var (
			filter model.Filter
			err    error
		)

		switch queryFilter.Field {
		case "category":
			filter, err = a.toCategoryFilter(queryFilter)

		default:
			log.WithContext(ctx).Errorf("unsuported datasets query filter: `%s`", queryFilter)
			return nil, domainErrors.ErrValidationFailed.Extend(fmt.Sprintf("unsuported datasets query filter: `%s`", queryFilter))
		}

		if err != nil {
			log.WithContext(ctx).Errorf("datasets filter parsing from query filter failed: `%s`", queryFilter)
			return nil, domainErrors.ErrValidationFailed.Extend(fmt.Sprintf("datasets filter parsing from query filter failed: `%s`", queryFilter))
		}

		filters = append(filters, filter)
	}

	return filters, nil
}

func (a *filterDTOAdapter) toCategoryFilter(queryFilter pagination.Filter) (model.Filter, error) {
	log.Trace("filterDTOAdapter toCategoryFilter")

	if queryFilter.Operation != pagination.FilterOpIn {
		return nil, fmt.Errorf("invalid operation for category filter: %s", queryFilter.Operation)
	}
	if len(queryFilter.Values) == 0 {
		return nil, fmt.Errorf("empty values for category filter")
	}

	filter := &model.CategoryFilter{
		Values: queryFilter.Values,
	}
	return filter, nil
}
