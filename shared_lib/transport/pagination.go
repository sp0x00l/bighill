package transport

import (
	"fmt"
	"net/url"
	"strconv"
)

const (
	MaximumPageSize = 1000
	DefaultPageSize = 25
	DefaultPage     = 1
)

type Pagination struct {
	Limit int
	Page  int
}

func PaginationFromURL(parsedURL *url.URL) (Pagination, error) {
	q := parsedURL.Query()
	var p Pagination

	s := q.Get("limit")
	if s == "" {
		p.Limit = DefaultPageSize
	} else {
		v, err := strconv.Atoi(s)
		if err != nil {
			return p, fmt.Errorf("invalid limit %q: %w", s, err)
		}
		p.Limit = v
	}
	if p.Limit <= 0 {
		return p, fmt.Errorf("limit must be greater than zero")
	}
	if p.Limit > MaximumPageSize {
		return p, fmt.Errorf("limit must be less than or equal to %d", MaximumPageSize)
	}

	offset := q.Get("offset")
	page := q.Get("page")
	if offset != "" && page != "" {
		return p, fmt.Errorf("use either offset or page, not both")
	}

	if offset != "" {
		v, err := strconv.Atoi(offset)
		if err != nil {
			return p, fmt.Errorf("invalid offset %q: %w", offset, err)
		}
		if v < 0 {
			return p, fmt.Errorf("offset must be greater than or equal to zero")
		}
		p.Page = (v / p.Limit) + 1
		return p, nil
	}

	if page == "" {
		p.Page = DefaultPage
		return p, nil
	}

	v, err := strconv.Atoi(page)
	if err != nil {
		return p, fmt.Errorf("invalid page %q: %w", page, err)
	}
	if v < 0 {
		return p, fmt.Errorf("page must be greater than zero")
	}
	if v == 0 {
		p.Page = DefaultPage
		return p, nil
	}
	p.Page = v
	return p, nil
}

func NewPagination(page, limit int) *Pagination {
	p := Pagination{
		Page:  page,
		Limit: limit,
	}
	if p.Page <= 0 {
		p.Page = DefaultPage
	}
	if p.Limit <= 0 {
		p.Limit = DefaultPageSize
	}
	if p.Limit > MaximumPageSize {
		p.Limit = MaximumPageSize
	}
	return &p
}

func (p *Pagination) GetOffset() int {
	if p.Page <= 1 {
		return 0
	}
	return (p.Page - 1) * p.Limit
}

func (p Pagination) IsValidForCount(count int) bool {
	return count > p.GetOffset()
}
