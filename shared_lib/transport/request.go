package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"lib/shared_lib/authz"
	"net/http"
	"net/url"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
)

var (
	ErrInvalidAccountID = errors.New("invalid accountID")
	ErrInvalidOrderID   = errors.New("invalid orderID")
)

func ReadAccountID(ctx context.Context, r *http.Request) (uuid.UUID, error) {
	log.Trace("ReadAccountID")

	return readUUIDPathParam(ctx, r, "accountId", "account_id", ErrInvalidAccountID)
}

func ReadOrderID(ctx context.Context, r *http.Request) (uuid.UUID, error) {
	log.Trace("ReadOrderID")

	return readUUIDPathParam(ctx, r, "orderId", "order_id", ErrInvalidOrderID)
}

func readUUIDPathParam(ctx context.Context, r *http.Request, param, field string, target error) (uuid.UUID, error) {
	raw := mux.Vars(r)[param]
	id, err := uuid.Parse(raw)
	if err != nil || id == uuid.Nil {
		log.WithContext(ctx).WithError(err).WithField(field, raw).Warn(target.Error())
		return uuid.Nil, fmt.Errorf("validation error: %w", target)
	}
	return id, nil
}

func ReadIdempotencyIDHeader(ctx context.Context, r *http.Request) (uuid.UUID, error) {
	log.Trace("ReadIdempotencyIDHeader")

	idempotencyKey := r.Header.Get("X-Request-ID")
	if idempotencyKey == "" {
		log.WithContext(ctx).Error("idempotent key header missing")
		return uuid.Nil, fmt.Errorf("missing idempotency X-Request-ID header")
	}

	idempotencyUUID, err := uuid.Parse(idempotencyKey)
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("parse idempotent key failed")
		return uuid.Nil, fmt.Errorf("invalid idempotency key")
	}

	return idempotencyUUID, nil
}

func ReadUserIDHeader(ctx context.Context, r *http.Request) (uuid.UUID, error) {
	log.Trace("ReadUserIDHeader")

	return authz.ReadUserIDHeader(ctx, r)
}

func ReadOrgIDHeader(ctx context.Context, r *http.Request) (uuid.UUID, error) {
	log.Trace("ReadOrgIDHeader")

	return authz.ReadOrgIDHeader(ctx, r)
}

func ReadSessionIDHeader(ctx context.Context, r *http.Request) (string, error) {
	log.Trace("ReadSessionIDHeader")

	return authz.ReadSessionIDHeader(ctx, r)
}

func ReadReqBody(ctx context.Context, r *http.Request) ([]byte, error) {
	log.Trace("ReadReqBody")

	defer r.Body.Close()
	reqBody, err := io.ReadAll(r.Body)
	if err != nil {
		errResp := fmt.Errorf("failed to read the request body")
		log.WithContext(ctx).WithError(err).Error(errResp.Error())
		return nil, errResp
	}

	if len(reqBody) == 0 {
		errResp := fmt.Errorf("empty request body")
		log.WithContext(ctx).Error(errResp.Error())
		return nil, errResp
	}
	return reqBody, nil
}

func ReadPagination(ctx context.Context, r *http.Request) (*Pagination, error) {
	log.Trace("ReadPagination")

	parsedURL, err := url.Parse(r.URL.String())
	if err != nil {
		errResp := fmt.Errorf("failed to parse URL")
		log.WithContext(ctx).WithError(err).Error(errResp.Error())
		return nil, errResp
	}

	queryPaginationParams, err := PaginationFromURL(parsedURL)
	if err != nil {
		errResp := fmt.Errorf("failed to parse query parameters")
		log.WithContext(ctx).WithError(err).Error(errResp.Error())
		return nil, errResp
	}

	if queryPaginationParams.Page <= 0 || queryPaginationParams.Limit <= 0 {
		errResp := fmt.Errorf("invalid pagination values, page: %d, limit: %d", queryPaginationParams.Page, queryPaginationParams.Limit)
		log.WithContext(ctx).Error(errResp.Error())
		return nil, errResp
	}

	return &queryPaginationParams, nil
}

func ReadFilters(ctx context.Context, r *http.Request) ([]Filter, error) {
	log.Trace("ReadFilters")

	parsedURL, err := url.Parse(r.URL.String())
	if err != nil {
		errResp := fmt.Errorf("failed to parse URL")
		log.WithContext(ctx).WithError(err).Error(errResp.Error())
		return nil, errResp
	}

	filterParams, err := FiltersFromURL(parsedURL)
	if err != nil {
		// no filters provided is not an error
		return nil, nil
	}

	filters, err := ParseFilterParams(filterParams)
	if err != nil {
		errResp := fmt.Errorf("failed to parse filters, %s", err.Error())
		log.WithContext(ctx).WithError(err).Error(errResp.Error())
		return nil, errResp
	}

	return filters, nil
}
