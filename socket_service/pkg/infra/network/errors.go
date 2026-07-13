package network

import (
	"errors"
	"net/http"

	"socket_service/pkg/domain"

	log "github.com/sirupsen/logrus"
)

type httpError struct {
	statusCode int
	message    string
	err        error
}

func (e *httpError) Error() string {
	log.Trace("httpError Error")

	return e.message
}

func (e *httpError) Unwrap() error {
	log.Trace("httpError Unwrap")

	return e.err
}

func mapSocketHTTPError(err error) *httpError {
	log.Trace("mapSocketHTTPError")

	if errors.Is(err, domain.ErrValidationFailed) {
		return &httpError{statusCode: http.StatusBadRequest, message: "bad request", err: err}
	}
	if errors.Is(err, domain.ErrUnauthorized) {
		return &httpError{statusCode: http.StatusUnauthorized, message: "unauthorized", err: err}
	}
	if errors.Is(err, domain.ErrDependencyFailed) {
		return &httpError{statusCode: http.StatusBadGateway, message: "dependency failed", err: err}
	}
	if errors.Is(err, domain.ErrBackpressure) {
		return &httpError{statusCode: http.StatusServiceUnavailable, message: "backpressure", err: err}
	}
	return &httpError{statusCode: http.StatusInternalServerError, message: "internal server error", err: err}
}

func socketErrorCode(err error) domain.ServerMessageCode {
	log.Trace("socketErrorCode")

	if errors.Is(err, domain.ErrValidationFailed) {
		return domain.ServerMessageCodeInvalidMessage
	}
	if errors.Is(err, domain.ErrUnauthorized) {
		return domain.ServerMessageCodeUnauthorized
	}
	return domain.ServerMessageCodeInternalError
}

func socketErrorMessage(err error) string {
	log.Trace("socketErrorMessage")

	if errors.Is(err, domain.ErrValidationFailed) {
		return "invalid message"
	}
	if errors.Is(err, domain.ErrUnauthorized) {
		return "unauthorized"
	}
	if errors.Is(err, domain.ErrDependencyFailed) {
		return "dependency failed"
	}
	if errors.Is(err, domain.ErrBackpressure) {
		return "backpressure"
	}
	return "internal server error"
}
