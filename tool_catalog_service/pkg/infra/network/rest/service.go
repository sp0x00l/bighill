package rest

import (
	"context"
	"net/http"

	"lib/shared_lib/transport"

	"go.opentelemetry.io/otel"
)

type APIResponse interface {
	StatusCode() int
	Payload() []byte
}

type response struct {
	statusCode int
	payload    []byte
}

func (r response) StatusCode() int {
	return r.statusCode
}

func (r response) Payload() []byte {
	return r.payload
}

type Route struct {
	Handler  func(context.Context, *http.Request) (APIResponse, error)
	Path     string
	Method   string
	SpanName string
}

type Service struct {
	server *transport.HttpServer
}

func NewService(routes []Route, port int, name string) *Service {
	transportRoutes := make([]transport.Route, 0, len(routes))
	for _, route := range routes {
		handler := route.Handler
		transportRoutes = append(transportRoutes, transport.Route{
			Path:     route.Path,
			Method:   route.Method,
			SpanName: route.SpanName,
			Handler: func(ctx context.Context, r *http.Request) (int, []byte, error) {
				res, err := handler(ctx, r)
				if err != nil {
					status := http.StatusInternalServerError
					if httpErr, ok := err.(*HTTPError); ok {
						status = httpErr.statusCode
					}
					return status, nil, err
				}
				if res == nil {
					return http.StatusNoContent, nil, nil
				}
				return res.StatusCode(), res.Payload(), nil
			},
		})
	}
	return &Service{server: transport.NewHttpServer(otel.Tracer(name), transportRoutes, port, name)}
}

func (s *Service) Connect() error {
	return s.server.Connect()
}

func (s *Service) Close() {
	s.server.Close()
}

func (s *Service) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

func (s *Service) Ready() bool {
	return s.server.Ready()
}

func NewResponseWithPayload(statusCode int, payload []byte) APIResponse {
	return response{statusCode: statusCode, payload: payload}
}

type HTTPError struct {
	statusCode int
	message    string
	cause      error
}

func ErrBadRequest() *HTTPError {
	return &HTTPError{statusCode: http.StatusBadRequest, message: http.StatusText(http.StatusBadRequest)}
}

func ErrInternalServer() *HTTPError {
	return &HTTPError{statusCode: http.StatusInternalServerError, message: http.StatusText(http.StatusInternalServerError)}
}

func ErrNotFound() *HTTPError {
	return &HTTPError{statusCode: http.StatusNotFound, message: http.StatusText(http.StatusNotFound)}
}

func ErrForbidden() *HTTPError {
	return &HTTPError{statusCode: http.StatusForbidden, message: http.StatusText(http.StatusForbidden)}
}

func (e *HTTPError) Error() string {
	if e.message != "" {
		return e.message
	}
	if e.cause != nil {
		return e.cause.Error()
	}
	return http.StatusText(e.statusCode)
}

func (e *HTTPError) Unwrap() error {
	return e.cause
}

func (e *HTTPError) Wrap(err error) *HTTPError {
	next := *e
	next.cause = err
	return &next
}

func (e *HTTPError) WithMessage(message string) *HTTPError {
	e.message = message
	return e
}
