package transport

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/gorilla/mux"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/trace"
)

type Route struct {
	Handler  handlerFunc
	Path     string
	Method   string
	SpanName string
}

type HttpServer struct {
	tracer     trace.Tracer
	httpServer *http.Server
	router     *mux.Router
	name       string
	ready      atomic.Bool
}

func NewHttpServer(tracer trace.Tracer, routes []Route, httpPort int, name string) *HttpServer {
	log.Trace(name, "NewHttpServer")

	s := &HttpServer{
		tracer: tracer,
		router: mux.NewRouter().StrictSlash(true),
		name:   name,
	}

	s.addRoutes(routes)

	wrappedHandler := otelhttp.NewHandler(s.router, fmt.Sprintf("%s-http-server", name))
	s.httpServer = &http.Server{
		Addr:         fmt.Sprintf(":%d", httpPort),
		Handler:      wrappedHandler,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	return s
}

func (s *HttpServer) Connect() error {
	log.Trace("HttpServer Connect")

	listener, err := net.Listen("tcp", s.httpServer.Addr)
	if err != nil {
		return err
	}
	s.ready.Store(true)
	defer s.ready.Store(false)
	return s.httpServer.Serve(listener)
}

func (s *HttpServer) Close() {
	log.Trace("HttpServer Close")

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	if err := s.Shutdown(ctx); err != nil {
		log.Errorf("%s http server shutdown error: %v", s.name, err)
	}
}

func (s *HttpServer) Shutdown(ctx context.Context) error {
	log.Trace("HttpServer Shutdown")

	s.ready.Store(false)
	return s.httpServer.Shutdown(ctx)
}

func (s *HttpServer) Ready() bool {
	log.Trace("HttpServer Ready")

	return s.ready.Load()
}

func (s *HttpServer) addRoutes(routes []Route) {
	log.Trace("HttpServer addRoutes")

	for _, route := range routes {
		s.router.HandleFunc(route.Path, Middleware(s.tracer, route.SpanName, route.Handler)).Methods(route.Method)
	}

	// print server routes
	err := s.router.Walk(func(route *mux.Route, router *mux.Router, ancestors []*mux.Route) error {
		if path, err := route.GetPathTemplate(); err == nil {
			if method, err := route.GetMethods(); err == nil {
				log.Infof("%s service route: %v %s", s.name, method, path)
			}
		}
		return nil
	})

	if err != nil {
		log.Errorf("unable to print %s service routes: %v", s.name, err)
	}
}
