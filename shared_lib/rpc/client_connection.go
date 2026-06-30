package rpc

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"time"

	metrics "lib/shared_lib/metrics"

	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

// Allow disabling retries per-call by setting ctx value "no-retry" = true.
type ctxKey string

type Config struct {
	Address          string
	Insecure         bool
	BlockUntilReady  bool
	DialTimeout      time.Duration
	MaxRetryAttempts int // per-call (idempotent) retry attempts
	PerCallTimeout   time.Duration
}

func NewClient(ctx context.Context, cfg Config, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	log.Trace("rpc NewClient")

	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = 5000 * time.Millisecond
	}
	if cfg.MaxRetryAttempts == 0 {
		cfg.MaxRetryAttempts = 3
	}
	if cfg.PerCallTimeout == 0 {
		cfg.PerCallTimeout = 30000 * time.Millisecond
	}

	svcCfg := `{
	  "methodConfig": [{
	    "name": [{}],
	    "retryPolicy": {
	      "MaxAttempts": ` + itoa(cfg.MaxRetryAttempts+1) + `,
	      "InitialBackoff": "0.2s",
	      "MaxBackoff": "2s",
	      "BackoffMultiplier": 1.6,
	      "RetryableStatusCodes": ["UNAVAILABLE","RESOURCE_EXHAUSTED","ABORTED"]
	    }
	  }]
	}`

	dialOpts := []grpc.DialOption{
		grpc.WithDefaultServiceConfig(svcCfg),
		grpc.WithConnectParams(grpc.ConnectParams{
			MinConnectTimeout: 2 * time.Second,
			Backoff: backoff.Config{
				BaseDelay:  200 * time.Millisecond,
				Multiplier: 1.6,
				Jitter:     0.2,
				MaxDelay:   5 * time.Second,
			},
		}),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                75 * time.Second, // >= server MinTime (here 60s)
			Timeout:             10 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		grpc.WithChainUnaryInterceptor(
			metricsUnaryInterceptor(),
			simpleRetryUnaryInterceptor(cfg.MaxRetryAttempts),
			timeoutUnaryInterceptor(cfg.PerCallTimeout),
		),
	}

	dialOpts = append(dialOpts, opts...)
	if cfg.Insecure {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	conn, err := grpc.NewClient(cfg.Address, dialOpts...)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func IsBackPressureTriggered(err error) bool {
	log.Trace("Connection IsBackPressureTriggered")
	if err == nil {
		return false
	}
	if st, ok := status.FromError(err); ok {
		return st.Code() == codes.DeadlineExceeded
	}
	return false
}

func metricsUnaryInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, callOpts ...grpc.CallOption) error {
		start := time.Now()
		err := invoker(ctx, method, req, reply, cc, callOpts...)
		statusLabel := codes.OK.String()
		if err != nil {
			class, statusCode := metrics.ClassifyGRPC(err)
			if statusCode != "" {
				statusLabel = statusCode
			} else {
				statusLabel = codes.Unknown.String()
			}
			metrics.Default().RecordError(ctx, metrics.BoundaryGrpcClient, method, class, statusLabel)
		}
		metrics.Default().RecordRequest(ctx, metrics.BoundaryGrpcClient, method, statusLabel)
		metrics.Default().RecordDuration(ctx, metrics.BoundaryGrpcClient, method, statusLabel, time.Since(start).Seconds())
		return err
	}
}

func simpleRetryUnaryInterceptor(maxAttempts int) grpc.UnaryClientInterceptor {
	log.Trace("accountClient simpleRetryUnaryInterceptor")
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, callOpts ...grpc.CallOption) error {
		log.Trace("accountClient simpleRetryUnaryInterceptor - invoker")
		var attempt int
		for {
			attempt++
			err := invoker(ctx, method, req, reply, cc, callOpts...)
			if err != nil {
				// Don't log context cancellation as error - it's expected during shutdown
				if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					st, _ := status.FromError(err)
					if shouldLogUnaryClientError(st, err) {
						log.WithContext(ctx).WithError(err).Error("accountClient simpleRetryUnaryInterceptor - invoker failed")
					}
				}
				st, _ := status.FromError(err)
				if attempt >= maxAttempts || !isRetryable(st.Code()) || !ctxRetryable(ctx) {
					return err
				}
				time.Sleep(expBackoffWithJitter(attempt))
				continue
			}
			return nil
		}
	}
}

func shouldLogUnaryClientError(st *status.Status, err error) bool {
	if IsNotFoundError(err) {
		return false
	}
	if st == nil {
		return true
	}
	switch st.Code() {
	case codes.AlreadyExists, codes.InvalidArgument, codes.FailedPrecondition:
		return false
	default:
		return true
	}
}

func isRetryable(c codes.Code) bool {
	return c == codes.Unavailable || c == codes.ResourceExhausted || c == codes.Aborted
}

func expBackoffWithJitter(attempt int) time.Duration {

	base := 200.0 * math.Pow(1.6, float64(attempt-1))
	max := 2000.0
	if base > max {
		base = max
	}
	// +/- 20% jitter
	j := base * (0.8 + 0.4*rand.Float64())
	return time.Duration(j) * time.Millisecond
}

func timeoutUnaryInterceptor(timeout time.Duration) grpc.UnaryClientInterceptor {
	log.Trace("accountClient timeoutUnaryInterceptor")

	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, callOpts ...grpc.CallOption) error {
		log.Trace("accountClient timeoutUnaryInterceptor - invoker")
		if _, hasDeadline := ctx.Deadline(); hasDeadline {
			return invoker(ctx, method, req, reply, cc, callOpts...)
		}
		c2, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		return invoker(c2, method, req, reply, cc, callOpts...)
	}
}

func ctxRetryable(ctx context.Context) bool {
	v := ctx.Value(ctxKey("no-retry"))
	if b, ok := v.(bool); ok && b {
		return false
	}
	return true
}

func itoa(i int) string {
	return fmt.Sprintf("%d", i)
}
