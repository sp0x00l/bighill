package metrics

import (
	"context"
	"errors"
	"net/http"

	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func ClassifyGRPC(err error) (ErrorClass, string) {
	if err == nil {
		return ErrorClassUnknown, ""
	}
	st, ok := status.FromError(err)
	if !ok {
		return ErrorClassUnknown, ""
	}
	code := st.Code()
	return classifyGRPCCode(code), code.String()
}

func ClassifyHTTPStatus(statusCode int) ErrorClass {
	switch statusCode {
	case http.StatusRequestTimeout:
		return ErrorClassTimeout
	case http.StatusTooManyRequests:
		return ErrorClassRateLimit
	case http.StatusUnauthorized:
		return ErrorClassAuth
	case http.StatusForbidden:
		return ErrorClassPermission
	case http.StatusNotFound:
		return ErrorClassNotFound
	case http.StatusConflict:
		return ErrorClassConflict
	default:
		if statusCode >= 500 {
			return ErrorClassInternal
		}
	}
	return ErrorClassUnknown
}

func ClassifyDB(err error) ErrorClass {
	if err == nil {
		return ErrorClassUnknown
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return ErrorClassTimeout
	}
	if errors.Is(err, context.Canceled) {
		return ErrorClassCanceled
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23505", "23503", "40001":
			return ErrorClassConflict
		case "28P01":
			return ErrorClassAuth
		default:
			return ErrorClassDB
		}
	}
	return ErrorClassDB
}

func ClassifyRedis(err error) ErrorClass {
	if err == nil {
		return ErrorClassUnknown
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return ErrorClassTimeout
	}
	if errors.Is(err, context.Canceled) {
		return ErrorClassCanceled
	}
	return ErrorClassNetwork
}

func classifyGRPCCode(code codes.Code) ErrorClass {
	switch code {
	case codes.DeadlineExceeded:
		return ErrorClassTimeout
	case codes.Unavailable:
		return ErrorClassUnavailable
	case codes.Canceled:
		return ErrorClassCanceled
	case codes.ResourceExhausted:
		return ErrorClassRateLimit
	case codes.Unauthenticated:
		return ErrorClassAuth
	case codes.PermissionDenied:
		return ErrorClassPermission
	case codes.NotFound:
		return ErrorClassNotFound
	case codes.AlreadyExists, codes.Aborted:
		return ErrorClassConflict
	case codes.Internal:
		return ErrorClassInternal
	default:
		return ErrorClassUnknown
	}
}
