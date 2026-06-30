package rpc

import (
	"context"
	"errors"
	"net/http"
	"strings"

	log "github.com/sirupsen/logrus"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ErrorMatcher func(error) bool

type HTTPStatusRule struct {
	Status int
	Match  ErrorMatcher
}

type GRPCCodeRule struct {
	Code  codes.Code
	Match ErrorMatcher
}

func HTTPStatus(status int, targets ...error) HTTPStatusRule {
	return HTTPStatusRule{Status: status, Match: MatchAny(targets...)}
}

func HTTPStatusFunc(status int, match ErrorMatcher) HTTPStatusRule {
	return HTTPStatusRule{Status: status, Match: match}
}

func GRPCCode(code codes.Code, targets ...error) GRPCCodeRule {
	return GRPCCodeRule{Code: code, Match: MatchAny(targets...)}
}

func GRPCCodeFunc(code codes.Code, match ErrorMatcher) GRPCCodeRule {
	return GRPCCodeRule{Code: code, Match: match}
}

func MatchAny(targets ...error) ErrorMatcher {
	return func(err error) bool {
		if err == nil {
			return false
		}
		for _, target := range targets {
			if target != nil && errors.Is(err, target) {
				return true
			}
		}
		return false
	}
}

func MapToHTTPStatus(err error, rules ...HTTPStatusRule) int {
	if err == nil {
		return http.StatusOK
	}
	for _, rule := range rules {
		if rule.Match != nil && rule.Match(err) {
			return rule.Status
		}
	}
	if errors.Is(err, context.Canceled) {
		return 499
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return http.StatusGatewayTimeout
	}
	if st, ok := status.FromError(err); ok {
		return MapGRPCCodeToHTTPStatus(st.Code())
	}
	return http.StatusInternalServerError
}

func MapGRPCCodeToHTTPStatus(code codes.Code) int {
	switch code {
	case codes.OK:
		return http.StatusOK
	case codes.Canceled:
		return 499
	case codes.InvalidArgument, codes.OutOfRange:
		return http.StatusBadRequest
	case codes.FailedPrecondition:
		return http.StatusPreconditionFailed
	case codes.NotFound:
		return http.StatusNotFound
	case codes.AlreadyExists, codes.Aborted:
		return http.StatusConflict
	case codes.PermissionDenied:
		return http.StatusForbidden
	case codes.Unauthenticated:
		return http.StatusUnauthorized
	case codes.ResourceExhausted:
		return http.StatusTooManyRequests
	case codes.Unimplemented:
		return http.StatusNotImplemented
	case codes.Unavailable:
		return http.StatusServiceUnavailable
	case codes.DeadlineExceeded:
		return http.StatusGatewayTimeout
	default:
		return http.StatusInternalServerError
	}
}

func MapToGRPCStatus(err error, rules ...GRPCCodeRule) codes.Code {
	if err == nil {
		return codes.OK
	}
	for _, rule := range rules {
		if rule.Match != nil && rule.Match(err) {
			return rule.Code
		}
	}
	if errors.Is(err, context.Canceled) {
		return codes.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return codes.DeadlineExceeded
	}
	if st, ok := status.FromError(err); ok {
		return st.Code()
	}
	return codes.Unknown
}

func IsNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	if st, ok := status.FromError(err); ok {
		if st.Code() == codes.NotFound {
			return true
		}
		if strings.Contains(strings.ToLower(st.Message()), "not found") {
			return true
		}
	}
	return strings.Contains(strings.ToLower(err.Error()), "not found")
}

func ExtractGRPCErrMsg(err error) error {
	log.Trace("ExtractGRPCErrMsg")
	if err == nil {
		return nil
	}

	if st, ok := status.FromError(err); ok {
		if st.Code() == codes.Unknown {
			return errors.New(st.Message())
		}
		return status.Error(st.Code(), st.Message())
	}

	const marker = "desc = "
	if i := strings.Index(err.Error(), marker); i >= 0 {
		return errors.New(err.Error()[i+len(marker):])
	}
	return err
}
