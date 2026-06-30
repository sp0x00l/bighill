package rpc_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	rpc "lib/shared_lib/rpc"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ = Describe("error status mapping", func() {
	It("maps domain rules before falling back to gRPC status", func() {
		errNotFound := errors.New("domain not found")
		err := fmt.Errorf("read account: %w", errNotFound)

		Expect(rpc.MapToHTTPStatus(err, rpc.HTTPStatus(http.StatusNotFound, errNotFound))).To(Equal(http.StatusNotFound))
	})

	It("maps gRPC codes to HTTP statuses", func() {
		tests := []struct {
			code codes.Code
			want int
		}{
			{code: codes.InvalidArgument, want: http.StatusBadRequest},
			{code: codes.FailedPrecondition, want: http.StatusPreconditionFailed},
			{code: codes.NotFound, want: http.StatusNotFound},
			{code: codes.AlreadyExists, want: http.StatusConflict},
			{code: codes.Aborted, want: http.StatusConflict},
			{code: codes.Unauthenticated, want: http.StatusUnauthorized},
			{code: codes.PermissionDenied, want: http.StatusForbidden},
			{code: codes.Unavailable, want: http.StatusServiceUnavailable},
			{code: codes.DeadlineExceeded, want: http.StatusGatewayTimeout},
			{code: codes.Internal, want: http.StatusInternalServerError},
		}

		for _, tt := range tests {
			Expect(rpc.MapToHTTPStatus(status.Error(tt.code, "grpc failure"))).To(Equal(tt.want), tt.code.String())
		}
	})

	It("maps native context errors without falling through to 500", func() {
		Expect(rpc.MapToHTTPStatus(context.Canceled)).To(Equal(499))
		Expect(rpc.MapToHTTPStatus(context.DeadlineExceeded)).To(Equal(http.StatusGatewayTimeout))
		Expect(rpc.MapToGRPCStatus(context.Canceled)).To(Equal(codes.Canceled))
		Expect(rpc.MapToGRPCStatus(context.DeadlineExceeded)).To(Equal(codes.DeadlineExceeded))
	})

	It("maps domain rules to gRPC codes and preserves existing gRPC status", func() {
		errValidation := errors.New("validation failed")

		Expect(rpc.MapToGRPCStatus(
			fmt.Errorf("wrapped: %w", errValidation),
			rpc.GRPCCode(codes.InvalidArgument, errValidation),
		)).To(Equal(codes.InvalidArgument))
		Expect(rpc.MapToGRPCStatus(status.Error(codes.Aborted, "deadlock"))).To(Equal(codes.Aborted))
	})
})
