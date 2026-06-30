package rpc

import (
	"errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ = Describe("ExtractGRPCErrMsg", func() {
	It("preserves non-Unknown gRPC status codes", func() {
		err := ExtractGRPCErrMsg(status.Error(codes.FailedPrecondition, "position is pending liquidation"))

		st, ok := status.FromError(err)
		Expect(ok).To(BeTrue())
		Expect(st.Code()).To(Equal(codes.FailedPrecondition))
		Expect(st.Message()).To(Equal("position is pending liquidation"))
	})

	It("keeps Unknown status errors as plain messages", func() {
		err := ExtractGRPCErrMsg(status.Error(codes.Unknown, "order not found"))

		_, ok := status.FromError(err)
		Expect(ok).To(BeFalse())
		Expect(err).To(MatchError("order not found"))
	})

	It("keeps plain errors plain", func() {
		err := ExtractGRPCErrMsg(errors.New("plain error"))

		_, ok := status.FromError(err)
		Expect(ok).To(BeFalse())
		Expect(err).To(MatchError("plain error"))
	})
})

var _ = Describe("gRPC error predicates", func() {
	It("handles extracted status errors without string matching", func() {
		tests := []struct {
			name      string
			code      codes.Code
			message   string
			notFound  bool
			shouldLog bool
			retryable bool
		}{
			{name: "not found", code: codes.NotFound, message: "order not found", notFound: true, shouldLog: false, retryable: false},
			{name: "failed precondition", code: codes.FailedPrecondition, message: "position is pending liquidation", shouldLog: false, retryable: false},
			{name: "invalid argument", code: codes.InvalidArgument, message: "invalid request", shouldLog: false, retryable: false},
			{name: "already exists", code: codes.AlreadyExists, message: "idempotency key already exists", shouldLog: false, retryable: false},
			{name: "aborted", code: codes.Aborted, message: "deadlock detected", shouldLog: true, retryable: true},
			{name: "internal", code: codes.Internal, message: "database failed", shouldLog: true, retryable: false},
		}

		for _, tt := range tests {
			tt := tt
			By(tt.name)
			err := ExtractGRPCErrMsg(status.Error(tt.code, tt.message))
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(tt.code))
			Expect(IsNotFoundError(err)).To(Equal(tt.notFound))
			Expect(shouldLogUnaryClientError(st, err)).To(Equal(tt.shouldLog))
			Expect(isRetryable(st.Code())).To(Equal(tt.retryable))
		}
	})

	It("treats Aborted as retryable", func() {
		Expect(isRetryable(codes.Aborted)).To(BeTrue())
	})

	It("suppresses deterministic domain statuses from unary client logs", func() {
		for _, code := range []codes.Code{codes.AlreadyExists, codes.InvalidArgument, codes.FailedPrecondition} {
			Expect(shouldLogUnaryClientError(status.New(code, "domain rejection"), status.Error(code, "domain rejection"))).To(BeFalse())
		}
	})
})
