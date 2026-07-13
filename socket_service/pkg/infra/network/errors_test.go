package network

import (
	"net/http"

	"socket_service/pkg/domain"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("socket error mapping", func() {
	It("maps domain validation errors to bad request and invalid-message responses", func() {
		err := domain.ErrValidationFailed.Extend("invalid payload")

		httpErr := mapSocketHTTPError(err)

		Expect(httpErr.statusCode).To(Equal(http.StatusBadRequest))
		Expect(httpErr.message).To(Equal("bad request"))
		Expect(socketErrorCode(err)).To(Equal(domain.ServerMessageCodeInvalidMessage))
		Expect(socketErrorMessage(err)).To(Equal("invalid message"))
	})

	It("maps domain dependency errors to gateway failures without leaking raw detail", func() {
		err := domain.ErrDependencyFailed.Extend("redis stream read failed")

		httpErr := mapSocketHTTPError(err)

		Expect(httpErr.statusCode).To(Equal(http.StatusBadGateway))
		Expect(httpErr.message).To(Equal("dependency failed"))
		Expect(socketErrorCode(err)).To(Equal(domain.ServerMessageCodeInternalError))
		Expect(socketErrorMessage(err)).To(Equal("dependency failed"))
	})
})
