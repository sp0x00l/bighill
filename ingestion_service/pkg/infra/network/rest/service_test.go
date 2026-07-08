package rest_test

import (
	"errors"
	"net/http"

	serviceRest "ingestion_service/pkg/infra/network/rest"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("REST responses", func() {
	It("returns status and payload for JSON responses", func() {
		res := serviceRest.NewJSONResponse(http.StatusCreated, []byte(`{"ok":true}`))

		Expect(res.StatusCode()).To(Equal(http.StatusCreated))
		Expect(res.Payload()).To(Equal([]byte(`{"ok":true}`)))
	})

	It("returns status-only responses", func() {
		res := serviceRest.NewReponse(http.StatusNoContent)

		Expect(res.StatusCode()).To(Equal(http.StatusNoContent))
		Expect(res.Payload()).To(BeNil())
	})
})

var _ = Describe("HTTPError", func() {
	It("wraps causes and preserves status codes", func() {
		cause := errors.New("decode failed")
		err := serviceRest.ErrBadRequest().WithMessage("bad payload").Wrap(cause)

		Expect(err.Error()).To(Equal("bad payload"))
		Expect(errors.Is(err, cause)).To(BeTrue())
		Expect(err.StatusCode()).To(Equal(http.StatusBadRequest))
	})

	It("uses standard messages for uncustomized errors", func() {
		Expect(serviceRest.ErrUnauthorized().Error()).To(Equal(http.StatusText(http.StatusUnauthorized)))
		Expect(serviceRest.ErrInternalServer().Error()).To(Equal(http.StatusText(http.StatusInternalServerError)))
		Expect(serviceRest.ErrNotFound().Error()).To(Equal(http.StatusText(http.StatusNotFound)))
		Expect(serviceRest.ErrServiceUnavailable().Error()).To(Equal(http.StatusText(http.StatusServiceUnavailable)))
	})
})
