package restsupport

import (
	"errors"
	"net/http"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestRestSupport(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Ingestion REST support unit test suite")
}

var _ = Describe("REST responses", func() {
	It("returns status and payload for JSON responses", func() {
		res := NewJSONResponse(http.StatusCreated, []byte(`{"ok":true}`))

		Expect(res.StatusCode()).To(Equal(http.StatusCreated))
		Expect(res.Payload()).To(Equal([]byte(`{"ok":true}`)))
	})

	It("returns status-only responses", func() {
		res := NewReponse(http.StatusNoContent)

		Expect(res.StatusCode()).To(Equal(http.StatusNoContent))
		Expect(res.Payload()).To(BeNil())
	})
})

var _ = Describe("HTTPError", func() {
	It("wraps causes and preserves status codes", func() {
		cause := errors.New("decode failed")
		err := ErrBadRequest().WithMessage("bad payload").Wrap(cause)

		Expect(err.statusCode).To(Equal(http.StatusBadRequest))
		Expect(err.Error()).To(Equal("bad payload"))
		Expect(errors.Is(err, cause)).To(BeTrue())
	})

	It("uses standard messages for uncustomized errors", func() {
		Expect(ErrUnauthorized().Error()).To(Equal(http.StatusText(http.StatusUnauthorized)))
		Expect(ErrInternalServer().statusCode).To(Equal(http.StatusInternalServerError))
		Expect(ErrNotFound().statusCode).To(Equal(http.StatusNotFound))
	})
})
