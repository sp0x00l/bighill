package network

import (
	"net/http"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("websocket origin policy", func() {
	It("allows only configured browser origins", func() {
		allowed := checkOrigin([]string{"https://app.bighill.io"})

		request, err := http.NewRequest(http.MethodGet, "/v1/socket", nil)
		Expect(err).NotTo(HaveOccurred())
		request.Header.Set("Origin", "https://app.bighill.io")
		Expect(allowed(request)).To(BeTrue())

		request.Header.Set("Origin", "https://attacker.example")
		Expect(allowed(request)).To(BeFalse())
	})

	It("allows non-browser clients without an Origin header", func() {
		allowed := checkOrigin([]string{"https://app.bighill.io"})
		request, err := http.NewRequest(http.MethodGet, "/v1/socket", nil)
		Expect(err).NotTo(HaveOccurred())

		Expect(allowed(request)).To(BeTrue())
	})
})
