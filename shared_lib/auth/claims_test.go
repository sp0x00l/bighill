package provider_test

import (
	"encoding/json"

	auth "lib/shared_lib/auth"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("ClaimUnixSeconds", func() {
	It("accepts whole-number JSON-decoded claim values", func() {
		iat, ok := auth.ClaimUnixSeconds(map[string]any{"iat": float64(1710000000)}, "iat")
		Expect(ok).To(BeTrue())
		Expect(iat).To(Equal(int64(1710000000)))
	})

	It("accepts integer claim values", func() {
		iat, ok := auth.ClaimUnixSeconds(map[string]any{"iat": int64(1710000000)}, "iat")
		Expect(ok).To(BeTrue())
		Expect(iat).To(Equal(int64(1710000000)))
	})

	It("accepts json.Number claim values", func() {
		iat, ok := auth.ClaimUnixSeconds(map[string]any{"iat": json.Number("1710000000")}, "iat")
		Expect(ok).To(BeTrue())
		Expect(iat).To(Equal(int64(1710000000)))
	})

	It("rejects missing, zero, and fractional values", func() {
		_, ok := auth.ClaimUnixSeconds(map[string]any{}, "iat")
		Expect(ok).To(BeFalse())

		_, ok = auth.ClaimUnixSeconds(map[string]any{"iat": float64(0)}, "iat")
		Expect(ok).To(BeFalse())

		_, ok = auth.ClaimUnixSeconds(map[string]any{"iat": float64(1710000000.5)}, "iat")
		Expect(ok).To(BeFalse())
	})
})

var _ = Describe("ClaimIssuedBefore", func() {
	It("rejects claims issued before a revocation timestamp", func() {
		Expect(auth.ClaimIssuedBefore(map[string]any{"iat": int64(1710000000)}, "iat", 1710000001)).To(BeTrue())
	})

	It("allows claims issued at the same second as the revocation timestamp", func() {
		Expect(auth.ClaimIssuedBefore(map[string]any{"iat": int64(1710000000)}, "iat", 1710000000)).To(BeFalse())
	})

	It("allows claims issued after the revocation timestamp", func() {
		Expect(auth.ClaimIssuedBefore(map[string]any{"iat": int64(1710000001)}, "iat", 1710000000)).To(BeFalse())
	})
})
