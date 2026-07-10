package idem

import (
	"os"
	"os/exec"
	"testing"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestIdem(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Idempotency unit test suite")
}

var _ = Describe("Idempotency keys", func() {
	It("matches FromParts for UUID seeds", func() {
		seed := uuid.MustParse("11111111-1111-1111-1111-111111111111")

		Expect(Key(seed, OAuthProfile, "verified")).To(Equal(FromParts(seed.String(), OAuthProfile, "verified")))
	})

	It("preserves canonical part order", func() {
		got := Join(Outbox, "profile", "user-updated", "22222222-2222-2222-2222-222222222222", "payload-hash", "2026-05-07T00:00:00Z")
		Expect(got).To(Equal("outbox:profile:user-updated:22222222-2222-2222-2222-222222222222:payload-hash:2026-05-07T00:00:00Z"))
	})

	It("rejects empty parts", func() {
		if os.Getenv("BIGHILL_IDEM_EMPTY_PART_FATAL_TEST") == "1" {
			Join(Outbox, "")
			return
		}

		cmd := exec.Command(os.Args[0], "-test.run=TestIdem")
		cmd.Env = append(os.Environ(), "BIGHILL_IDEM_EMPTY_PART_FATAL_TEST=1")

		err := cmd.Run()

		Expect(err).To(HaveOccurred())
		exitErr, ok := err.(*exec.ExitError)
		Expect(ok).To(BeTrue())
		Expect(exitErr.ExitCode()).NotTo(Equal(0))
	})
})
