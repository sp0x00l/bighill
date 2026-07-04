package secret_test

import (
	"context"
	"testing"

	"lib/shared_lib/secret"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestSecretCodec(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Secret codec suite")
}

var _ = Describe("AESGCMCodec", func() {
	It("encrypts and decrypts a secret", func() {
		codec, err := secret.NewAESGCMCodec("01234567890123456789012345678901")
		Expect(err).NotTo(HaveOccurred())

		ciphertext, err := codec.Encrypt(context.Background(), "hf_token")
		Expect(err).NotTo(HaveOccurred())
		Expect(ciphertext).NotTo(Equal("hf_token"))

		plaintext, err := codec.Decrypt(context.Background(), ciphertext)
		Expect(err).NotTo(HaveOccurred())
		Expect(plaintext).To(Equal("hf_token"))
	})
})
