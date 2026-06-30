package kms_test

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"fmt"
	"testing"

	kms "shared_lib/key_management"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestKMS(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "KMS unit test suite")
}

var _ = Describe("LocalKMS (RS256-compatible)", func() {
	var (
		ctx       context.Context
		kmsClient kms.KMSClient
		err       error

		expectedKeyID string
	)

	BeforeEach(func() {
		ctx = context.TODO()
		expectedKeyID = "local-dev-kms-key"
		kmsClient, err = kms.NewLocalKMS(ctx, expectedKeyID)
		Expect(err).NotTo(HaveOccurred())
		Expect(kmsClient).NotTo(BeNil())
	})

	Context("KeyID()", func() {
		It("should return the provided key ID", func() {
			Expect(kmsClient.KeyID()).To(Equal(expectedKeyID))
		})
	})

	Context("PublicKey()", func() {
		It("should return a valid RSA public key", func() {
			pubKey, err := kmsClient.PublicKey(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(pubKey).NotTo(BeNil())
			_, ok := any(pubKey).(*rsa.PublicKey)
			Expect(ok).To(BeTrue(), "public key should be *rsa.PublicKey")
		})
	})

	Context("SignJWT() and verify signature", func() {
		It("should sign a message and produce a valid RS256 signature", func() {
			// Pretend this is "<base64url(header)>.<base64url(payload)>"
			signingString := "eyJoZWFkZXIiOiAiZm9vIn0.eyJwYXlsb2FkIjogImJhciJ9"

			// Ask LocalKMS to sign it
			sig, err := kmsClient.SignJWT(ctx, signingString)
			Expect(err).NotTo(HaveOccurred())
			Expect(sig).NotTo(BeNil())
			Expect(len(sig)).To(BeNumerically(">", 0))

			// Get the public key so we can verify locally
			pubKey, err := kmsClient.PublicKey(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(pubKey).NotTo(BeNil())

			// RS256 = SHA256(signingString) + PKCS#1 v1.5 verify
			digest := sha256.Sum256([]byte(signingString))
			verifyErr := rsa.VerifyPKCS1v15(pubKey, crypto.SHA256, digest[:], sig)
			Expect(verifyErr).NotTo(HaveOccurred(), "signature should be valid for the given signing string")
		})
	})

	Context("Concurrency", func() {
		It("should handle concurrent signing and verification", func() {
			numGoroutines := 10
			done := make(chan bool, numGoroutines)

			for range numGoroutines {
				go func() {
					defer GinkgoRecover()

					// unique header.payload each goroutine
					signingString := fmt.Sprintf("header.%s", uuid.New().String())

					sig, err := kmsClient.SignJWT(ctx, signingString)
					Expect(err).NotTo(HaveOccurred())
					Expect(sig).NotTo(BeNil())
					Expect(len(sig)).To(BeNumerically(">", 0))

					pubKey, err := kmsClient.PublicKey(ctx)
					Expect(err).NotTo(HaveOccurred())
					Expect(pubKey).NotTo(BeNil())

					digest := sha256.Sum256([]byte(signingString))
					verifyErr := rsa.VerifyPKCS1v15(pubKey, crypto.SHA256, digest[:], sig)
					Expect(verifyErr).NotTo(HaveOccurred(), "signature should verify")

					done <- true
				}()
			}

			for range numGoroutines {
				<-done
			}
		})
	})
})
