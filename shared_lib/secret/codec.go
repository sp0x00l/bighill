package secret

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"strings"

	log "github.com/sirupsen/logrus"
)

const ciphertextPrefix = "v1:"

type Codec interface {
	Encrypt(context.Context, string) (string, error)
	Decrypt(context.Context, string) (string, error)
}

type AESGCMCodec struct {
	aead cipher.AEAD
}

func NewAESGCMCodec(keyMaterial string) (*AESGCMCodec, error) {
	log.Trace("NewAESGCMCodec")

	key, err := parseKeyMaterial(keyMaterial)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("create aes cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create aes-gcm codec: %w", err)
	}
	return &AESGCMCodec{aead: aead}, nil
}

func (c *AESGCMCodec) Encrypt(_ context.Context, plaintext string) (string, error) {
	log.Trace("AESGCMCodec Encrypt")

	if plaintext == "" {
		return "", nil
	}
	nonce := make([]byte, c.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", fmt.Errorf("generate secret nonce: %w", err)
	}
	sealed := c.aead.Seal(nonce, nonce, []byte(plaintext), nil)
	return ciphertextPrefix + base64.RawStdEncoding.EncodeToString(sealed), nil
}

func (c *AESGCMCodec) Decrypt(_ context.Context, ciphertext string) (string, error) {
	log.Trace("AESGCMCodec Decrypt")

	if ciphertext == "" {
		return "", nil
	}
	payload := strings.TrimPrefix(strings.TrimSpace(ciphertext), ciphertextPrefix)
	data, err := base64.RawStdEncoding.DecodeString(payload)
	if err != nil {
		return "", fmt.Errorf("decode secret ciphertext: %w", err)
	}
	nonceSize := c.aead.NonceSize()
	if len(data) < nonceSize {
		return "", fmt.Errorf("secret ciphertext is too short")
	}
	plaintext, err := c.aead.Open(nil, data[:nonceSize], data[nonceSize:], nil)
	if err != nil {
		return "", fmt.Errorf("decrypt secret ciphertext: %w", err)
	}
	return string(plaintext), nil
}

func parseKeyMaterial(keyMaterial string) ([]byte, error) {
	log.Trace("parseKeyMaterial")

	keyMaterial = strings.TrimSpace(keyMaterial)
	if keyMaterial == "" {
		return nil, fmt.Errorf("secret encryption key is required")
	}
	if decoded, err := base64.StdEncoding.DecodeString(keyMaterial); err == nil && len(decoded) == 32 {
		return decoded, nil
	}
	if decoded, err := base64.RawStdEncoding.DecodeString(keyMaterial); err == nil && len(decoded) == 32 {
		return decoded, nil
	}
	if decoded, err := hex.DecodeString(keyMaterial); err == nil && len(decoded) == 32 {
		return decoded, nil
	}
	if len([]byte(keyMaterial)) == 32 {
		return []byte(keyMaterial), nil
	}
	return nil, fmt.Errorf("secret encryption key must be 32 bytes, base64, or hex encoded 32 bytes")
}
