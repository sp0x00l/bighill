package kms

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"sync"

	log "github.com/sirupsen/logrus"
)

type LocalKMS struct {
	keyID   string
	privKey *rsa.PrivateKey
	pubKey  *rsa.PublicKey

	mutex sync.RWMutex
}

var (
	localKMSInstance *LocalKMS
	localKMSOnce     sync.Once
	localDevKeyPEM   = `-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDdDvm3l1B2j5Xd
tbPzkmv5WVQRn1YWysV8ERJ9XHjFOUZ43jm+wmELhGd5H/DkP+iMEf85XHaTHpV3
kP9t9ESQtQMrLJhi/U3vFAcOfkeerfW0NMttAlQBF62g5WhK9QJFa/PzKJcB6ATM
jHl+j4ubul/Frhg6H/hoIDrtNFjyk6w5shR6Qr+JUDMYtoP70hlrIaCnGIpBA2NF
gkYBn2WEYDK6H06zr4TTQUXzekR3VLB4ouNSYuzGFBKV6VaH6wThnfmLWi1QqbVd
VRb6T9IVnwVaQaY9nbA5w0Li/Tj7ML0UVzl+ZK0xsjX4XBGj2z07w8J1jYN5jvmB
rv9HXyqdAgMBAAECggEAPXqFO6hzAc40FVmL5wFBUzMjPNVt+V/CZZtP45p8ogko
TsQrFiD0IWJc7qRR+ADIGXCF5TuQZKEcW4jxaPCGwyH8dBzucpVU/9k3jzHSlFB4
JXqLWtFLcJRXvwgeJb6XN5xq16Thvm97KjIlNewRHSnLqewdo8ixarAQA1lMJYYk
80L4vKiymPr274LBepYN5W44WF8M33WWS6XaZI99LAbw8ulqBiz5r8pnqVe1eBqN
iqugKtnEcWUgEGpCc64+8ahFNHlD5YFB09o+y3AqfoF/shiRxN7OPLsvsoc4A+xY
+HzbUmVeeejigRqTYkTR/zI7s+qiaiK+/2PwsFwvhwKBgQD3ZoNm/B2wIv9PcjNM
f2u/mkiGgzDhQxj8QMD2NqRZIf/+wAzaERTjdLTJiNUVckA8GsMeKeunOoSb1xf5
ymCXqDWKjIOPzyddHdBcWm9GXNbbNKInIRVNmvgCQgNGZzdpeR7rp5QlZsbnXRmT
YePO2vFNYVY17//IJH7WgaCM1wKBgQDkvg8cSekYrJlETgX5KxjZHgTyv5fy1fpV
XC+o0kdpECxKkGVTKdyuYPTPO3fh8qiwRH3XHZTh53roqvBcze+hL9cHltT7FIOY
3OIPJc0mYZg7ScPusldAq/2idalIo8Ue+u9wUwJSzyYYe3zb/9oAjKBxWtT8o3pg
I/0ft0XBqwKBgFKYrxYa5e6AQKzNe8L2Z4q4f64o7pDGTfkpxUJuS8BWUZlDlQbY
3RhzRkhinoFie3+Vj77qT/qs1skQrrh+kHERf46aCvJgPswfwAiVSME9DZ5xnBFk
QjB+pH5ce6ttmlpkTaZvdE5oWc+0jW1fKSdOgXFMJfQsBEFVreL/tBJRAoGARBox
9YIr3CTHHQb90El8hGfjoUJZwvriJTflGKZCjI08IpcLE8+K3IARYwGZl7PfdVtu
+/TatsdsWIlMNtU5WwwbQS8vCfH5nDFnPItMoPi9kilMJG0EfUS3pv7Q/8eCkM61
KwQL1QvHk9JwQi/SgAdeXWFluDIT5TvRyPeP1TECgYEApDpUyoju78N/MDzJSWky
7Ft2JDMUJfgVp3xLz3mGvXeF6HaJcep+XF+cUxZ8m0o7i+frTn7cQUn95nZuvA3A
Rw1LgjjGs/50rjeFHBcuHCt3jiajyn2+b2+QWeD8AbQwKOwazMHj6vl6cmhUzhMt
tOSKp3+Mky6W4kc2Su4AMv8=
-----END PRIVATE KEY-----`
)

func NewLocalKMS(ctx context.Context, keyID string) (KMSClient, error) {
	log.Trace("NewLocalKMS")

	var initErr error
	localKMSOnce.Do(func() {
		block, _ := pem.Decode([]byte(localDevKeyPEM))
		if block == nil {
			initErr = fmt.Errorf("failed to decode PEM block")
			return
		}

		priv, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			privKey, err := x509.ParsePKCS1PrivateKey(block.Bytes)
			if err != nil {
				initErr = fmt.Errorf("failed to parse private key: %w", err)
				return
			}
			priv = privKey
		}

		rsaPrivKey, ok := priv.(*rsa.PrivateKey)
		if !ok {
			initErr = fmt.Errorf("key is not an RSA private key")
			return
		}

		log.WithContext(ctx).Info("Using shared local KMS key for development")

		localKMSInstance = &LocalKMS{
			keyID:   keyID,
			privKey: rsaPrivKey,
			pubKey:  &rsaPrivKey.PublicKey,
		}
	})

	if initErr != nil {
		log.WithContext(ctx).WithError(initErr).Error("failed to initialize LocalKMS")
		return nil, initErr
	}

	return localKMSInstance, nil
}

func (k *LocalKMS) KeyID() string {
	k.mutex.RLock()
	defer k.mutex.RUnlock()
	return k.keyID
}

func (k *LocalKMS) SignJWT(ctx context.Context, signingString string) ([]byte, error) {
	log.Trace("LocalKMS SignJWT")

	hashed := sha256.Sum256([]byte(signingString))

	k.mutex.RLock()
	defer k.mutex.RUnlock()

	sig, err := rsa.SignPKCS1v15(rand.Reader, k.privKey, crypto.SHA256, hashed[:])
	if err != nil {
		log.WithContext(ctx).WithError(err).Error("failed to sign JWT with LocalKMS")
		return nil, fmt.Errorf("failed to sign JWT with LocalKMS: %w", err)
	}

	return sig, nil
}

func (k *LocalKMS) PublicKey(ctx context.Context) (*rsa.PublicKey, error) {
	log.Trace("LocalKMS PublicKey")

	k.mutex.RLock()
	defer k.mutex.RUnlock()

	return k.pubKey, nil
}
