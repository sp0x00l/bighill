package materialization

import "context"

type ArtifactStore interface {
	Read(ctx context.Context, storageLocation string) ([]byte, error)
	Write(ctx context.Context, key, contentType string, body []byte) (string, error)
}
