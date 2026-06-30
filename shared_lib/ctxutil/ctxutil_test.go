package ctxutil_test

import (
	"context"
	"errors"
	"lib/shared_lib/ctxutil"
	"testing"
)

func TestIsCanceled(t *testing.T) {
	if !ctxutil.IsCanceled(context.Canceled) {
		t.Fatalf("expected context.Canceled to be treated as canceled")
	}
	if !ctxutil.IsCanceled(context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded to be treated as canceled")
	}
	if ctxutil.IsCanceled(errors.New("other")) {
		t.Fatalf("expected unrelated errors not to be treated as canceled")
	}
}
