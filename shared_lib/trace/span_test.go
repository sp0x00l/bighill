package trace

import (
	"context"
	"errors"
	"testing"
)

func TestExpectedSpanErrorClassifierComposes(t *testing.T) {
	errA := errors.New("a")
	errB := errors.New("b")
	errC := errors.New("c")

	ctx := ContextWithExpectedErrorClassifier(context.Background(), func(err error) bool {
		return errors.Is(err, errA)
	})
	ctx = ContextWithExpectedErrorClassifier(ctx, func(err error) bool {
		return errors.Is(err, errB)
	})

	if !IsExpectedSpanError(ctx, errA) {
		t.Fatal("expected first classifier to match")
	}
	if !IsExpectedSpanError(ctx, errB) {
		t.Fatal("expected second classifier to match")
	}
	if IsExpectedSpanError(ctx, errC) {
		t.Fatal("did not expect unrelated error to match")
	}
}
