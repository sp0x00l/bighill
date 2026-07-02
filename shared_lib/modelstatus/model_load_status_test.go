package modelstatus

import (
	"errors"
	"testing"
)

func TestModelLoadStatusString(t *testing.T) {
	if ModelLoadStatusNotLoaded.String() != "NOT_LOADED" {
		t.Fatalf("unexpected not-loaded string: %s", ModelLoadStatusNotLoaded.String())
	}
	if ModelLoadStatusLoaded.String() != "LOADED" {
		t.Fatalf("unexpected loaded string: %s", ModelLoadStatusLoaded.String())
	}
	if ModelLoadStatusFailed.String() != "FAILED" {
		t.Fatalf("unexpected failed string: %s", ModelLoadStatusFailed.String())
	}
	if ModelLoadStatus(99).String() != "UNKNOWN" {
		t.Fatalf("unexpected out-of-range string: %s", ModelLoadStatus(99).String())
	}
}

func TestToModelLoadStatus(t *testing.T) {
	cases := map[string]ModelLoadStatus{
		"":           ModelLoadStatusNotLoaded,
		"not_loaded": ModelLoadStatusNotLoaded,
		"LOADED":     ModelLoadStatusLoaded,
		" failed ":   ModelLoadStatusFailed,
	}

	for input, want := range cases {
		got, err := ToModelLoadStatus(input)
		if err != nil {
			t.Fatalf("unexpected error for %q: %v", input, err)
		}
		if got != want {
			t.Fatalf("unexpected status for %q: got=%s want=%s", input, got, want)
		}
	}
}

func TestToModelLoadStatusRejectsUnknown(t *testing.T) {
	_, err := ToModelLoadStatus("READY")
	if !errors.Is(err, ErrUnknownModelLoadStatus) {
		t.Fatalf("expected unknown model load status error, got %v", err)
	}
}
