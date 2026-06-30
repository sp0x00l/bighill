package messaging

import (
	"errors"
	"fmt"
)

type nonRetryableError struct {
	err error
}

type alreadyProcessedError struct {
	err error
}

func (e nonRetryableError) Error() string {
	return e.err.Error()
}

func (e nonRetryableError) Unwrap() error {
	return e.err
}

func (e nonRetryableError) NonRetryable() bool {
	return true
}

func (e alreadyProcessedError) Error() string {
	return e.err.Error()
}

func (e alreadyProcessedError) Unwrap() error {
	return e.err
}

func (e alreadyProcessedError) AlreadyProcessed() bool {
	return true
}

// NonRetryable marks an already-classified handler error as deterministic.
// Services should make this decision close to their domain logic, not in the
// shared subscriber package.
func NonRetryable(err error) error {
	if err == nil {
		return nil
	}
	if IsNonRetryable(err) {
		return err
	}
	return nonRetryableError{err: err}
}

func IsNonRetryable(err error) bool {
	if err == nil {
		return false
	}
	var marked interface {
		NonRetryable() bool
	}
	return errors.As(err, &marked) && marked.NonRetryable()
}

// AlreadyProcessed marks an idempotent duplicate handler result. The subscriber
// commits these messages without retrying or DLQ-ing because the desired state
// has already been applied.
func AlreadyProcessed(err error) error {
	if err == nil {
		return nil
	}
	if IsAlreadyProcessed(err) {
		return err
	}
	return alreadyProcessedError{err: err}
}

func IsAlreadyProcessed(err error) bool {
	if err == nil {
		return false
	}
	var marked interface {
		AlreadyProcessed() bool
	}
	return errors.As(err, &marked) && marked.AlreadyProcessed()
}

// ErrorPolicy is the service-owned hook used by the shared subscriber retry
// flow. The shared subscriber owns backoff, DLQ, and commit behavior; services
// only decide whether a handler error is deterministic.
type ErrorPolicy interface {
	IsNonRetryableError(error) bool
}

type ErrorPolicyFunc func(error) bool

func (f ErrorPolicyFunc) IsNonRetryableError(err error) bool {
	return f(err)
}

func ConfigureErrorPolicy(sub Subscriber, policy ErrorPolicy) error {
	configurable, ok := sub.(interface {
		ConfigureErrorPolicy(ErrorPolicy)
	})
	if !ok {
		return fmt.Errorf("subscriber does not support error policy")
	}
	configurable.ConfigureErrorPolicy(policy)
	return nil
}
