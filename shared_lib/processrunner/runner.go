package processrunner

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	defaultTerminationGrace         = 5 * time.Second
	defaultCapturedOutputLimitBytes = 4 * 1024 * 1024
)

var (
	ErrCanceled = errors.New("process canceled")
	ErrTimeout  = errors.New("process timed out")
)

type Command struct {
	Name             string
	Args             []string
	Dir              string
	Env              []string
	Timeout          time.Duration
	TerminationGrace time.Duration
	StdoutLimitBytes int64
	StderrLimitBytes int64
}

type Result struct {
	Stdout   []byte
	Stderr   string
	ExitCode int
}

type ExitError struct {
	Err      error
	Stderr   string
	ExitCode int
}

func (e *ExitError) Error() string {
	log.Trace("ExitError Error")

	details := strings.TrimSpace(e.Stderr)
	if details == "" {
		return e.Err.Error()
	}
	return e.Err.Error() + ": " + details
}

func (e *ExitError) Unwrap() error {
	log.Trace("ExitError Unwrap")

	return e.Err
}

func Run(ctx context.Context, command Command) (Result, error) {
	log.Trace("processrunner Run")

	stdout := newLimitedBuffer(outputLimit(command.StdoutLimitBytes))
	result, err := run(ctx, command, &stdout, nil)
	result.Stdout = stdout.Bytes()
	return result, err
}

func StreamStdout(ctx context.Context, command Command, consume func(io.Reader) error) (Result, error) {
	log.Trace("processrunner StreamStdout")

	return run(ctx, command, nil, consume)
}

func run(ctx context.Context, command Command, stdout io.Writer, consume func(io.Reader) error) (Result, error) {
	log.Trace("processrunner run")

	runCtx := ctx
	cancel := func() {}
	timedOut := false
	if command.Timeout > 0 {
		runCtx, cancel = context.WithTimeout(ctx, command.Timeout)
	}
	defer cancel()

	cmd := exec.Command(command.Name, command.Args...)
	cmd.Dir = strings.TrimSpace(command.Dir)
	if len(command.Env) > 0 {
		cmd.Env = command.Env
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stderr := newLimitedBuffer(outputLimit(command.StderrLimitBytes))
	cmd.Stderr = &stderr
	if stdout != nil {
		cmd.Stdout = stdout
	}

	var stdoutPipe io.ReadCloser
	var err error
	if consume != nil {
		stdoutPipe, err = cmd.StdoutPipe()
		if err != nil {
			return Result{}, fmt.Errorf("stdout pipe: %w", err)
		}
	}
	if err := cmd.Start(); err != nil {
		return Result{}, err
	}

	waitDone := make(chan error, 1)
	go func() {
		waitDone <- cmd.Wait()
	}()

	var waitErr error
	var consumeErr error
	canceledByContext := false
	if consume == nil {
		select {
		case waitErr = <-waitDone:
		case <-runCtx.Done():
			canceledByContext = true
			timedOut = errors.Is(runCtx.Err(), context.DeadlineExceeded)
			waitErr = terminateProcessGroup(cmd, command.TerminationGrace, waitDone)
		}
		return commandResult(timedOut, canceledByContext, waitErr, nil, stderr.String())
	}

	consumeDone := make(chan error, 1)
	if consume != nil {
		go func() {
			consumeDone <- consume(stdoutPipe)
		}()
	}

	select {
	case consumeErr = <-consumeDone:
		if consumeErr != nil {
			waitErr = terminateProcessGroup(cmd, command.TerminationGrace, waitDone)
		} else {
			waitErr = <-waitDone
		}
	case waitErr = <-waitDone:
		consumeErr = <-consumeDone
	case <-runCtx.Done():
		canceledByContext = true
		timedOut = errors.Is(runCtx.Err(), context.DeadlineExceeded)
		waitErr = terminateProcessGroup(cmd, command.TerminationGrace, waitDone)
		consumeErr = <-consumeDone
	}

	return commandResult(timedOut, canceledByContext, waitErr, consumeErr, stderr.String())
}

func commandResult(timedOut bool, canceledByContext bool, waitErr error, consumeErr error, stderr string) (Result, error) {
	log.Trace("processrunner commandResult")

	stderrText := strings.TrimSpace(stderr)
	result := Result{Stderr: stderrText, ExitCode: exitCode(waitErr)}
	switch {
	case timedOut:
		return result, &ExitError{Err: ErrTimeout, Stderr: stderrText, ExitCode: result.ExitCode}
	case canceledByContext:
		return result, &ExitError{Err: ErrCanceled, Stderr: stderrText, ExitCode: result.ExitCode}
	case consumeErr != nil:
		return result, consumeErr
	case waitErr != nil:
		return result, &ExitError{Err: waitErr, Stderr: stderrText, ExitCode: result.ExitCode}
	default:
		return result, nil
	}
}

func terminateProcessGroup(cmd *exec.Cmd, grace time.Duration, waitDone <-chan error) error {
	log.Trace("processrunner terminateProcessGroup")

	if cmd == nil || cmd.Process == nil {
		return nil
	}
	if grace <= 0 {
		grace = defaultTerminationGrace
	}
	pgid := -cmd.Process.Pid
	_ = syscall.Kill(pgid, syscall.SIGTERM)
	timer := time.NewTimer(grace)
	defer timer.Stop()
	select {
	case err := <-waitDone:
		return err
	case <-timer.C:
		_ = syscall.Kill(pgid, syscall.SIGKILL)
		return <-waitDone
	}
}

func exitCode(err error) int {
	log.Trace("processrunner exitCode")

	if err == nil {
		return 0
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode()
	}
	return -1
}

func outputLimit(configured int64) int64 {
	log.Trace("processrunner outputLimit")

	if configured > 0 {
		return configured
	}
	return defaultCapturedOutputLimitBytes
}

type limitedBuffer struct {
	buffer bytes.Buffer
	limit  int64
}

func newLimitedBuffer(limit int64) limitedBuffer {
	log.Trace("processrunner newLimitedBuffer")

	return limitedBuffer{limit: limit}
}

func (b *limitedBuffer) Write(payload []byte) (int, error) {
	log.Trace("processrunner limitedBuffer Write")

	if b.limit <= 0 {
		return len(payload), nil
	}
	remaining := b.limit - int64(b.buffer.Len())
	if remaining > 0 {
		if int64(len(payload)) < remaining {
			remaining = int64(len(payload))
		}
		_, _ = b.buffer.Write(payload[:remaining])
	}
	return len(payload), nil
}

func (b *limitedBuffer) Bytes() []byte {
	log.Trace("processrunner limitedBuffer Bytes")

	return b.buffer.Bytes()
}

func (b *limitedBuffer) String() string {
	log.Trace("processrunner limitedBuffer String")

	return b.buffer.String()
}
