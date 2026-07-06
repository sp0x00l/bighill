package processrunner

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestProcessRunner(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Process runner unit test suite")
}

var _ = Describe("processrunner", func() {
	It("captures stdout and stderr for a successful command", func() {
		result, err := Run(context.Background(), shellCommand("printf out; printf err >&2"))

		Expect(err).NotTo(HaveOccurred())
		Expect(string(result.Stdout)).To(Equal("out"))
		Expect(result.Stderr).To(Equal("err"))
		Expect(result.ExitCode).To(Equal(0))
	})

	It("returns a typed exit error for failed commands", func() {
		result, err := Run(context.Background(), shellCommand("printf nope >&2; exit 7"))

		var exitErr *ExitError
		Expect(errors.As(err, &exitErr)).To(BeTrue())
		Expect(result.Stderr).To(Equal("nope"))
		Expect(exitErr.ExitCode).To(Equal(7))
	})

	It("terminates the process group on timeout", func() {
		command := shellCommand("trap 'exit 0' TERM; sleep 2")
		command.Timeout = 10 * time.Millisecond
		command.TerminationGrace = 1 * time.Millisecond

		_, err := Run(context.Background(), command)

		Expect(errors.Is(err, ErrTimeout)).To(BeTrue())
	})

	It("does not wait for the full termination grace when the child exits on SIGTERM", func() {
		command := shellCommand("trap 'exit 0' TERM; while true; do :; done")
		command.Timeout = 10 * time.Millisecond
		command.TerminationGrace = 2 * time.Second

		started := time.Now()
		_, err := Run(context.Background(), command)

		Expect(errors.Is(err, ErrTimeout)).To(BeTrue())
		Expect(time.Since(started)).To(BeNumerically("<", 750*time.Millisecond))
	})

	It("caps captured stdout and stderr", func() {
		command := shellCommand("printf 'abcdefghijklmnopqrstuvwxyz'; printf 'ABCDEFGHIJKLMNOPQRSTUVWXYZ' >&2")
		command.StdoutLimitBytes = 8
		command.StderrLimitBytes = 6

		result, err := Run(context.Background(), command)

		Expect(err).NotTo(HaveOccurred())
		Expect(string(result.Stdout)).To(Equal("abcdefgh"))
		Expect(result.Stderr).To(Equal("ABCDEF"))
	})

	It("streams stdout to a consumer", func() {
		var payload []byte

		_, err := StreamStdout(context.Background(), shellCommand("printf stream"), func(reader io.Reader) error {
			var readErr error
			payload, readErr = io.ReadAll(reader)
			return readErr
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(string(payload)).To(Equal("stream"))
	})
})

func shellCommand(script string) Command {
	return Command{
		Name: "/bin/sh",
		Args: []string{"-c", script},
	}
}
