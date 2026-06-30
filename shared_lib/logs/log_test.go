package logs_test

import (
	"os"

	"testing"

	env "lib/shared_lib/env"
	"lib/shared_lib/logs"

	log "github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestLogConfig(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "logger config unit test suite")
}

var _ = Describe("log environment variables", func() {
	var (
		hook *logtest.Hook
	)

	Describe("Reading debug env variables for TRACE level logging", func() {
		Context("when the LOG_LEVEL environment variable is LOCAL-DEV", func() {
			BeforeEach(func() {
				os.Setenv("ENVIRONMENT", "LOCAL-DEV")
				env.ResetEnvironmentCache()

				hook = new(logtest.Hook)
				log.AddHook(hook)
				logs.Init()
			})
			It("should write error messages to the logs", func() {
				log.Error("Error test")
				Expect(len(hook.Entries)).Should(Equal(int(1)))
				Expect(log.ErrorLevel).Should(Equal(hook.LastEntry().Level))
				Expect("Error test").Should(Equal(hook.LastEntry().Message))

				hook.Reset()
				Expect(hook.LastEntry()).Should(BeNil())
			})

			It("should write warn messages to the logs", func() {
				hook.Reset()

				log.Warn("Warn test")
				Expect(len(hook.Entries)).Should(Equal(int(1)))
				Expect(log.WarnLevel).Should(Equal(hook.LastEntry().Level))
				Expect("Warn test").Should(Equal(hook.LastEntry().Message))

				hook.Reset()
				Expect(hook.LastEntry()).Should(BeNil())
			})

			It("should write info messages to the logs", func() {
				hook.Reset()

				log.Info("Info test")
				Expect(len(hook.Entries)).Should(Equal(int(1)))
				Expect(log.InfoLevel).Should(Equal(hook.LastEntry().Level))
				Expect("Info test").Should(Equal(hook.LastEntry().Message))

				hook.Reset()
				Expect(hook.LastEntry()).Should(BeNil())
			})

			It("should write debug messages to the logs", func() {
				hook.Reset()

				log.Debug("Debug test")
				Expect(len(hook.Entries)).Should(Equal(int(1)))
				Expect(log.DebugLevel).Should(Equal(hook.LastEntry().Level))
				Expect("Debug test").Should(Equal(hook.LastEntry().Message))

				hook.Reset()
				Expect(hook.LastEntry()).Should(BeNil())
			})

			It("should write trace messages to the logs", func() {
				hook.Reset()

				log.Trace("Trace test")
				Expect(len(hook.Entries)).Should(Equal(int(1)))
				Expect(log.TraceLevel).Should(Equal(hook.LastEntry().Level))
				Expect("Trace test").Should(Equal(hook.LastEntry().Message))

				hook.Reset()
				Expect(hook.LastEntry()).Should(BeNil())
			})
		})
	})

	Context("when the LOG_LEVEL environment variable is PROD", func() {
		BeforeEach(func() {
			os.Setenv("ENVIRONMENT", "PROD")
			env.ResetEnvironmentCache()
			hook = new(logtest.Hook)
			log.AddHook(hook)
			logs.Init()
		})

		It("should write error messages to the logs", func() {
			hook.Reset()

			log.Error("Error test")
			Expect(len(hook.Entries)).Should(Equal(int(1)))
			Expect(log.ErrorLevel).Should(Equal(hook.LastEntry().Level))
			Expect("Error test").Should(Equal(hook.LastEntry().Message))

			hook.Reset()
			Expect(hook.LastEntry()).Should(BeNil())
		})

		It("should write warn messages to the logs", func() {
			hook.Reset()

			log.Warn("Warn test")
			Expect(len(hook.Entries)).Should(Equal(int(1)))
			Expect(log.WarnLevel).Should(Equal(hook.LastEntry().Level))
			Expect("Warn test").Should(Equal(hook.LastEntry().Message))

			hook.Reset()
			Expect(hook.LastEntry()).Should(BeNil())
		})

		It("should write info messages to the logs", func() {
			hook.Reset()

			log.Info("Info test")
			Expect(len(hook.Entries)).Should(Equal(int(1)))
			Expect(log.InfoLevel).Should(Equal(hook.LastEntry().Level))
			Expect("Info test").Should(Equal(hook.LastEntry().Message))

			hook.Reset()
			Expect(hook.LastEntry()).Should(BeNil())
		})

		It("should not debug info messages to the logs", func() {
			hook.Reset()

			log.Debug("Debug test")
			Expect(len(hook.Entries)).Should(Equal(int(0)))

			hook.Reset()
			Expect(hook.LastEntry()).Should(BeNil())
		})

		It("should not trace messages to the logs", func() {
			hook.Reset()

			log.Trace("Trace test")
			Expect(len(hook.Entries)).Should(Equal(int(0)))

			hook.Reset()
			Expect(hook.LastEntry()).Should(BeNil())
		})
	})
})
