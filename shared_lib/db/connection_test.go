package database_test

import (
	"context"
	"os"
	"testing"

	db "lib/shared_lib/db"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
)

func TestInfraDbConnection(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "database unit test suite")
}

var _ = Describe("infrastructure database opentelemetry trace", func() {
	var (
		logs *logrus.Logger
		hook *logtest.Hook
	)

	BeforeEach(func() {
		os.Setenv("LOG_LEVEL", "DEBUG")
		os.Setenv("DB_LOG_LEVEL", "DEBUG")
		hook = new(logtest.Hook)
		logs = logrus.New()
		logs.AddHook(hook)
	})

	AfterEach(func() {
		os.Unsetenv("LOG_LEVEL")
		os.Unsetenv("DB_LOG_LEVEL")
	})

	Describe("Creating a new connection", func() {
		Context("when the connection string is valid", func() {
			It("should add tracing", func() {
				ctx := context.Background()

				connectionString := "postgres://test:test@localhost:5432/test?sslmode=disable&pool_max_conns=60"
				conn, err := db.NewPgxConnection(ctx, "test", connectionString, logs)
				Expect(err).Should(HaveOccurred())
				Expect(conn).Should(BeNil())

				// ping fails
				Expect(len(hook.Entries)).Should(BeNumerically(">", 1))
				Expect(logrus.ErrorLevel).Should(Equal(hook.LastEntry().Level))
				Expect("Connect").Should(Equal(hook.AllEntries()[0].Message))

				hook.Reset()
			})
		})
	})
})
