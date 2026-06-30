package db_test

import (
	"context"
	"testing"

	featuredb "feature_materializer_service/pkg/infra/repo/db"
	coreDB "lib/shared_lib/db"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestSnapshotRepository(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Feature materializer db unit test suite")
}

type connectionPoolStub struct{}

func (p connectionPoolStub) Close() {}

func (p connectionPoolStub) QueryRow(context.Context, string, ...any) pgx.Row {
	return nil
}

func (p connectionPoolStub) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, nil
}

func (p connectionPoolStub) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}

func (p connectionPoolStub) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) {
	return nil, nil
}

var _ = Describe("SnapshotRepository", func() {
	It("wraps the shared database with the configured schema name", func() {
		database := coreDB.NewDatabase(connectionPoolStub{}, "bighill_feature_materializer_db")

		repository := featuredb.NewSnapshotRepository(database)

		Expect(repository.Name).To(Equal("bighill_feature_materializer_db"))
	})
})
