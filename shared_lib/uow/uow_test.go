package uow_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	coreDB "lib/shared_lib/db"
	msgConn "lib/shared_lib/messaging"
	"lib/shared_lib/uow"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestUnitOfWork(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Shared unit-of-work test suite")
}

var _ = Describe("UnitOfWork", func() {
	It("enqueues outbox messages inside the transaction and signals after commit", func() {
		pool := &uowPoolStub{}
		outbox := &uowOutboxStub{}
		signals := 0
		work := uow.New(pool, uow.WithTransactionalOutbox(outbox), uow.WithOutboxSignal(func() { signals++ }))
		message := validOutboundMessage()

		err := work.Do(context.Background(), func(ctx context.Context, tx pgx.Tx, enqueue uow.EnqueueFunc) error {
			return enqueue(message)
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(pool.commitCalled).To(BeTrue())
		Expect(outbox.messages).To(Equal([]msgConn.OutboundMessage{message}))
		Expect(outbox.tx).NotTo(BeNil())
		Expect(signals).To(Equal(1))
	})

	It("does not signal when the transaction callback fails", func() {
		pool := &uowPoolStub{}
		outbox := &uowOutboxStub{}
		signals := 0
		work := uow.New(pool, uow.WithTransactionalOutbox(outbox), uow.WithOutboxSignal(func() { signals++ }))
		expectedErr := errors.New("operation failed")

		err := work.Do(context.Background(), func(context.Context, pgx.Tx, uow.EnqueueFunc) error {
			return expectedErr
		})

		Expect(err).To(MatchError(expectedErr))
		Expect(pool.rollbackCalled).To(BeTrue())
		Expect(pool.commitCalled).To(BeFalse())
		Expect(outbox.messages).To(BeEmpty())
		Expect(signals).To(Equal(0))
	})

	It("rolls back and returns outbox enqueue errors", func() {
		pool := &uowPoolStub{}
		expectedErr := errors.New("enqueue failed")
		outbox := &uowOutboxStub{err: expectedErr}
		signals := 0
		work := uow.New(pool, uow.WithTransactionalOutbox(outbox), uow.WithOutboxSignal(func() { signals++ }))

		err := work.Do(context.Background(), func(ctx context.Context, tx pgx.Tx, enqueue uow.EnqueueFunc) error {
			return enqueue(validOutboundMessage())
		})

		Expect(err).To(MatchError(expectedErr))
		Expect(pool.rollbackCalled).To(BeTrue())
		Expect(pool.commitCalled).To(BeFalse())
		Expect(signals).To(Equal(0))
	})
})

type uowPoolStub struct {
	commitCalled   bool
	rollbackCalled bool
}

func (p *uowPoolStub) Close() {}

func (p *uowPoolStub) QueryRow(context.Context, string, ...any) pgx.Row {
	return uowErrorRow{err: pgx.ErrNoRows}
}

func (p *uowPoolStub) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, nil
}

func (p *uowPoolStub) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.NewCommandTag("UPDATE 0"), nil
}

func (p *uowPoolStub) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) {
	return &uowTxStub{pool: p}, nil
}

type uowTxStub struct {
	pool *uowPoolStub
}

func (tx *uowTxStub) Begin(context.Context) (pgx.Tx, error) {
	return tx, nil
}

func (tx *uowTxStub) Commit(context.Context) error {
	tx.pool.commitCalled = true
	return nil
}

func (tx *uowTxStub) Rollback(context.Context) error {
	tx.pool.rollbackCalled = true
	return nil
}

func (tx *uowTxStub) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, nil
}

func (tx *uowTxStub) SendBatch(context.Context, *pgx.Batch) pgx.BatchResults {
	return nil
}

func (tx *uowTxStub) LargeObjects() pgx.LargeObjects {
	return pgx.LargeObjects{}
}

func (tx *uowTxStub) Prepare(context.Context, string, string) (*pgconn.StatementDescription, error) {
	return nil, nil
}

func (tx *uowTxStub) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.NewCommandTag("UPDATE 0"), nil
}

func (tx *uowTxStub) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, nil
}

func (tx *uowTxStub) QueryRow(context.Context, string, ...any) pgx.Row {
	return uowErrorRow{err: pgx.ErrNoRows}
}

func (tx *uowTxStub) Conn() *pgx.Conn {
	return nil
}

type uowErrorRow struct {
	err error
}

func (r uowErrorRow) Scan(...any) error {
	return r.err
}

type uowOutboxStub struct {
	tx       pgx.Tx
	messages []msgConn.OutboundMessage
	err      error
}

func (o *uowOutboxStub) EnqueueTx(_ context.Context, tx pgx.Tx, msg msgConn.OutboundMessage) error {
	if o.err != nil {
		return o.err
	}
	o.tx = tx
	o.messages = append(o.messages, msg)
	return nil
}

func validOutboundMessage() msgConn.OutboundMessage {
	resourceID := uuid.New()
	return msgConn.OutboundMessage{
		Topic: "data_registry",
		Message: msgConn.Message{
			ResourceKey: resourceID,
			MsgType:     msgConn.MsgTypeDatasetCreated,
			Payload:     []byte("payload"),
		},
		DispatchKey: fmt.Sprintf("dataset_created:%s:1", resourceID),
	}
}

var _ coreDB.ConnectionPool = (*uowPoolStub)(nil)
