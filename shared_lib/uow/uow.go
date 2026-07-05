package uow

import (
	"context"

	coreDB "lib/shared_lib/db"
	msgConn "lib/shared_lib/messaging"

	"github.com/jackc/pgx/v5"
	log "github.com/sirupsen/logrus"
)

type OutboundMessage = msgConn.OutboundMessage

type EnqueueFunc func(OutboundMessage) error

type TxFunc func(ctx context.Context, tx pgx.Tx, enqueue EnqueueFunc) error

type UnitOfWork struct {
	runner *coreDB.UnitOfWork
	outbox msgConn.OrderedOutbox
	signal func()
}

type Option func(*UnitOfWork)

func WithTransactionalOutbox(outbox msgConn.OrderedOutbox) Option {
	log.Trace("uow WithTransactionalOutbox")

	return func(work *UnitOfWork) {
		work.outbox = outbox
	}
}

func WithOutboxSignal(signal func()) Option {
	log.Trace("uow WithOutboxSignal")

	return func(work *UnitOfWork) {
		work.signal = signal
	}
}

func New(pool coreDB.ConnectionPool, opts ...Option) *UnitOfWork {
	log.Trace("uow New")

	work := &UnitOfWork{
		runner: coreDB.NewUnitOfWork(pool),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(work)
		}
	}
	return work
}

func (u *UnitOfWork) Do(ctx context.Context, fn TxFunc) error {
	log.Trace("uow Do")

	enqueued := false
	err := u.runner.Do(ctx, func(ctx context.Context, tx pgx.Tx) error {
		return fn(ctx, tx, func(message msgConn.OutboundMessage) error {
			if err := u.outbox.EnqueueTx(ctx, tx, message); err != nil {
				return err
			}
			enqueued = true
			return nil
		})
	})
	if err == nil && enqueued && u.signal != nil {
		u.signal()
	}
	return err
}
