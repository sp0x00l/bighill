package messaging_test

import (
	"context"

	"lib/shared_lib/messaging"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type postgresOutboxPoolStub struct {
	tx *postgresOutboxTxStub
}

func (p postgresOutboxPoolStub) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) {
	return p.tx, nil
}

type postgresOutboxTxStub struct {
	execSQL  string
	execArgs []any
	commits  int
}

func (t *postgresOutboxTxStub) Begin(context.Context) (pgx.Tx, error) {
	return t, nil
}

func (t *postgresOutboxTxStub) Commit(context.Context) error {
	t.commits++
	return nil
}

func (t *postgresOutboxTxStub) Rollback(context.Context) error {
	return nil
}

func (t *postgresOutboxTxStub) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, nil
}

func (t *postgresOutboxTxStub) SendBatch(context.Context, *pgx.Batch) pgx.BatchResults {
	return nil
}

func (t *postgresOutboxTxStub) LargeObjects() pgx.LargeObjects {
	return pgx.LargeObjects{}
}

func (t *postgresOutboxTxStub) Prepare(context.Context, string, string) (*pgconn.StatementDescription, error) {
	return nil, nil
}

func (t *postgresOutboxTxStub) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	t.execSQL = sql
	t.execArgs = args
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}

func (t *postgresOutboxTxStub) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, nil
}

func (t *postgresOutboxTxStub) QueryRow(context.Context, string, ...any) pgx.Row {
	return nil
}

func (t *postgresOutboxTxStub) Conn() *pgx.Conn {
	return nil
}

var _ = Describe("PostgresOutbox", func() {
	It("enqueues outbound messages with the caller transaction", func() {
		tx := &postgresOutboxTxStub{}
		outboxWriter, err := messaging.NewPostgresOutbox(postgresOutboxPoolStub{tx: tx}, "service_db", "")
		Expect(err).NotTo(HaveOccurred())
		outbox := outboxWriter.(messaging.OrderedOutbox)

		err = outbox.EnqueueTx(context.Background(), tx, messaging.OutboundMessage{
			Topic: "feature_materializer",
			Message: messaging.Message{
				ResourceKey: uuid.MustParse("11111111-1111-1111-1111-111111111111"),
				MsgType:     messaging.MsgTypeRawSnapshotReady,
				Payload:     []byte("payload"),
			},
			Headers:     []kafka.Header{{Key: "traceparent", Value: []byte("trace")}},
			DispatchKey: "raw_snapshot_ready:22222222-2222-2222-2222-222222222222",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(tx.execSQL).To(ContainSubstring("INSERT INTO service_db.outbox_messages"))
		Expect(tx.execSQL).To(ContainSubstring("ON CONFLICT (dispatch_key) DO NOTHING"))
		Expect(tx.execArgs).To(HaveLen(1))
		args := tx.execArgs[0].(pgx.NamedArgs)
		Expect(args["dispatch_key"]).To(Equal("raw_snapshot_ready:22222222-2222-2222-2222-222222222222"))
		Expect(args["topic"]).To(Equal("feature_materializer"))
		Expect(args["event_type"]).To(Equal("raw_snapshot_ready"))
	})

	It("rejects unsafe schema names", func() {
		_, err := messaging.NewPostgresOutbox(postgresOutboxPoolStub{tx: &postgresOutboxTxStub{}}, "service-db", "")

		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("invalid postgres outbox schema"))
	})
})
