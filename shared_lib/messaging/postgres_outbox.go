package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode"

	metrics "lib/shared_lib/metrics"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	log "github.com/sirupsen/logrus"
)

const defaultPostgresOutboxTable = "outbox_messages"

type postgresOutboxPool interface {
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
}

type postgresOutbox struct {
	pool          postgresOutboxPool
	schema        string
	table         string
	relayOwnerID  string
	relayLeaseTTL time.Duration
}

type outboxHeader struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

var _ Outbox = (*postgresOutbox)(nil)
var _ OrderedOutbox = (*postgresOutbox)(nil)
var _ RelayOutbox = (*postgresOutbox)(nil)
var _ OutboxWriter = (*postgresOutbox)(nil)

func NewPostgresOutbox(pool postgresOutboxPool, schema string, table string) (OutboxWriter, error) {
	log.Trace("NewPostgresOutbox")

	if pool == nil {
		return nil, fmt.Errorf("postgres outbox requires a pool")
	}
	if !isSafeSQLIdentifier(schema) {
		return nil, fmt.Errorf("invalid postgres outbox schema %q", schema)
	}
	if strings.TrimSpace(table) == "" {
		table = defaultPostgresOutboxTable
	}
	if !isSafeSQLIdentifier(table) {
		return nil, fmt.Errorf("invalid postgres outbox table %q", table)
	}
	return &postgresOutbox{
		pool:          pool,
		schema:        schema,
		table:         table,
		relayLeaseTTL: 30 * time.Second,
	}, nil
}

func MustNewPostgresOutbox(pool postgresOutboxPool, schema string, table string) OutboxWriter {
	log.Trace("MustNewPostgresOutbox")

	outbox, err := NewPostgresOutbox(pool, schema, table)
	if err != nil {
		log.Fatalf("create postgres outbox: %v", err)
	}
	return outbox
}

func (p *postgresOutbox) ConfigureRelayIdentity(ownerID string, leaseDuration time.Duration) {
	log.Trace("postgresOutbox ConfigureRelayIdentity")

	if strings.TrimSpace(ownerID) != "" {
		p.relayOwnerID = ownerID
	}
	if leaseDuration > 0 {
		p.relayLeaseTTL = leaseDuration
	}
}

func (p *postgresOutbox) WriteMessage(ctx context.Context, message OutboxMessage) error {
	log.Trace("postgresOutbox WriteMessage")

	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin postgres outbox transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	if err := p.writeMessageTx(ctx, tx, message); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit postgres outbox transaction: %w", err)
	}
	return nil
}

func (p *postgresOutbox) Enqueue(ctx context.Context, msg OutboundMessage) error {
	log.Trace("postgresOutbox Enqueue")

	outboxMessage, err := outboxMessageFromOutbound(ctx, msg)
	if err != nil {
		return err
	}
	return p.WriteMessage(ctx, outboxMessage)
}

func (p *postgresOutbox) EnqueueTx(ctx context.Context, tx pgx.Tx, msg OutboundMessage) error {
	log.Trace("postgresOutbox EnqueueTx")

	if tx == nil {
		return fmt.Errorf("postgres outbox transaction is required")
	}
	outboxMessage, err := outboxMessageFromOutbound(ctx, msg)
	if err != nil {
		return err
	}
	return p.writeMessageTx(ctx, tx, outboxMessage)
}

func (p *postgresOutbox) writeMessageTx(ctx context.Context, tx pgx.Tx, message OutboxMessage) error {
	log.Trace("postgresOutbox writeMessageTx")

	payload := message.Payload
	if payload == nil {
		payload = []byte{}
	}
	headers, err := marshalHeaders(message.Headers)
	if err != nil {
		return err
	}
	dispatchKey := strings.TrimSpace(message.DispatchKey)
	if dispatchKey == "" {
		dispatchKey = deriveOutboxEventID(message.Topic, message.Message, payload, time.Now().UTC().Format(time.RFC3339Nano))
	}

	query := `INSERT INTO ` + p.qualifiedTable() + ` (
		dispatch_key, topic, event_type, resource_key, payload, headers, status, attempts, next_attempt_at
	) VALUES (
		@dispatch_key, @topic, @event_type, @resource_key, @payload, @headers::jsonb, 'PENDING', 0, now()
	)
	ON CONFLICT (dispatch_key) DO NOTHING`
	_, err = tx.Exec(ctx, query, pgx.NamedArgs{
		"dispatch_key": dispatchKey,
		"topic":        message.Topic,
		"event_type":   message.Message.MsgType.String(),
		"resource_key": pgtype.UUID{Bytes: message.Message.ResourceKey, Valid: true},
		"payload":      payload,
		"headers":      string(headers),
	})
	if err != nil {
		metrics.Default().RecordError(ctx, metrics.BoundaryDB, "outbox_write", metrics.ClassifyDB(err), "")
		return fmt.Errorf("write postgres outbox message: %w", err)
	}
	return nil
}

func (p *postgresOutbox) ReadPending(ctx context.Context, maxMessages int32) ([]OutboxPendingMessage, error) {
	log.Trace("postgresOutbox ReadPending")

	if maxMessages <= 0 {
		return []OutboxPendingMessage{}, nil
	}
	ownerID := p.relayOwnerID
	if ownerID == "" {
		ownerID = uuid.NewString()
		p.relayOwnerID = ownerID
	}
	leaseTTL := p.relayLeaseTTL
	if leaseTTL <= 0 {
		leaseTTL = 30 * time.Second
	}
	claimToken := uuid.NewString()
	leaseExpiresAt := time.Now().UTC().Add(leaseTTL)

	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		metrics.Default().RecordError(ctx, metrics.BoundaryDB, "outbox_read_pending", metrics.ClassifyDB(err), "")
		return nil, fmt.Errorf("begin postgres outbox read transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	query := `WITH candidate AS (
		SELECT outbox_id
		FROM ` + p.qualifiedTable() + `
		WHERE (status = 'PENDING' AND next_attempt_at <= now())
		   OR (status = 'PROCESSING' AND lease_expires_at <= now())
		ORDER BY created_at
		LIMIT @limit
		FOR UPDATE SKIP LOCKED
	)
	UPDATE ` + p.qualifiedTable() + ` outbox
	SET status = 'PROCESSING',
		processing_owner = @owner,
		claim_token = @claim_token,
		lease_expires_at = @lease_expires_at,
		updated_at = now()
	FROM candidate
	WHERE outbox.outbox_id = candidate.outbox_id
	RETURNING outbox.outbox_id::text, outbox.dispatch_key, outbox.topic, outbox.payload, outbox.headers::text, outbox.attempts`
	rows, err := tx.Query(ctx, query, pgx.NamedArgs{
		"limit":            maxMessages,
		"owner":            ownerID,
		"claim_token":      claimToken,
		"lease_expires_at": leaseExpiresAt,
	})
	if err != nil {
		metrics.Default().RecordError(ctx, metrics.BoundaryDB, "outbox_read_pending", metrics.ClassifyDB(err), "")
		return nil, fmt.Errorf("read pending postgres outbox messages: %w", err)
	}
	defer rows.Close()

	pending := make([]OutboxPendingMessage, 0, maxMessages)
	for rows.Next() {
		var outboxID, dispatchKey, topic, headersRaw string
		var payload []byte
		var attempts int
		if err := rows.Scan(&outboxID, &dispatchKey, &topic, &payload, &headersRaw, &attempts); err != nil {
			return nil, fmt.Errorf("scan postgres outbox message: %w", err)
		}
		headers, err := unmarshalHeaders([]byte(headersRaw))
		if err != nil {
			return nil, err
		}
		pending = append(pending, OutboxPendingMessage{
			PK:              outboxID,
			SK:              dispatchKey,
			Topic:           topic,
			Payload:         payload,
			Headers:         headers,
			Attempts:        attempts,
			ProcessingOwner: ownerID,
			ClaimToken:      claimToken,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate postgres outbox messages: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit postgres outbox read transaction: %w", err)
	}
	return pending, nil
}

func (p *postgresOutbox) MarkSent(ctx context.Context, pending OutboxPendingMessage) error {
	log.Trace("postgresOutbox MarkSent")

	outboxID, err := uuid.Parse(pending.PK)
	if err != nil {
		return fmt.Errorf("invalid postgres outbox id %q: %w", pending.PK, err)
	}
	query := `UPDATE ` + p.qualifiedTable() + `
		SET status = 'SENT',
			sent_at = now(),
			updated_at = now()
		WHERE outbox_id = @outbox_id
		  AND status = 'PROCESSING'
		  AND processing_owner = @owner
		  AND claim_token = @claim_token`
	tag, err := p.exec(ctx, query, pgx.NamedArgs{
		"outbox_id":   pgtype.UUID{Bytes: outboxID, Valid: true},
		"owner":       pending.ProcessingOwner,
		"claim_token": pending.ClaimToken,
	})
	if err != nil {
		metrics.Default().RecordError(ctx, metrics.BoundaryDB, "outbox_mark_sent", metrics.ClassifyDB(err), "")
		return fmt.Errorf("mark postgres outbox sent: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("postgres outbox message was not claimed by this relay: %s", pending.PK)
	}
	return nil
}

func (p *postgresOutbox) MarkFailed(ctx context.Context, pending OutboxPendingMessage, lastError string, nextAttemptAt time.Time) error {
	log.Trace("postgresOutbox MarkFailed")

	outboxID, err := uuid.Parse(pending.PK)
	if err != nil {
		return fmt.Errorf("invalid postgres outbox id %q: %w", pending.PK, err)
	}
	query := `UPDATE ` + p.qualifiedTable() + `
		SET status = 'PENDING',
			attempts = attempts + 1,
			last_error = @last_error,
			next_attempt_at = @next_attempt_at,
			updated_at = now()
		WHERE outbox_id = @outbox_id
		  AND status = 'PROCESSING'
		  AND processing_owner = @owner
		  AND claim_token = @claim_token`
	tag, err := p.exec(ctx, query, pgx.NamedArgs{
		"outbox_id":       pgtype.UUID{Bytes: outboxID, Valid: true},
		"owner":           pending.ProcessingOwner,
		"claim_token":     pending.ClaimToken,
		"last_error":      lastError,
		"next_attempt_at": nextAttemptAt.UTC(),
	})
	if err != nil {
		metrics.Default().RecordError(ctx, metrics.BoundaryDB, "outbox_mark_failed", metrics.ClassifyDB(err), "")
		return fmt.Errorf("mark postgres outbox failed: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("postgres outbox message was not claimed by this relay: %s", pending.PK)
	}
	return nil
}

func (p *postgresOutbox) exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	log.Trace("postgresOutbox exec")

	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return pgconn.CommandTag{}, err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, sql, args...)
	if err != nil {
		return tag, err
	}
	if err := tx.Commit(ctx); err != nil {
		return tag, err
	}
	return tag, nil
}

func (p *postgresOutbox) qualifiedTable() string {
	return p.schema + "." + p.table
}

func marshalHeaders(headers []kafka.Header) ([]byte, error) {
	log.Trace("marshalHeaders")

	out := make([]outboxHeader, 0, len(headers))
	for _, header := range headers {
		out = append(out, outboxHeader{Key: header.Key, Value: header.Value})
	}
	return json.Marshal(out)
}

func unmarshalHeaders(data []byte) ([]kafka.Header, error) {
	log.Trace("unmarshalHeaders")

	var in []outboxHeader
	if len(data) > 0 {
		if err := json.Unmarshal(data, &in); err != nil {
			return nil, fmt.Errorf("unmarshal postgres outbox headers: %w", err)
		}
	}
	headers := make([]kafka.Header, 0, len(in))
	for _, header := range in {
		headers = append(headers, kafka.Header{Key: header.Key, Value: header.Value})
	}
	return headers, nil
}

func isSafeSQLIdentifier(name string) bool {
	if name == "" {
		return false
	}
	for i, r := range name {
		if i == 0 {
			if r != '_' && !unicode.IsLetter(r) {
				return false
			}
			continue
		}
		if r != '_' && !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			return false
		}
	}
	return true
}
