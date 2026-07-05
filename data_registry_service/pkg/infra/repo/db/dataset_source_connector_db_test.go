package db

import (
	"context"
	"errors"
	"fmt"

	domainErrors "data_registry_service/pkg/domain"
	"data_registry_service/pkg/domain/model"
	"lib/shared_lib/ctxutil"
	coreDB "lib/shared_lib/db"
	msgConn "lib/shared_lib/messaging"
	core "lib/shared_lib/transport"

	"github.com/google/uuid"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("DatasetDB", func() {
	var (
		ctx     context.Context
		userID  uuid.UUID
		pool    *repositoryPoolStub
		repo    *datasetDB
		dataset *model.Dataset
	)

	BeforeEach(func() {
		userID = uuid.New()
		ctx = ctxutil.WithTenantID(context.Background(), userID)
		pool = &repositoryPoolStub{}
		repo = NewDatasetDB(coreDB.NewDatabase(pool, "test_schema"))
		dataset = validDatasetDomain(uuid.New(), userID)
	})

	It("creates a dataset in a transaction and emits the outbox message", func() {
		outbox := &orderedOutboxStub{}
		signaled := false
		repo = NewDatasetDB(
			coreDB.NewDatabase(pool, "test_schema"),
			WithTransactionalOutbox(outbox, "data_registry"),
			WithOutboxSignal(func() { signaled = true }),
		)
		pool.txRows = []pgx.Row{stringScanRow(nil,
			dataset.ID.String(),
			userID.String(),
			model.Standard.String(),
			model.Draft.String(),
			model.DatasetProcessingPending.String(),
		)}
		idempotencyKey := uuid.New()

		err := repo.Create(ctx, dataset, idempotencyKey)

		Expect(err).NotTo(HaveOccurred())
		Expect(pool.beginTxCalled).To(BeTrue())
		Expect(pool.commitCalled).To(BeTrue())
		Expect(outbox.messages).To(HaveLen(1))
		Expect(outbox.messages[0].Topic).To(Equal("data_registry"))
		Expect(outbox.messages[0].Message.ResourceKey).To(Equal(dataset.ID))
		Expect(signaled).To(BeTrue())
		Expect(pool.lastQuery).To(ContainSubstring("INSERT INTO test_schema.datasets"))
		args := pool.lastArgs[0].(pgx.NamedArgs)
		Expect(args["user_id"]).To(Equal(pgtype.UUID{Bytes: userID, Valid: true}))
		Expect(args["idempotency_key"]).To(Equal(pgtype.UUID{Bytes: idempotencyKey, Valid: true}))
	})

	It("maps duplicate dataset creation to the domain conflict error", func() {
		pool.txRows = []pgx.Row{errorRowStub{err: &pgconn.PgError{Code: pgerrcode.UniqueViolation}}}

		err := repo.Create(ctx, dataset, uuid.New())

		Expect(errors.Is(err, domainErrors.ErrResourceAlreadyExists)).To(BeTrue())
		Expect(pool.commitCalled).To(BeFalse())
	})

	It("maps tenant projection failures to validation errors", func() {
		pool.txRows = []pgx.Row{errorRowStub{err: &pgconn.PgError{Code: pgerrcode.ForeignKeyViolation}}}

		err := repo.Create(ctx, dataset, uuid.New())

		Expect(errors.Is(err, domainErrors.ErrValidationFailed)).To(BeTrue())
		Expect(err.Error()).To(ContainSubstring("tenant projection is not ready"))
		Expect(pool.commitCalled).To(BeFalse())
	})

	It("reads datasets using the tenant-scoped filters", func() {
		pool.poolRows = []pgx.Row{intScanRow(1, nil)}
		pool.queryRows = []pgx.Rows{newRepositoryRowsStub(&datasetRowStub{dao: validDatasetDAO(dataset.ID, userID)})}

		got, count, err := repo.Read(ctx, userID, *core.NewPagination(1, 10), []model.Filter{
			model.CategoryFilter{Values: []string{"rag"}},
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(count).To(Equal(1))
		Expect(got).To(HaveLen(1))
		Expect(got[0].ID).To(Equal(dataset.ID))
		Expect(pool.queryCalled).To(BeTrue())
		Expect(pool.lastQuery).To(ContainSubstring("user_id = @user_id"))
		Expect(pool.lastQuery).To(ContainSubstring("category IN (@value_0)"))
		args := pool.lastArgs[0].(pgx.NamedArgs)
		Expect(args["user_id"]).To(Equal(userID))
		Expect(args["value_0"]).To(Equal("rag"))
	})

	It("returns resource-not-found when a read has no rows", func() {
		pool.poolRows = []pgx.Row{intScanRow(0, nil)}

		got, count, err := repo.Read(ctx, userID, *core.NewPagination(1, 10), nil)

		Expect(got).To(BeNil())
		Expect(count).To(Equal(0))
		Expect(errors.Is(err, domainErrors.ErrResourceNotFound)).To(BeTrue())
		Expect(pool.queryCalled).To(BeFalse())
	})

	It("rejects unsupported filters before querying", func() {
		got, count, err := repo.Read(ctx, userID, *core.NewPagination(1, 10), []model.Filter{unsupportedDatasetFilter{}})

		Expect(got).To(BeNil())
		Expect(count).To(Equal(0))
		Expect(errors.Is(err, domainErrors.ErrValidationFailed)).To(BeTrue())
		Expect(pool.queryRowCalled).To(BeFalse())
		Expect(pool.queryCalled).To(BeFalse())
	})

	It("reads a dataset by id with user scoping", func() {
		pool.poolRows = []pgx.Row{&datasetRowStub{dao: validDatasetDAO(dataset.ID, userID)}}

		got, err := repo.ReadByID(ctx, dataset.ID, userID)

		Expect(err).NotTo(HaveOccurred())
		Expect(got.ID).To(Equal(dataset.ID))
		Expect(pool.lastQuery).To(ContainSubstring("id = @id AND user_id = @user_id"))
		args := pool.lastArgs[0].(pgx.NamedArgs)
		Expect(args["id"]).To(Equal(dataset.ID))
		Expect(args["user_id"]).To(Equal(userID))
	})

	It("returns resource-not-found when reading a missing dataset by id", func() {
		pool.poolRows = []pgx.Row{errorRowStub{err: pgx.ErrNoRows}}

		got, err := repo.ReadByID(ctx, dataset.ID, userID)

		Expect(got).To(BeNil())
		Expect(errors.Is(err, domainErrors.ErrResourceNotFound)).To(BeTrue())
	})

	It("replaces a dataset and emits an update", func() {
		outbox := &orderedOutboxStub{}
		repo = NewDatasetDB(
			coreDB.NewDatabase(pool, "test_schema"),
			WithTransactionalOutbox(outbox, "data_registry"),
		)
		updatedDAO := validDatasetDAO(dataset.ID, userID)
		updatedDAO.Title = pgtype.Text{String: "Updated", Valid: true}
		pool.txRows = []pgx.Row{&datasetRowStub{dao: updatedDAO}}

		got, err := repo.Replace(ctx, dataset)

		Expect(err).NotTo(HaveOccurred())
		Expect(got.Title).To(Equal("Updated"))
		Expect(pool.commitCalled).To(BeTrue())
		Expect(outbox.messages).To(HaveLen(1))
		Expect(outbox.messages[0].Message.MsgType).To(Equal(msgConn.MsgTypeDatasetUpdated))
	})

	It("returns resource-not-found when replacing a missing dataset", func() {
		pool.txRows = []pgx.Row{errorRowStub{err: pgx.ErrNoRows}}

		got, err := repo.Replace(ctx, dataset)

		Expect(got).To(BeNil())
		Expect(errors.Is(err, domainErrors.ErrResourceNotFound)).To(BeTrue())
		Expect(pool.commitCalled).To(BeFalse())
	})

	It("deletes a dataset and emits a delete event", func() {
		outbox := &orderedOutboxStub{}
		repo = NewDatasetDB(
			coreDB.NewDatabase(pool, "test_schema"),
			WithTransactionalOutbox(outbox, "data_registry"),
		)
		pool.execRowsAffected = []int64{1}

		err := repo.Delete(ctx, dataset.ID, userID)

		Expect(err).NotTo(HaveOccurred())
		Expect(pool.commitCalled).To(BeTrue())
		Expect(outbox.messages).To(HaveLen(1))
		Expect(outbox.messages[0].Message.MsgType).To(Equal(msgConn.MsgTypeDatasetDeleted))
		Expect(pool.lastQuery).To(ContainSubstring("user_id = @user_id"))
	})

	It("returns resource-not-found when deleting a missing dataset", func() {
		pool.execRowsAffected = []int64{0}

		err := repo.Delete(ctx, dataset.ID, userID)

		Expect(errors.Is(err, domainErrors.ErrResourceNotFound)).To(BeTrue())
		Expect(pool.commitCalled).To(BeFalse())
	})

	It("updates processing state only when the state advances", func() {
		updatedDAO := validDatasetDAO(dataset.ID, userID)
		updatedDAO.ProcessingState = pgtype.Text{String: model.DatasetProcessingFeatureMaterialized.String(), Valid: true}
		pool.txRows = []pgx.Row{&datasetRowStub{dao: updatedDAO}}

		got, changed, err := repo.UpdateProcessingState(ctx, dataset.ID, userID, model.DatasetProcessingFeatureMaterialized)

		Expect(err).NotTo(HaveOccurred())
		Expect(changed).To(BeTrue())
		Expect(got.ProcessingState).To(Equal(model.DatasetProcessingFeatureMaterialized))
		Expect(pool.commitCalled).To(BeTrue())
	})

	It("returns the current dataset when a processing state update is unchanged", func() {
		currentDAO := validDatasetDAO(dataset.ID, userID)
		currentDAO.ProcessingState = pgtype.Text{String: model.DatasetProcessingEmbeddingsMaterialized.String(), Valid: true}
		pool.txRows = []pgx.Row{errorRowStub{err: pgx.ErrNoRows}}
		pool.poolRows = []pgx.Row{&datasetRowStub{dao: currentDAO}}

		got, changed, err := repo.UpdateProcessingState(ctx, dataset.ID, userID, model.DatasetProcessingRawMaterialized)

		Expect(err).NotTo(HaveOccurred())
		Expect(changed).To(BeFalse())
		Expect(got.ProcessingState).To(Equal(model.DatasetProcessingEmbeddingsMaterialized))
		Expect(pool.commitCalled).To(BeFalse())
		Expect(pool.rollbackCalled).To(BeTrue())
	})

	It("records materialization metadata and emits an update when values change", func() {
		outbox := &orderedOutboxStub{}
		repo = NewDatasetDB(
			coreDB.NewDatabase(pool, "test_schema"),
			WithTransactionalOutbox(outbox, "data_registry"),
		)
		rawSnapshotID := uuid.New()
		materialized := validDatasetDomain(dataset.ID, userID)
		materialized.Location = "s3://bucket/raw/movies.parquet"
		materialized.RawSnapshotID = rawSnapshotID
		updatedDAO := validDatasetDAO(dataset.ID, userID)
		updatedDAO.Location = pgtype.Text{String: materialized.Location, Valid: true}
		updatedDAO.RawSnapshotID = pgtype.UUID{Bytes: rawSnapshotID, Valid: true}
		pool.txRows = []pgx.Row{&datasetRowStub{dao: updatedDAO}}

		got, err := repo.RecordMaterialization(ctx, materialized, model.DatasetProcessingRawMaterialized)

		Expect(err).NotTo(HaveOccurred())
		Expect(got.RawSnapshotID).To(Equal(rawSnapshotID))
		Expect(outbox.messages).To(HaveLen(1))
		Expect(pool.commitCalled).To(BeTrue())
	})

	It("keeps materialization table fields unset when no table metadata is present", func() {
		args := (&Dataset{}).toDAO(validDatasetDomain(dataset.ID, userID))
		materialized := validDatasetDomain(dataset.ID, userID)
		materialized.Location = ""
		materialized.TableNamespace = ""
		materialized.TableName = ""
		materialized.RawSnapshotID = uuid.Nil

		applyMaterializationOptionalFields(args, materialized)

		Expect(args["table_format"]).To(Equal(pgtype.Text{String: "", Valid: true}))
		Expect(args["catalog_provider"]).To(Equal(pgtype.Text{String: "", Valid: true}))
		Expect(args["processing_profile"]).To(Equal(pgtype.Text{String: "", Valid: true}))
		Expect(hasTableMaterializationMetadata(materialized)).To(BeFalse())
	})

	It("publishes a draft dataset", func() {
		pool.execRowsAffected = []int64{1}

		err := repo.UpdatePublishedState(ctx, dataset.ID, userID)

		Expect(err).NotTo(HaveOccurred())
		Expect(pool.execCalled).To(BeTrue())
		Expect(pool.lastQuery).To(ContainSubstring("status = 'published'"))
		args := pool.lastArgs[0].(pgx.NamedArgs)
		Expect(args["id"]).To(Equal(dataset.ID))
		Expect(args["user_id"]).To(Equal(userID))
	})

	It("returns resource-not-found when publishing a non-draft or missing dataset", func() {
		pool.execRowsAffected = []int64{0}

		err := repo.UpdatePublishedState(ctx, dataset.ID, userID)

		Expect(errors.Is(err, domainErrors.ErrResourceNotFound)).To(BeTrue())
	})

	It("surfaces row iteration errors when scanning dataset rows", func() {
		rows := newRepositoryRowsStub(&datasetRowStub{dao: validDatasetDAO(dataset.ID, userID)})
		rows.err = errors.New("cursor failed")

		got, err := repo.scanRows(ctx, rows)

		Expect(got).To(BeNil())
		Expect(err).To(MatchError(ContainSubstring("database error. Failed to iterate datasets")))
	})
})

var _ = Describe("SourceConnectorDB", func() {
	var (
		ctx       context.Context
		userID    uuid.UUID
		pool      *repositoryPoolStub
		repo      *sourceConnectorDB
		connector *model.SourceConnector
	)

	BeforeEach(func() {
		userID = uuid.New()
		ctx = ctxutil.WithTenantID(context.Background(), userID)
		pool = &repositoryPoolStub{}
		repo = NewSourceConnectorDB(coreDB.NewDatabase(pool, "test_schema"))
		connector = validSourceConnectorDomain(uuid.New(), userID)
	})

	It("creates a source connector with tenant-scoped arguments", func() {
		pool.execRowsAffected = []int64{1}
		idempotencyKey := uuid.New()

		err := repo.Create(ctx, connector, idempotencyKey)

		Expect(err).NotTo(HaveOccurred())
		Expect(pool.execCalled).To(BeTrue())
		Expect(pool.lastQuery).To(ContainSubstring("INSERT INTO test_schema.connectors"))
		args := pool.lastArgs[0].(pgx.NamedArgs)
		Expect(args["user_id"]).To(Equal(pgtype.UUID{Bytes: userID, Valid: true}))
		Expect(args["idempotency_key"]).To(Equal(pgtype.UUID{Bytes: idempotencyKey, Valid: true}))
	})

	It("maps duplicate connector creation to the domain conflict error", func() {
		pool.execErrors = []error{&pgconn.PgError{Code: pgerrcode.UniqueViolation}}

		err := repo.Create(ctx, connector, uuid.New())

		Expect(errors.Is(err, domainErrors.ErrResourceAlreadyExists)).To(BeTrue())
	})

	It("maps connector tenant projection failures to validation errors", func() {
		pool.execErrors = []error{&pgconn.PgError{Code: pgerrcode.ForeignKeyViolation}}

		err := repo.Create(ctx, connector, uuid.New())

		Expect(errors.Is(err, domainErrors.ErrValidationFailed)).To(BeTrue())
		Expect(err.Error()).To(ContainSubstring("tenant projection is not ready"))
	})

	It("reads source connectors for a user", func() {
		dao := validSourceConnectorDAO(connector.ID, userID, connector.CatalogID)
		pool.queryRows = []pgx.Rows{newRepositoryRowsStub(&sourceConnectorRowStub{dao: dao})}

		got, err := repo.ReadByUserID(ctx, userID)

		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(HaveLen(1))
		Expect(got[0].ID).To(Equal(connector.ID))
		Expect(got[0].UserID).To(Equal(userID))
		Expect(pool.lastQuery).To(ContainSubstring("WHERE user_id = @user_id"))
		args := pool.lastArgs[0].(pgx.NamedArgs)
		Expect(args["user_id"]).To(Equal(userID))
	})

	It("returns resource-not-found when a user has no source connectors", func() {
		pool.queryRows = []pgx.Rows{newRepositoryRowsStub()}

		got, err := repo.ReadByUserID(ctx, userID)

		Expect(got).To(BeNil())
		Expect(errors.Is(err, domainErrors.ErrResourceNotFound)).To(BeTrue())
	})

	It("surfaces connector row iteration errors", func() {
		rows := newRepositoryRowsStub(&sourceConnectorRowStub{dao: validSourceConnectorDAO(connector.ID, userID, connector.CatalogID)})
		rows.err = errors.New("cursor failed")
		pool.queryRows = []pgx.Rows{rows}

		got, err := repo.ReadByUserID(ctx, userID)

		Expect(got).To(BeNil())
		Expect(err).To(MatchError(ContainSubstring("database error. Failed to iterate connectors")))
	})

	It("reads a source connector by id", func() {
		pool.poolRows = []pgx.Row{&sourceConnectorRowStub{dao: validSourceConnectorDAO(connector.ID, userID, connector.CatalogID)}}

		got, err := repo.ReadByID(ctx, connector.ID, userID)

		Expect(err).NotTo(HaveOccurred())
		Expect(got.ID).To(Equal(connector.ID))
		Expect(got.CatalogID).To(Equal(connector.CatalogID))
		Expect(pool.lastQuery).To(ContainSubstring("id = @id AND user_id = @user_id"))
	})

	It("returns resource-not-found when reading a missing connector by id", func() {
		pool.poolRows = []pgx.Row{errorRowStub{err: pgx.ErrNoRows}}

		got, err := repo.ReadByID(ctx, connector.ID, userID)

		Expect(got).To(BeNil())
		Expect(errors.Is(err, domainErrors.ErrResourceNotFound)).To(BeTrue())
	})

	It("reads a source connector catalog id", func() {
		pool.poolRows = []pgx.Row{uuidScanRow(connector.CatalogID, nil)}

		got, err := repo.ReadCatalogID(ctx, connector.ID, userID)

		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(connector.CatalogID))
		Expect(pool.lastQuery).To(ContainSubstring("SELECT catalog_id"))
	})

	It("returns resource-not-found when reading a missing catalog id", func() {
		pool.poolRows = []pgx.Row{errorRowStub{err: pgx.ErrNoRows}}

		got, err := repo.ReadCatalogID(ctx, connector.ID, userID)

		Expect(got).To(Equal(uuid.Nil))
		Expect(errors.Is(err, domainErrors.ErrResourceNotFound)).To(BeTrue())
	})

	It("deletes a source connector", func() {
		pool.execRowsAffected = []int64{1}

		err := repo.Delete(ctx, connector.ID, userID)

		Expect(err).NotTo(HaveOccurred())
		Expect(pool.execCalled).To(BeTrue())
		Expect(pool.lastQuery).To(ContainSubstring("SET deleted = true"))
	})

	It("returns resource-not-found when deleting a missing source connector", func() {
		pool.execRowsAffected = []int64{0}

		err := repo.Delete(ctx, connector.ID, userID)

		Expect(errors.Is(err, domainErrors.ErrResourceNotFound)).To(BeTrue())
	})

	It("replaces a source connector", func() {
		pool.execRowsAffected = []int64{1}

		err := repo.Replace(ctx, connector)

		Expect(err).NotTo(HaveOccurred())
		Expect(pool.execCalled).To(BeTrue())
		Expect(pool.lastQuery).To(ContainSubstring("UPDATE test_schema.connectors"))
		args := pool.lastArgs[0].(pgx.NamedArgs)
		Expect(args["user_id"]).To(Equal(pgtype.UUID{Bytes: userID, Valid: true}))
	})

	It("returns resource-not-found when replacing a missing source connector", func() {
		pool.execRowsAffected = []int64{0}

		err := repo.Replace(ctx, connector)

		Expect(errors.Is(err, domainErrors.ErrResourceNotFound)).To(BeTrue())
	})
})

type repositoryPoolStub struct {
	poolRows         []pgx.Row
	txRows           []pgx.Row
	queryRows        []pgx.Rows
	queryErrors      []error
	execErrors       []error
	execRowsAffected []int64
	beginErr         error
	commitErr        error
	rollbackErr      error
	closed           bool
	queryRowCalled   bool
	queryCalled      bool
	execCalled       bool
	beginTxCalled    bool
	commitCalled     bool
	rollbackCalled   bool
	lastQuery        string
	lastArgs         []any
}

func (p *repositoryPoolStub) Close() {
	p.closed = true
}

func (p *repositoryPoolStub) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	p.queryRowCalled = true
	p.lastQuery = sql
	p.lastArgs = args
	return p.popPoolRow()
}

func (p *repositoryPoolStub) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	p.queryCalled = true
	p.lastQuery = sql
	p.lastArgs = args
	var err error
	if len(p.queryErrors) > 0 {
		err = p.queryErrors[0]
		p.queryErrors = p.queryErrors[1:]
	}
	if len(p.queryRows) > 0 {
		rows := p.queryRows[0]
		p.queryRows = p.queryRows[1:]
		return rows, err
	}
	return newRepositoryRowsStub(), err
}

func (p *repositoryPoolStub) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	p.execCalled = true
	p.lastQuery = sql
	p.lastArgs = args
	return p.nextCommandTag(), p.popExecError()
}

func (p *repositoryPoolStub) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) {
	p.beginTxCalled = true
	if p.beginErr != nil {
		return nil, p.beginErr
	}
	return &repositoryTxStub{pool: p}, nil
}

func (p *repositoryPoolStub) popPoolRow() pgx.Row {
	if len(p.poolRows) == 0 {
		return errorRowStub{err: pgx.ErrNoRows}
	}
	row := p.poolRows[0]
	p.poolRows = p.poolRows[1:]
	return row
}

func (p *repositoryPoolStub) popTxRow() pgx.Row {
	if len(p.txRows) == 0 {
		return errorRowStub{err: pgx.ErrNoRows}
	}
	row := p.txRows[0]
	p.txRows = p.txRows[1:]
	return row
}

func (p *repositoryPoolStub) popExecError() error {
	if len(p.execErrors) == 0 {
		return nil
	}
	err := p.execErrors[0]
	p.execErrors = p.execErrors[1:]
	return err
}

func (p *repositoryPoolStub) nextCommandTag() pgconn.CommandTag {
	if len(p.execRowsAffected) == 0 {
		return pgconn.NewCommandTag("UPDATE 0")
	}
	rowsAffected := p.execRowsAffected[0]
	p.execRowsAffected = p.execRowsAffected[1:]
	return pgconn.NewCommandTag(fmt.Sprintf("UPDATE %d", rowsAffected))
}

type repositoryTxStub struct {
	pool *repositoryPoolStub
}

func (tx *repositoryTxStub) Begin(context.Context) (pgx.Tx, error) {
	return tx, nil
}

func (tx *repositoryTxStub) Commit(context.Context) error {
	tx.pool.commitCalled = true
	return tx.pool.commitErr
}

func (tx *repositoryTxStub) Rollback(context.Context) error {
	tx.pool.rollbackCalled = true
	return tx.pool.rollbackErr
}

func (tx *repositoryTxStub) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, nil
}

func (tx *repositoryTxStub) SendBatch(context.Context, *pgx.Batch) pgx.BatchResults {
	return nil
}

func (tx *repositoryTxStub) LargeObjects() pgx.LargeObjects {
	return pgx.LargeObjects{}
}

func (tx *repositoryTxStub) Prepare(context.Context, string, string) (*pgconn.StatementDescription, error) {
	return nil, nil
}

func (tx *repositoryTxStub) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	tx.pool.execCalled = true
	tx.pool.lastQuery = sql
	tx.pool.lastArgs = args
	return tx.pool.nextCommandTag(), tx.pool.popExecError()
}

func (tx *repositoryTxStub) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	tx.pool.queryCalled = true
	tx.pool.lastQuery = sql
	tx.pool.lastArgs = args
	return newRepositoryRowsStub(), nil
}

func (tx *repositoryTxStub) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	tx.pool.queryRowCalled = true
	tx.pool.lastQuery = sql
	tx.pool.lastArgs = args
	return tx.pool.popTxRow()
}

func (tx *repositoryTxStub) Conn() *pgx.Conn {
	return nil
}

type orderedOutboxStub struct {
	messages []msgConn.OutboundMessage
	err      error
}

func (o *orderedOutboxStub) EnqueueTx(_ context.Context, _ pgx.Tx, msg msgConn.OutboundMessage) error {
	if o.err != nil {
		return o.err
	}
	o.messages = append(o.messages, msg)
	return nil
}

type errorRowStub struct {
	err error
}

func (r errorRowStub) Scan(...any) error {
	return r.err
}

type scanFuncRow func(dest ...any) error

func (r scanFuncRow) Scan(dest ...any) error {
	return r(dest...)
}

func intScanRow(value int, err error) pgx.Row {
	return scanFuncRow(func(dest ...any) error {
		if err != nil {
			return err
		}
		*(dest[0].(*int)) = value
		return nil
	})
}

func stringScanRow(err error, values ...string) pgx.Row {
	return scanFuncRow(func(dest ...any) error {
		if err != nil {
			return err
		}
		for i := range dest {
			*(dest[i].(*string)) = values[i]
		}
		return nil
	})
}

func uuidScanRow(value uuid.UUID, err error) pgx.Row {
	return scanFuncRow(func(dest ...any) error {
		if err != nil {
			return err
		}
		*(dest[0].(*pgtype.UUID)) = pgtype.UUID{Bytes: value, Valid: true}
		return nil
	})
}

type repositoryRowsStub struct {
	rows   []datasetScanner
	index  int
	err    error
	closed bool
}

func newRepositoryRowsStub(rows ...datasetScanner) *repositoryRowsStub {
	return &repositoryRowsStub{rows: rows, index: -1}
}

func (r *repositoryRowsStub) Close() {
	r.closed = true
}

func (r *repositoryRowsStub) Err() error {
	return r.err
}

func (r *repositoryRowsStub) CommandTag() pgconn.CommandTag {
	return pgconn.NewCommandTag("SELECT 1")
}

func (r *repositoryRowsStub) FieldDescriptions() []pgconn.FieldDescription {
	return nil
}

func (r *repositoryRowsStub) Next() bool {
	if r.index+1 >= len(r.rows) {
		return false
	}
	r.index++
	return true
}

func (r *repositoryRowsStub) Scan(dest ...any) error {
	if r.index < 0 || r.index >= len(r.rows) {
		return fmt.Errorf("scan called without a current row")
	}
	return r.rows[r.index].Scan(dest...)
}

func (r *repositoryRowsStub) Values() ([]any, error) {
	return nil, nil
}

func (r *repositoryRowsStub) RawValues() [][]byte {
	return nil
}

func (r *repositoryRowsStub) Conn() *pgx.Conn {
	return nil
}

type sourceConnectorRowStub struct {
	dao SourceConnectorDAO
	err error
}

func (r *sourceConnectorRowStub) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	*(dest[0].(*pgtype.UUID)) = r.dao.ID
	*(dest[1].(*pgtype.UUID)) = r.dao.UserID
	if len(dest) == 5 {
		*(dest[2].(*pgtype.UUID)) = r.dao.CatalogID
		*(dest[3].(*pgtype.Text)) = r.dao.StorageType
		*(dest[4].(*[]byte)) = r.dao.Config
		return nil
	}
	*(dest[2].(*pgtype.Text)) = r.dao.StorageType
	*(dest[3].(*[]byte)) = r.dao.Config
	return nil
}

type unsupportedDatasetFilter struct{}

func (unsupportedDatasetFilter) GetType() model.FilterBy {
	return model.FilterByInvalid
}

func (unsupportedDatasetFilter) GetFilterAndFillArguments(string, map[string]any) string {
	return ""
}

func validDatasetDomain(datasetID, userID uuid.UUID) *model.Dataset {
	return &model.Dataset{
		ID:                datasetID,
		UserID:            userID,
		Title:             "Movies",
		Description:       "Movie rows",
		Origin:            model.Standard,
		Location:          "s3://bucket/raw/movies.parquet",
		Status:            model.Draft,
		Category:          "rag",
		TableNamespace:    "features",
		TableName:         "movies",
		TableFormat:       model.Parquet,
		CatalogProvider:   model.LocalCatalog,
		ProcessingProfile: model.TextRAGProfile,
		SchemaVersion:     1,
		SchemaMetadata:    "{}",
		ProcessingState:   model.DatasetProcessingPending,
		DatasetVersion:    1,
	}
}

func validSourceConnectorDomain(connectorID, userID uuid.UUID) *model.SourceConnector {
	return &model.SourceConnector{
		ID:        connectorID,
		UserID:    userID,
		CatalogID: uuid.New(),
		Config: &model.PostgresDBConnCfg{
			Hostname:           "localhost",
			Port:               5432,
			DatabaseName:       "mlops",
			Username:           "postgres",
			Password:           "password",
			AuthenticationType: model.Master,
		},
	}
}

func validSourceConnectorDAO(connectorID, userID, catalogID uuid.UUID) SourceConnectorDAO {
	return SourceConnectorDAO{
		ID:          pgtype.UUID{Bytes: connectorID, Valid: true},
		UserID:      pgtype.UUID{Bytes: userID, Valid: true},
		CatalogID:   pgtype.UUID{Bytes: catalogID, Valid: true},
		StorageType: pgtype.Text{String: model.Postgres.String(), Valid: true},
		Config:      []byte(`{"Hostname":"localhost","Port":5432,"DatabaseName":"mlops","Username":"postgres","Password":"password","AuthenticationType":1}`),
	}
}
