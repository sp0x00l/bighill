package db_test

import (
	"context"
	"errors"
	"fmt"
	dbConn "lib/shared_lib/db"
	"profile_service/pkg/domain"
	"profile_service/pkg/infra/repo/db"
	"strings"
	"time"

	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type testConnectionPool struct {
	QueryRowCalled     bool
	CloseCalled        bool
	QueryCalled        bool
	QueryRowCalls      []string
	QueryRowArgs       [][]any
	ExecCalled         bool
	ExecCalls          []string
	PoolQueryCalled    bool
	PoolQueryRowCalled bool
	TxQueryCalled      bool
	TxQueryRowCalled   bool
	BeginTxCalled      bool
	CommitCalled       bool
	RollbackCalled     bool
	NextRow            pgx.Row
	NextQueryRow       pgx.Row
	NextRows           pgx.Rows
	NextTxRows         []pgx.Row
	NextError          error
	NextExecErrors     []error
	NextBeginError     error
	NextCommitError    error
	NextRollbackError  error
	RollbackContextErr error
	NextRowsAffected   int64
	ExecCalledCount    int
	LastQuery          string
	LastArgs           []any
}

func (p *testConnectionPool) Close() { p.CloseCalled = true }

type testErrRow struct{ err error }

func (r *testErrRow) Scan(...any) error { return r.err }

func (p *testConnectionPool) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	p.QueryRowCalled = true
	p.PoolQueryRowCalled = true
	p.LastQuery = sql
	p.LastArgs = args
	p.QueryRowCalls = append(p.QueryRowCalls, sql)
	p.QueryRowArgs = append(p.QueryRowArgs, args)
	if p.NextRow != nil {
		return p.NextRow
	}
	if p.NextQueryRow != nil {
		return p.NextQueryRow
	}
	return &testErrRow{err: pgx.ErrNoRows}
}

func (p *testConnectionPool) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	p.QueryCalled = true
	p.PoolQueryCalled = true
	p.LastQuery = sql
	p.LastArgs = args
	return p.NextRows, p.NextError
}

func (p *testConnectionPool) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	p.ExecCalled = true
	p.LastQuery = sql
	p.LastArgs = args
	p.ExecCalledCount++
	p.ExecCalls = append(p.ExecCalls, sql)
	nextErr := p.NextError
	if nextErr == nil && len(p.NextExecErrors) > 0 {
		nextErr = p.NextExecErrors[0]
		p.NextExecErrors = p.NextExecErrors[1:]
	}
	return pgconn.NewCommandTag(fmt.Sprintf("UPDATE %d", p.NextRowsAffected)), nextErr
}

func (p *testConnectionPool) BeginTx(_ context.Context, _ pgx.TxOptions) (pgx.Tx, error) {
	p.BeginTxCalled = true
	if p.NextBeginError != nil {
		return nil, p.NextBeginError
	}
	return &testTx{pool: p}, nil
}

type testTx struct{ pool *testConnectionPool }

func (tx *testTx) LargeObjects() pgx.LargeObjects                         { return pgx.LargeObjects{} }
func (tx *testTx) SendBatch(context.Context, *pgx.Batch) pgx.BatchResults { return nil }
func (tx *testTx) Conn() *pgx.Conn                                        { return nil }
func (tx *testTx) Begin(context.Context) (pgx.Tx, error) {
	if tx.pool.NextBeginError != nil {
		return nil, tx.pool.NextBeginError
	}
	return tx, nil
}
func (tx *testTx) Commit(context.Context) error {
	tx.pool.CommitCalled = true
	return tx.pool.NextCommitError
}
func (tx *testTx) Rollback(ctx context.Context) error {
	tx.pool.RollbackCalled = true
	tx.pool.RollbackContextErr = ctx.Err()
	return tx.pool.NextRollbackError
}
func (tx *testTx) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	tx.pool.ExecCalled = true
	tx.pool.ExecCalls = append(tx.pool.ExecCalls, sql)
	tx.pool.LastQuery = sql
	tx.pool.LastArgs = args
	tx.pool.QueryRowCalls = append(tx.pool.QueryRowCalls, sql)
	tx.pool.QueryRowArgs = append(tx.pool.QueryRowArgs, args)
	tx.pool.ExecCalledCount++
	nextErr := tx.pool.NextError
	if nextErr == nil && len(tx.pool.NextExecErrors) > 0 {
		nextErr = tx.pool.NextExecErrors[0]
		tx.pool.NextExecErrors = tx.pool.NextExecErrors[1:]
	}
	return pgconn.NewCommandTag(fmt.Sprintf("UPDATE %d", tx.pool.NextRowsAffected)), nextErr
}
func (tx *testTx) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, tx.pool.NextError
}
func (tx *testTx) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	tx.pool.QueryCalled = true
	tx.pool.TxQueryCalled = true
	tx.pool.LastQuery = sql
	tx.pool.LastArgs = args
	return tx.pool.NextRows, tx.pool.NextError
}
func (tx *testTx) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	tx.pool.QueryRowCalled = true
	tx.pool.TxQueryRowCalled = true
	tx.pool.LastQuery = sql
	tx.pool.LastArgs = args
	if len(tx.pool.NextTxRows) > 0 {
		nextRow := tx.pool.NextTxRows[0]
		tx.pool.NextTxRows = tx.pool.NextTxRows[1:]
		return nextRow
	}
	return tx.pool.NextRow
}
func (tx *testTx) Prepare(context.Context, string, string) (*pgconn.StatementDescription, error) {
	return nil, nil
}

type mockProfileScan struct {
	ID                         string
	DefaultOrgID               string
	Email                      string
	FirstName                  string
	LastName                   string
	PhoneNumber                string
	DateOfBirth                time.Time
	CountryCode                string
	AddressLine1               string
	AddressLine2               string
	City                       string
	State                      string
	PostalCode                 string
	Country                    string
	HuggingFaceTokenCiphertext string
	PasswordHash               string
	EmailVerified              bool
	Deleted                    bool
}

func (m mockProfileScan) scan(dest ...any) error {
	if len(dest) > 2 {
		if IDPtr, ok := dest[0].(*pgtype.UUID); ok {
			err := IDPtr.Scan(m.ID)
			Expect(err).ShouldNot(HaveOccurred())
		}
		offset := 0
		if defaultOrgIDPtr, ok := dest[1].(*pgtype.UUID); ok {
			offset = 1
			if m.DefaultOrgID != "" {
				err := defaultOrgIDPtr.Scan(m.DefaultOrgID)
				Expect(err).ShouldNot(HaveOccurred())
			}
		}
		if emailPtr, ok := dest[1+offset].(*pgtype.Text); ok {
			err := emailPtr.Scan(m.Email)
			Expect(err).ShouldNot(HaveOccurred())
		}
		if firstnamePtr, ok := dest[2+offset].(*pgtype.Text); ok {
			err := firstnamePtr.Scan(m.FirstName)
			Expect(err).ShouldNot(HaveOccurred())
		}
		if lastnamePtr, ok := dest[3+offset].(*pgtype.Text); ok {
			err := lastnamePtr.Scan(m.LastName)
			Expect(err).ShouldNot(HaveOccurred())
		}
		if phoneNumberPtr, ok := dest[4+offset].(*pgtype.Text); ok {
			err := phoneNumberPtr.Scan(m.PhoneNumber)
			Expect(err).ShouldNot(HaveOccurred())
		}
		if dateOfBirthPtr, ok := dest[5+offset].(*pgtype.Date); ok {
			err := dateOfBirthPtr.Scan(m.DateOfBirth)
			Expect(err).ShouldNot(HaveOccurred())
		}
		if countryCodePtr, ok := dest[6+offset].(*pgtype.Text); ok {
			err := countryCodePtr.Scan(m.CountryCode)
			Expect(err).ShouldNot(HaveOccurred())
		}
		if addressLine1Ptr, ok := dest[7+offset].(*pgtype.Text); ok {
			err := addressLine1Ptr.Scan(m.AddressLine1)
			Expect(err).ShouldNot(HaveOccurred())
		}
		if addressLine2Ptr, ok := dest[8+offset].(*pgtype.Text); ok {
			err := addressLine2Ptr.Scan(m.AddressLine2)
			Expect(err).ShouldNot(HaveOccurred())
		}
		if cityPtr, ok := dest[9+offset].(*pgtype.Text); ok {
			err := cityPtr.Scan(m.City)
			Expect(err).ShouldNot(HaveOccurred())
		}
		if statePtr, ok := dest[10+offset].(*pgtype.Text); ok {
			err := statePtr.Scan(m.State)
			Expect(err).ShouldNot(HaveOccurred())
		}
		if postalCodePtr, ok := dest[11+offset].(*pgtype.Text); ok {
			err := postalCodePtr.Scan(m.PostalCode)
			Expect(err).ShouldNot(HaveOccurred())
		}
		if countryPtr, ok := dest[12+offset].(*pgtype.Text); ok {
			err := countryPtr.Scan(m.Country)
			Expect(err).ShouldNot(HaveOccurred())
		}
		if huggingFaceTokenPtr, ok := dest[13+offset].(*pgtype.Text); ok {
			err := huggingFaceTokenPtr.Scan(m.HuggingFaceTokenCiphertext)
			Expect(err).ShouldNot(HaveOccurred())
		}
		if emailVerifiedPtr, ok := dest[14+offset].(*pgtype.Bool); ok {
			err := emailVerifiedPtr.Scan(m.EmailVerified)
			Expect(err).ShouldNot(HaveOccurred())
		}
	}
	return nil
}

type mockProfileRow struct {
	ScanCalled bool

	NextError error
	NextScan  mockProfileScan
}

func (m *mockProfileRow) Scan(dest ...any) error {
	m.ScanCalled = true

	if m.NextError != nil {
		return m.NextError
	}

	if len(dest) == 1 {
		// dest[0] is the string profile id when saving the record
		if idPtr, ok := dest[0].(*string); ok {
			*idPtr = m.NextScan.ID
		}
	} else if len(dest) == 2 {
		if idPtr, ok := dest[0].(*string); ok {
			*idPtr = m.NextScan.ID
		}
		if passwordHashPtr, ok := dest[1].(*string); ok {
			*passwordHashPtr = m.NextScan.PasswordHash
		}
	} else {
		m.NextScan.scan(dest...)
	}
	return nil
}

type mockProfilePasswordRow struct {
	ScanCalled bool
	NextError  error
	NextScan   mockProfileScan
}

func (m *mockProfilePasswordRow) Scan(dest ...any) error {
	m.ScanCalled = true

	if m.NextError != nil {
		return m.NextError
	}

	if len(dest) == 3 {
		if idPtr, ok := dest[0].(*string); ok {
			*idPtr = m.NextScan.ID
		}
		if passwordHashPtr, ok := dest[1].(*string); ok {
			*passwordHashPtr = m.NextScan.PasswordHash
		}
		if emailVerifiedPtr, ok := dest[2].(*bool); ok {
			*emailVerifiedPtr = m.NextScan.EmailVerified
		}
	}
	return nil
}

func TestProfileDatabaseService(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Profile db unit test suite")
}

var _ = Describe("Profile Database", func() {
	var (
		dbConnMock     *testConnectionPool
		database       db.ProfileDB
		dbCore         *dbConn.Database
		userID         uuid.UUID
		email          string
		idempotencyKey uuid.UUID
		dbName         string
		profile        *domain.Profile
		profileAccount *domain.ProfileAccount
		ctx            context.Context
	)

	BeforeEach(func() {
		userID = uuid.New()
		dbName = "test_db"
		email = "test@example.com"

		dbConnMock = &testConnectionPool{}
		dbCore = dbConn.NewDatabase(dbConnMock, dbName)
		ctx = context.Background()
	})

	Describe("NewProfileDatabase - create a new profile database", func() {
		It("returns a database with the given name and connection pool ", func() {
			database = db.NewProfileDB(dbCore)
			Expect(database).ShouldNot(BeNil())
		})
	})

	Describe("Save - create a new Profile", Ordered, func() {
		BeforeEach(func() {
			idempotencyKey = uuid.New()
			profileAccount = &domain.ProfileAccount{
				ID:          userID,
				Email:       "email",
				PhoneNumber: "1234567890",
				CountryCode: "US",
				Password:    "password",
			}
			database = db.NewProfileDB(dbCore)
			Expect(database).ShouldNot(BeNil())
		})

		When("idempotency is not unique", func() {
			It("returns not unique error", func() {
				dbConnMock.NextRow = &mockProfileRow{
					NextError: &pgconn.PgError{
						Code:           pgerrcode.UniqueViolation,
						ConstraintName: "profiles_idempotency_key_key",
					},
				}

				err := database.Save(ctx, profileAccount, idempotencyKey)
				Expect(err).Should(HaveOccurred())
				Expect(err.Error()).Should(ContainSubstring("idempotency key"))
				Expect(err.Error()).Should(ContainSubstring("already exists"))
				Expect(profileAccount.ID).Should(Equal(uuid.Nil))
			})
		})

		When("fails to execute insert query", func() {
			It("returns error", func() {
				dbConnMock.NextRow = &mockProfileRow{
					NextError: errors.New("failed to execute query"),
				}
				err := database.Save(ctx, profileAccount, idempotencyKey)
				Expect(err).Should(HaveOccurred())
				Expect(err).Should(MatchError("database error. Failed to insert profile: failed to execute query"))
				Expect(profileAccount.ID).Should(Equal(uuid.Nil))
				Expect(dbConnMock.QueryRowCalled).Should(BeTrue())
			})
		})

		When("insert query is successful", func() {
			It("returns Profile ID without error", func() {
				profileAccount.EmailVerifyToken = "token-1"
				profileAccount.EmailVerifyExpiresAt = time.Date(2026, 4, 19, 10, 0, 0, 0, time.UTC)
				mockRow := &mockProfileRow{
					NextScan: mockProfileScan{
						ID: userID.String(),
					},
				}
				dbConnMock.NextRow = mockRow

				err := database.Save(ctx, profileAccount, idempotencyKey)

				Expect(err).ShouldNot(HaveOccurred())
				Expect(profileAccount.ID).Should(Equal(userID))
				Expect(mockRow.ScanCalled).Should(BeTrue())

				Expect(dbConnMock.QueryRowCalled).Should(BeTrue())
				Expect(dbConnMock.QueryRowCalls).To(HaveLen(1))
				insertQuery := `INSERT INTO test_db.profiles (id, idempotency_key, email, phone_number, country_code, password_hash, email_verified, email_verify_token_hash, email_verify_expires_at)VALUES (uuid_generate_v4(), @idempotency_key, @email, @phone_number, @country_code, @password_hash, @email_verified, @email_verify_token_hash, @email_verify_expires_at)RETURNING id;`
				lastQuery := strings.ReplaceAll(dbConnMock.QueryRowCalls[0], "\n\t", "")
				Expect(lastQuery).Should(ContainSubstring(insertQuery))
				Expect(dbConnMock.QueryRowArgs[0][0]).Should(SatisfyAll(
					HaveKeyWithValue("idempotency_key", pgtype.UUID{Bytes: idempotencyKey, Valid: true}),
					HaveKeyWithValue("email", pgtype.Text{String: "email", Valid: true}),
					HaveKeyWithValue("phone_number", pgtype.Text{String: "1234567890", Valid: true}),
					HaveKeyWithValue("country_code", pgtype.Text{String: "US", Valid: true}),
					HaveKey("email_verify_token_hash"),
				))
				tokenHash := dbConnMock.QueryRowArgs[0][0].(pgx.NamedArgs)["email_verify_token_hash"].(pgtype.Text)
				Expect(tokenHash.Valid).To(BeTrue())
				Expect(tokenHash.String).NotTo(Equal("token-1"))
				Expect(dbConnMock.ExecCalls).To(HaveLen(3))
				Expect(dbConnMock.ExecCalls[0]).To(ContainSubstring("INSERT INTO test_db.organizations"))
				Expect(dbConnMock.ExecCalls[1]).To(ContainSubstring("INSERT INTO test_db.organization_memberships"))
				Expect(dbConnMock.ExecCalls[2]).To(ContainSubstring("UPDATE test_db.profiles SET default_org_id"))
				Expect(profileAccount.DefaultOrgID).NotTo(Equal(uuid.Nil))
			})
		})
	})

	Describe("Update - update an existing Profile", func() {
		BeforeEach(func() {
			profile = &domain.Profile{
				ProfileAccount: domain.ProfileAccount{
					ID:          userID,
					Email:       "email",
					PhoneNumber: "1234567890",
					CountryCode: "US",
				},
				FirstName:    "first_name",
				LastName:     "last_name",
				DateOfBirth:  time.Date(1990, 1, 1, 0, 0, 0, 0, time.UTC),
				AddressLine1: "123 Test St",
				AddressLine2: "Apt 4B",
				City:         "Test City",
				State:        "Test State",
				PostalCode:   "10001",
				Country:      "Test Country",
			}

			database = db.NewProfileDB(dbCore)
			Expect(database).ShouldNot(BeNil())
		})

		When("fails to execute update", func() {
			It("returns error", func() {
				rowMock := &mockProfileRow{
					NextError: errors.New("failed to execute query"),
				}
				dbConnMock.NextRow = rowMock

				res, err := database.Update(ctx, userID, profile)
				Expect(res).To(BeNil())
				Expect(err).Should(HaveOccurred())
				Expect(err.Error()).Should(ContainSubstring("database error. Failed to update profile"))
				Expect(err.Error()).Should(ContainSubstring("failed to execute query"))

				Expect(dbConnMock.QueryRowCalled).Should(BeTrue())
				Expect(rowMock.ScanCalled).Should(BeTrue())
			})
		})

		When("no rows are updated", func() {
			It("returns error", func() {
				rowMock := &mockProfileRow{
					NextError: pgx.ErrNoRows,
				}
				dbConnMock.NextRow = rowMock

				res, err := database.Update(ctx, userID, profile)

				Expect(res).To(BeNil())
				Expect(err).Should(HaveOccurred())
				Expect(err.Error()).Should(ContainSubstring("not found"))

				Expect(dbConnMock.QueryRowCalled).Should(BeTrue())
				Expect(rowMock.ScanCalled).Should(BeTrue())
			})
		})

		When("update is successful", func() {
			It("returns the whole profile without error", func() {
				rowMock := &mockProfileRow{
					NextScan: mockProfileScan{
						ID:           userID.String(),
						Email:        "email",
						FirstName:    "first_name",
						LastName:     "last_name",
						PhoneNumber:  "1234567890",
						DateOfBirth:  time.Date(1990, 1, 1, 0, 0, 0, 0, time.UTC),
						CountryCode:  "US",
						AddressLine1: "123 Test St",
						AddressLine2: "Apt 4B",
						City:         "Test City",
						State:        "Test State",
						PostalCode:   "10001",
						Country:      "Test Country",
					},
				}
				dbConnMock.NextRow = rowMock

				res, err := database.Update(ctx, userID, profile)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(dbConnMock.QueryRowCalled).Should(BeTrue())
				Expect(rowMock.ScanCalled).Should(BeTrue())

				updateQuery := `UPDATE test_db.profilesSET email = @email, first_name = @first_name, last_name = @last_name, phone_number = @phone_number,date_of_birth = @date_of_birth, country_code = @country_code, address_line_1 = @address_line_1,address_line_2 = @address_line_2, city = @city, state = @state, postal_code = @postal_code, country = @countryWHERE id = @id AND deleted = false RETURNING id, default_org_id, email, first_name, last_name, phone_number, date_of_birth, country_code,address_line_1, address_line_2, city, state, postal_code, country, huggingface_token_ciphertext, email_verified;`
				lastQuery := strings.ReplaceAll(dbConnMock.LastQuery, "\n\t", "")
				lastQuery = strings.ReplaceAll(lastQuery, "    ", "")
				Expect(lastQuery).Should(Equal(updateQuery))
				Expect(dbConnMock.LastArgs[0]).Should(SatisfyAll(
					HaveKeyWithValue("email", pgtype.Text{String: "email", Valid: true}),
					HaveKeyWithValue("first_name", pgtype.Text{String: "first_name", Valid: true}),
					HaveKeyWithValue("last_name", pgtype.Text{String: "last_name", Valid: true}),
					HaveKeyWithValue("phone_number", pgtype.Text{String: "1234567890", Valid: true}),
					HaveKeyWithValue("date_of_birth", pgtype.Date{Time: time.Date(1990, 1, 1, 0, 0, 0, 0, time.UTC), Valid: true}),
					HaveKeyWithValue("country_code", pgtype.Text{String: "US", Valid: true}),
					HaveKeyWithValue("address_line_1", pgtype.Text{String: "123 Test St", Valid: true}),
					HaveKeyWithValue("address_line_2", pgtype.Text{String: "Apt 4B", Valid: true}),
					HaveKeyWithValue("city", pgtype.Text{String: "Test City", Valid: true}),
					HaveKeyWithValue("state", pgtype.Text{String: "Test State", Valid: true}),
					HaveKeyWithValue("postal_code", pgtype.Text{String: "10001", Valid: true}),
					HaveKeyWithValue("country", pgtype.Text{String: "Test Country", Valid: true}),
				))

				Expect(res).ShouldNot(BeNil())
				Expect(res.ID).Should(Equal(userID))
				Expect(res.Email).Should(Equal("email"))
				Expect(res.FirstName).Should(Equal("first_name"))
				Expect(res.LastName).Should(Equal("last_name"))
				Expect(res.PhoneNumber).Should(Equal("1234567890"))
				Expect(res.DateOfBirth).Should(Equal(time.Date(1990, 1, 1, 0, 0, 0, 0, time.UTC)))
				Expect(res.CountryCode).Should(Equal("US"))
				Expect(res.AddressLine1).Should(Equal("123 Test St"))
				Expect(res.AddressLine2).Should(Equal("Apt 4B"))
				Expect(res.City).Should(Equal("Test City"))
				Expect(res.State).Should(Equal("Test State"))
				Expect(res.PostalCode).Should(Equal("10001"))
				Expect(res.Country).Should(Equal("Test Country"))
			})
		})

		When("update with same email and phone number (no change)", func() {
			It("returns the whole profile without error and without unique constraint violation", func() {
				rowMock := &mockProfileRow{
					NextScan: mockProfileScan{
						ID:           userID.String(),
						Email:        "email",
						FirstName:    "updated_first",
						LastName:     "updated_last",
						PhoneNumber:  "1234567890",
						DateOfBirth:  time.Date(1990, 1, 1, 0, 0, 0, 0, time.UTC),
						CountryCode:  "US",
						AddressLine1: "456 New St",
						AddressLine2: "Suite 5",
						City:         "New City",
						State:        "New State",
						PostalCode:   "20002",
						Country:      "New Country",
					},
				}
				dbConnMock.NextRow = rowMock

				// Update profile with same email/phone but different other fields
				profile.FirstName = "updated_first"
				profile.LastName = "updated_last"
				profile.AddressLine1 = "456 New St"
				profile.AddressLine2 = "Suite 5"
				profile.City = "New City"
				profile.State = "New State"
				profile.PostalCode = "20002"
				profile.Country = "New Country"

				res, err := database.Update(ctx, userID, profile)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(res).ShouldNot(BeNil())
				Expect(res.Email).Should(Equal("email"))
				Expect(res.PhoneNumber).Should(Equal("1234567890"))
				Expect(res.FirstName).Should(Equal("updated_first"))
				Expect(res.LastName).Should(Equal("updated_last"))
				Expect(res.AddressLine1).Should(Equal("456 New St"))
			})
		})
	})

	Describe("UpdateHuggingFaceToken", func() {
		BeforeEach(func() {
			database = db.NewProfileDB(dbCore)
			Expect(database).ShouldNot(BeNil())
		})

		It("updates the encrypted token and returns the profile", func() {
			rowMock := &mockProfileRow{
				NextScan: mockProfileScan{
					ID:                         userID.String(),
					Email:                      "email",
					HuggingFaceTokenCiphertext: "ciphertext-1",
				},
			}
			dbConnMock.NextRow = rowMock

			res, err := database.UpdateHuggingFaceToken(ctx, userID, "ciphertext-1")

			Expect(err).ShouldNot(HaveOccurred())
			Expect(res).ShouldNot(BeNil())
			Expect(res.ID).Should(Equal(userID))
			Expect(res.HuggingFaceTokenCiphertext).Should(Equal("ciphertext-1"))
			Expect(dbConnMock.QueryRowCalled).Should(BeTrue())
			Expect(dbConnMock.LastQuery).Should(ContainSubstring("SET huggingface_token_ciphertext = @huggingface_token_ciphertext"))
			Expect(dbConnMock.LastArgs[0]).Should(SatisfyAll(
				HaveKeyWithValue("id", pgtype.UUID{Bytes: userID, Valid: true}),
				HaveKeyWithValue("huggingface_token_ciphertext", pgtype.Text{String: "ciphertext-1", Valid: true}),
			))
		})

		It("returns not found when no profile is updated", func() {
			rowMock := &mockProfileRow{NextError: pgx.ErrNoRows}
			dbConnMock.NextRow = rowMock

			res, err := database.UpdateHuggingFaceToken(ctx, userID, "ciphertext-1")

			Expect(res).To(BeNil())
			Expect(err).To(MatchError(domain.ErrProfileNotFound))
		})
	})

	Describe("Update password - update the password for a given profile", func() {
		var password string

		BeforeEach(func() {
			password = "new_hashed_password"

			database = db.NewProfileDB(dbCore)
			Expect(database).ShouldNot(BeNil())
		})

		When("fails to execute update", func() {
			It("returns error", func() {
				dbConnMock.NextError = errors.New("failed to execute query")

				err := database.UpdatePassword(ctx, userID, password)
				Expect(err).Should(HaveOccurred())
				Expect(err.Error()).Should(ContainSubstring("failed to execute query"))

				Expect(dbConnMock.ExecCalled).Should(BeTrue())
			})

		})

		When("fails to update the resource", func() {
			It("returns error", func() {
				dbConnMock.NextRowsAffected = 0

				err := database.UpdatePassword(ctx, userID, password)
				Expect(err).Should(HaveOccurred())
				Expect(err.Error()).Should(ContainSubstring("not found"))

				Expect(dbConnMock.ExecCalled).Should(BeTrue())
			})
		})

		When("update is successful", func() {
			It("returns Profile ID without error", func() {
				dbConnMock.NextRowsAffected = 1

				err := database.UpdatePassword(ctx, userID, password)
				Expect(err).ShouldNot(HaveOccurred())

				Expect(dbConnMock.ExecCalled).Should(BeTrue())
				updateQuery := `UPDATE test_db.profilesSET password_hash = @password_hashWHERE id = @id AND deleted = false;`
				lastQuery := strings.ReplaceAll(dbConnMock.LastQuery, "\n\t", "")
				Expect(lastQuery).Should(ContainSubstring(updateQuery))
				Expect(dbConnMock.LastArgs[0]).Should(SatisfyAll(
					HaveKeyWithValue("id", pgtype.UUID{Bytes: userID, Valid: true}),
					HaveKey("password_hash"),
				))
			})
		})
	})

	Describe("Read - retrieve a profile by userID", func() {
		BeforeEach(func() {
			database = db.NewProfileDB(dbCore)
			Expect(database).ShouldNot(BeNil())
		})

		When("query execution fails", func() {
			It("returns error", func() {
				rowMock := &mockProfileRow{
					NextError: errors.New("failed to execute query"),
				}
				dbConnMock.NextRow = rowMock

				res, err := database.Read(ctx, userID)
				Expect(err).Should(HaveOccurred())
				Expect(err.Error()).Should(ContainSubstring("failed to execute query"))
				Expect(res).Should(BeNil())
				Expect(dbConnMock.QueryRowCalled).Should(BeTrue())
				Expect(rowMock.ScanCalled).Should(BeTrue())
			})
		})

		When("no rows are retrieved", func() {
			It("returns error", func() {
				rowMock := &mockProfileRow{
					NextError: pgx.ErrNoRows,
				}
				dbConnMock.NextRow = rowMock

				res, err := database.Read(ctx, userID)
				Expect(err).Should(HaveOccurred())
				Expect(err.Error()).Should(ContainSubstring("not found"))
				Expect(res).Should(BeNil())
				Expect(dbConnMock.QueryRowCalled).Should(BeTrue())
				Expect(rowMock.ScanCalled).Should(BeTrue())
			})
		})

		When("rows are retrieved", func() {
			It("returns a single profile", func() {
				rowMock := &mockProfileRow{
					NextScan: mockProfileScan{
						ID:           userID.String(),
						Email:        "email",
						FirstName:    "fistname",
						LastName:     "lastname",
						PhoneNumber:  "1234567890",
						DateOfBirth:  time.Date(1990, 1, 1, 0, 0, 0, 0, time.UTC),
						CountryCode:  "US",
						AddressLine1: "123 Test St",
						AddressLine2: "Apt 4B",
						City:         "Test City",
						State:        "Test State",
						PostalCode:   "10001",
						Country:      "Test Country",
					},
				}
				dbConnMock.NextRow = rowMock

				res, err := database.Read(ctx, userID)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(res).ShouldNot(BeNil())
				Expect(res.ID).Should(Equal(userID))
				Expect(res.Email).Should(Equal("email"))
				Expect(res.FirstName).Should(Equal("fistname"))
				Expect(res.LastName).Should(Equal("lastname"))
				Expect(res.PhoneNumber).Should(Equal("1234567890"))
				Expect(res.DateOfBirth).Should(Equal(time.Date(1990, 1, 1, 0, 0, 0, 0, time.UTC)))
				Expect(res.CountryCode).Should(Equal("US"))
				Expect(res.AddressLine1).Should(Equal("123 Test St"))
				Expect(res.AddressLine2).Should(Equal("Apt 4B"))
				Expect(res.City).Should(Equal("Test City"))
				Expect(res.State).Should(Equal("Test State"))
				Expect(res.PostalCode).Should(Equal("10001"))
				Expect(res.Country).Should(Equal("Test Country"))

				expectedQuery := "\n\tSELECT id, default_org_id, email, first_name, last_name, phone_number,\n\tdate_of_birth, country_code, address_line_1, address_line_2, city, state,\n\tpostal_code, country, huggingface_token_ciphertext, email_verified\n\tFROM test_db.profiles WHERE id = @id AND deleted = false;"
				Expect(dbConnMock.LastQuery).Should(Equal(expectedQuery))
				Expect(dbConnMock.LastArgs[0]).Should(Equal(
					pgx.NamedArgs{
						"id": pgtype.UUID{Bytes: userID, Valid: true},
					}))
			})
		})
	})

	Describe("Read - retrieve a password hash by userID", func() {
		BeforeEach(func() {
			database = db.NewProfileDB(dbCore)
			Expect(database).ShouldNot(BeNil())
		})

		When("query execution fails", func() {
			It("returns error", func() {
				rowMock := &mockProfileRow{
					NextError: errors.New("failed to execute query"),
				}
				dbConnMock.NextRow = rowMock

				userID, password, err := database.ReadPasswordHash(ctx, email)
				Expect(err).Should(HaveOccurred())
				Expect(err.Error()).Should(ContainSubstring("failed to execute query"))
				Expect(userID).Should(Equal(uuid.Nil))
				Expect(password).Should(BeEmpty())
				Expect(dbConnMock.QueryRowCalled).Should(BeTrue())
				Expect(rowMock.ScanCalled).Should(BeTrue())
			})
		})

		When("no rows are retrieved", func() {
			It("returns error", func() {
				rowMock := &mockProfilePasswordRow{
					NextError: pgx.ErrNoRows,
				}
				dbConnMock.NextRow = rowMock

				userID, password, err := database.ReadPasswordHash(ctx, email)
				Expect(err).Should(HaveOccurred())
				Expect(err.Error()).Should(ContainSubstring("not found"))
				Expect(userID).Should(Equal(uuid.Nil))
				Expect(password).Should(BeEmpty())
				Expect(dbConnMock.QueryRowCalled).Should(BeTrue())
				Expect(rowMock.ScanCalled).Should(BeTrue())
			})
		})

		When("rows are retrieved", func() {
			It("returns a single password hash", func() {
				rowMock := &mockProfilePasswordRow{
					NextScan: mockProfileScan{
						ID:            userID.String(),
						PasswordHash:  "hashed_password",
						EmailVerified: true,
					},
				}
				dbConnMock.NextRow = rowMock

				userID, password, err := database.ReadPasswordHash(ctx, email)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(userID).Should(Equal(userID))
				Expect(password).Should(Equal("hashed_password"))

				expectedQuery := "\n\tSELECT id, password_hash, email_verified\n\tFROM test_db.profiles WHERE email = @email AND deleted = false;"
				Expect(dbConnMock.LastQuery).Should(Equal(expectedQuery))
				Expect(dbConnMock.LastArgs[0]).Should(Equal(pgx.NamedArgs{
					"email": pgtype.Text{String: email, Valid: true},
				}))
			})

			It("returns an email not verified error", func() {
				rowMock := &mockProfilePasswordRow{
					NextScan: mockProfileScan{
						ID:            userID.String(),
						PasswordHash:  "hashed_password",
						EmailVerified: false,
					},
				}
				dbConnMock.NextRow = rowMock

				returnedUserID, password, err := database.ReadPasswordHash(ctx, email)
				Expect(err).Should(HaveOccurred())
				Expect(err).To(MatchError(ContainSubstring("email not verified")))
				Expect(errors.Is(err, db.ErrEmailNotVerified)).To(BeTrue())
				Expect(returnedUserID).Should(Equal(uuid.Nil))
				Expect(password).Should(BeEmpty())
			})
		})
	})

	Describe("Delete - delete a profile by userID", func() {
		BeforeEach(func() {
			database = db.NewProfileDB(dbCore)
			Expect(database).ShouldNot(BeNil())
		})

		When("query execution fails", func() {
			It("returns error", func() {
				dbConnMock.NextError = errors.New("failed to execute query")

				err := database.Delete(ctx, userID)
				Expect(err).Should(HaveOccurred())
				Expect(err.Error()).Should(ContainSubstring("failed to execute query"))
				Expect(dbConnMock.ExecCalled).Should(BeTrue())
			})
		})

		When("no rows are deleted", func() {
			It("returns error", func() {
				dbConnMock.NextRowsAffected = 0

				err := database.Delete(ctx, userID)
				Expect(err).Should(HaveOccurred())
				Expect(err.Error()).Should(ContainSubstring("not found"))
				Expect(dbConnMock.ExecCalled).Should(BeTrue())
			})
		})

		When("delete is successful", func() {
			It("returns no error", func() {
				dbConnMock.NextRowsAffected = 1

				err := database.Delete(ctx, userID)
				Expect(err).ShouldNot(HaveOccurred())

				Expect(dbConnMock.ExecCalled).Should(BeTrue())
				deleteQuery := `UPDATE test_db.profilesSET deleted = trueWHERE id = @id AND deleted = false;`
				lastQuery := strings.ReplaceAll(dbConnMock.LastQuery, "\n\t", "")
				Expect(lastQuery).Should(Equal(deleteQuery))
				Expect(dbConnMock.LastArgs[0]).Should(HaveKeyWithValue("id", pgtype.UUID{Bytes: userID, Valid: true}))
			})
		})
	})
})
