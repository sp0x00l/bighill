package database

import (
	"errors"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

const pgForeignKeyViolationCode = "23503"
const pgInsufficientPrivilegeCode = "42501"

func IsForeignKeyViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgForeignKeyViolationCode
}

func IsRowLevelSecurityViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) &&
		pgErr.Code == pgInsufficientPrivilegeCode &&
		strings.Contains(strings.ToLower(pgErr.Message), "row-level security")
}
