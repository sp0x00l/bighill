package database

import (
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
)

const pgForeignKeyViolationCode = "23503"

func IsForeignKeyViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgForeignKeyViolationCode
}
