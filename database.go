package database

import (
	"context"
	"database/sql"
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jmoiron/sqlx"
)

// Database interface.
type Database interface {
	// S returns standard sql.DB interface.
	S() *sql.DB

	// X returns sqlx.DB wrapper.
	X() *sqlx.DB

	// P returns pgx connections pool.
	P() *pgxpool.Pool

	// InTx is a helper function for simple transactions.
	InTx(ctx context.Context, f func(context.Context, *sqlx.Tx) error) error

	// Close database connection.
	Close()
}

type database struct {
	ctx  context.Context
	sql  *sql.DB
	sqlx *sqlx.DB
	pool *pgxpool.Pool
}

func (d *database) P() *pgxpool.Pool {
	return d.pool
}

func (d *database) S() *sql.DB {
	return d.sql
}

func (d *database) X() *sqlx.DB {
	return d.sqlx
}

func (d *database) InTx(ctx context.Context, f func(context.Context, *sqlx.Tx) error) error {
	tx, err := d.sqlx.BeginTxx(ctx, &sql.TxOptions{
		Isolation: sql.LevelDefault,
	})
	if err != nil {
		return errors.Join(err, errTxStart)
	}

	if err = f(ctx, tx); err != nil {
		if err1 := tx.Rollback(); err1 != nil {
			return errors.Join(err1, errTxRollback)
		}
		return err
	}

	if err = tx.Commit(); err != nil {
		return errors.Join(err, errTxCommit)
	}
	return nil
}

func (d *database) Close() {
	d.pool.Close()
}

var (
	errTxStart    = errors.New("start transaction")
	errTxRollback = errors.New("rollback transaction")
	errTxCommit   = errors.New("commit transaction")
)
