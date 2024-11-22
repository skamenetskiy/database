package database

import (
	"context"
	"database/sql"
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
)

// Connect to postgres database.
func Connect(ctx context.Context, dsn string) (Database, error) {
	var (
		db  = new(database)
		err error
	)

	db.pool, err = pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}

	db.sql = stdlib.OpenDBFromPool(db.pool)
	db.sqlx = sqlx.NewDb(db.sql, "pgx")

	return db, nil
}

type Database interface {
	standard
	P() *pgxpool.Pool
	Close() error
}

type standard interface {
	S() *sql.DB
	X() *sqlx.DB
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

func (d *database) Close() error {
	return d.sqlx.Close()
}

var (
	errTxStart    = errors.New("start transaction")
	errTxRollback = errors.New("rollback transaction")
	errTxCommit   = errors.New("commit transaction")
)
