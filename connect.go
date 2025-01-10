package database

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
)

// Connect to a single postgres database.
func Connect(ctx context.Context, dsn string) (Database, error) {
	return connect(ctx, dsn)
}

func connect(ctx context.Context, dsn string) (*database, error) {
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
