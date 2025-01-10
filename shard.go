package database

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jmoiron/sqlx"
)

// Shard interface.
type Shard interface {
	Database

	// ID of the shard.
	ID() uint16

	// Writable returns true if the shard is writable.
	Writable() bool

	// initialize shard.
	initialize(tables []string) error
}

type shard struct {
	id       uint16
	writable bool
	conn     *database
}

func (s *shard) ID() uint16 {
	return s.id
}

func (s *shard) Writable() bool {
	return s.writable
}

func (s *shard) S() *sql.DB {
	return s.conn.S()
}

func (s *shard) X() *sqlx.DB {
	return s.conn.X()
}

func (s *shard) P() *pgxpool.Pool {
	return s.conn.P()
}

func (s *shard) InTx(ctx context.Context, f func(context.Context, *sqlx.Tx) error) error {
	return s.conn.InTx(ctx, f)
}

func (s *shard) Close() {
	s.conn.Close()
}

func (s *shard) initialize(tables []string) error {
	var err error
	if err = s.createSequences(tables); err != nil {
		return err
	}
	if err = s.createGenerator(); err != nil {
		return err
	}
	return nil
}

func (s *shard) createSequences(tables []string) error {
	const query = `CREATE SEQUENCE IF NOT EXISTS "%s_id_seq"`
	for _, table := range tables {
		_, err := s.S().Exec(fmt.Sprintf(query, table))
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *shard) createGenerator() error {
	const query = `
		CREATE OR REPLACE FUNCTION next_id(tbl text, tableschema text = 'public') returns bigint AS $$
		DECLARE
			our_epoch bigint := 1314220021721;
			seq_id bigint;
			now_millis bigint;
			shard_id int := %shard_id%;
			result bigint;
		BEGIN
			SELECT nextval(tableschema||'."' || tbl || '_id_seq"') % 1024 INTO seq_id;
			SELECT FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000) INTO now_millis;
			result := (now_millis - our_epoch) << 23;
			result := result | (shard_id << 10);
			result := result | (seq_id);
			RETURN result;
		END;
		$$ LANGUAGE PLPGSQL;
	`
	_, err := s.S().Exec(strings.ReplaceAll(query, "%shard_id%", strconv.FormatUint(uint64(s.id), 10)))
	if err != nil {
		return err
	}
	return nil
}

func newShard(id uint16, writable bool, conn *database) *shard {
	return &shard{id, writable, conn}
}
