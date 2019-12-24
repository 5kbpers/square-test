package test

import (
	"context"
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
)

var (
	CreateCustomersSQL = "CREATE TABLE IF NOT EXISTS customers (" +
		"id BIGINT NOT NULL AUTO_INCREMENT," +
		"created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP," +
		"updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP," +
		"name TEXT NOT NULL," +
		"PRIMARY KEY (id)" +
		");"

	CreateMovementsSQL = "CREATE TABLE IF NOT EXISTS movements (" +
		"id BIGINT NOT NULL AUTO_INCREMENT," +
		"created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP," +
		"updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP," +
		"payment_id BIGINT NOT NULL," +
		"customer_id BIGINT NOT NULL," +
		"counterparty_id BIGINT NOT NULL," +
		"role VARCHAR(20) NOT NULL," +
		"amount_cents BIGINT NOT NULL," +
		"PRIMARY KEY (id)," +
		"INDEX idx_customer_id (customer_id)," +
		"INDEX idx_created_at (role, created_at)" +
		");"
)

var (
	InsertCustomerSQL = "INSERT INTO customers (id, name) VALUES (?, ?);"
	InsertMovementSQL = "INSERT INTO movements (role, customer_id, counterparty_id, payment_id, amount_cents) VALUES (?, ?, ?, ?, ?);"

	QueryCustomerMovementsSQL = "SELECT * FROM movements WHERE customer_id = ? ORDER BY created_at desc LIMIT 100;"
	QueryAllMovementsSQL      = "SELECT * FROM movements WHERE role = 'SENDER' ORDER BY created_at desc LIMIT 100;"
)

type DB struct {
	db *sql.DB
}

type Conn struct {
	stmtCache map[string]*sql.Stmt
	conn      *sql.Conn
	db        *sql.DB
}

func NewDB(dsn string, concurrency int) (*DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxIdleConns(concurrency + 1)
	db.SetMaxOpenConns(concurrency * 2)
	return &DB{
		db: db,
	}, nil
}

func (db *DB) GetConn(ctx context.Context) (*Conn, error) {
	conn, err := db.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	return &Conn{
		stmtCache: make(map[string]*sql.Stmt),
		conn:      conn,
		db:        db.db,
	}, nil
}

func (db *DB) Close() error {
	return db.db.Close()
}

func (conn *Conn) getAndCacheStmt(ctx context.Context, query string) (*sql.Stmt, error) {
	if stmt, ok := conn.stmtCache[query]; ok {
		return stmt, nil
	}

	stmt, err := conn.conn.PrepareContext(ctx, query)
	if err == sql.ErrConnDone {
		if conn.conn, err = conn.db.Conn(ctx); err == nil {
			stmt, err = conn.conn.PrepareContext(ctx, query)
		}
	}

	if err != nil {
		return nil, err
	}

	conn.stmtCache[query] = stmt
	return stmt, nil
}

func (conn *Conn) clearCacheIfFailed(query string, err error) {
	if err == nil {
		return
	}

	if stmt, ok := conn.stmtCache[query]; ok {
		_ = stmt.Close()
	}
	delete(conn.stmtCache, query)
}

func (conn *Conn) execQuery(ctx context.Context, query string, args ...interface{}) error {
	stmt, err := conn.getAndCacheStmt(ctx, query)
	if err != nil {
		return err
	}
	_, err = stmt.ExecContext(ctx, args...)
	conn.clearCacheIfFailed(query, err)
	return err
}

func (conn *Conn) beginTx(ctx context.Context) (*sql.Tx, error) {
	return conn.conn.BeginTx(ctx, nil)
}

func (conn *Conn) Close() error {
	return conn.conn.Close()
}
