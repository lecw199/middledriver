package middledriver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
)

// Conn is a connection to a database.
type Conn struct {
	driver Driver
	target driver.Conn

	queryContextFunc QueryContextFunc

	execContextFunc ExecContextFunc
}

func newConn(target driver.Conn, dri Driver, queryContextMiddleware QueryContextMiddleware, execContextMiddleware ExecContextMiddleware) Conn {
	conn := Conn{
		driver: dri,
		target: target,
	}

	conn.queryContextFunc = conn.generateQueryContextFunc()
	if queryContextMiddleware != nil {
		conn.queryContextFunc = queryContextMiddleware(conn.queryContextFunc)
	}

	conn.execContextFunc = conn.generateExecContextFunc()
	if execContextMiddleware != nil {
		conn.execContextFunc = execContextMiddleware(conn.execContextFunc)
	}

	return conn
}

// Ping implements Pinger.
func (conn Conn) Ping(ctx context.Context) error {
	pinger, ok := conn.target.(driver.Pinger)
	if ok {
		return pinger.Ping(ctx)
	}

	_, err := conn.QueryContext(ctx, "SELECT ?", []driver.NamedValue{{Value: 1}})
	return err
}

// Prepare implements Conn.
func (conn Conn) Prepare(query string) (driver.Stmt, error) {
	return nil, errors.New("Please update Go to 1.8+ version")
}

// PrepareContext implements ConnPrepareContext.
func (conn Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	connPrepareContext, ok := conn.target.(driver.ConnPrepareContext)
	if ok {
		stmtTarget, err := connPrepareContext.PrepareContext(ctx, query)
		if err != nil {
			return nil, err
		}
		return newStmt(stmtTarget, conn, query, conn.driver.MiddlewareGroup.NewStmtQueryContextMiddleware, conn.driver.MiddlewareGroup.NewStmtExecContextMiddleware)
	}

	if ctx.Done() != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
	}

	stmtTarget, err := conn.target.Prepare(query)
	if err != nil {
		return nil, err
	}
	return newStmt(stmtTarget, conn, query, conn.driver.MiddlewareGroup.NewStmtQueryContextMiddleware, conn.driver.MiddlewareGroup.NewStmtExecContextMiddleware)
}

// Close implements Conn.
func (conn Conn) Close() error {
	return conn.target.Close()
}

// Begin implements Conn.
func (conn Conn) Begin() (driver.Tx, error) {
	return nil, errors.New("Please update Go to 1.8+ version")
}

// BeginTx implements ConnBeginTx.
func (conn Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	connBeginTx, ok := conn.target.(driver.ConnBeginTx)
	if ok {
		return connBeginTx.BeginTx(ctx, opts)
	}

	if opts.Isolation != driver.IsolationLevel(int(sql.LevelDefault)) {
		return nil, errors.New("not support non-default isolation level")
	}
	if opts.ReadOnly {
		return nil, errors.New("not support read-only transactions")
	}

	txTarget, err := conn.target.Begin()
	if ctx.Done() == nil {
		return txTarget, err
	}
	if err == nil {
		select {
		case <-ctx.Done():
			txTarget.Rollback()
			return nil, ctx.Err()
		default:
		}
	}
	return txTarget, err
}

func (conn Conn) generateQueryContextFunc() QueryContextFunc {
	targetQueryerContext, ok := conn.target.(driver.QueryerContext)
	if ok {
		return targetQueryerContext.QueryContext
	}

	queryer, ok := conn.target.(driver.Queryer)
	if ok {
		return func(ctx context.Context, query string, namedArg []driver.NamedValue) (driver.Rows, error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}
			arg, err := namedValueToValue(namedArg)
			if err != nil {
				return nil, err
			}
			return queryer.Query(query, arg)
		}
	}

	return func(ctx context.Context, query string, namedArg []driver.NamedValue) (driver.Rows, error) {
		stmt, err := conn.PrepareContext(ctx, query)
		if err != nil {
			return nil, err
		}
		defer stmt.Close()
		stmtContext := stmt.(Stmt)
		return stmtContext.QueryContext(ctx, namedArg)
	}
}

// QueryContext implements QueryerContext.
func (conn Conn) QueryContext(ctx context.Context, query string, namedArg []driver.NamedValue) (driver.Rows, error) {
	return conn.queryContextFunc(ctx, query, namedArg)
}

func (conn Conn) generateExecContextFunc() ExecContextFunc {
	execerConntext, ok := conn.target.(driver.ExecerContext)
	if ok {
		return execerConntext.ExecContext
	}

	execer, ok := conn.target.(driver.Execer)
	if ok {
		return func(ctx context.Context, query string, namedArg []driver.NamedValue) (driver.Result, error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}
			arg, err := namedValueToValue(namedArg)
			if err != nil {
				return nil, err
			}
			return execer.Exec(query, arg)
		}
	}

	return func(ctx context.Context, query string, namedArg []driver.NamedValue) (driver.Result, error) {
		stmt, err := conn.PrepareContext(ctx, query)
		if err != nil {
			return nil, err
		}
		defer stmt.Close()
		stmtContext := stmt.(Stmt)
		return stmtContext.ExecContext(ctx, namedArg)
	}
}

// ExecContext implements ExecerContext.
func (conn Conn) ExecContext(ctx context.Context, query string, namedArg []driver.NamedValue) (driver.Result, error) {
	return conn.execContextFunc(ctx, query, namedArg)
}
