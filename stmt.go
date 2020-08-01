package middledriver

import (
	"context"
	"database/sql/driver"
	"errors"
)

// Stmt is a prepared statement.
type Stmt struct {
	target driver.Stmt

	queryContextMiddleware StmtQueryContextMiddleware

	execContextMiddleware StmtExecContextMiddleware

	conn Conn

	query string

	queryContextFunc StmtQueryContextFunc

	execContextFunc StmtExecContextFunc
}

func newStmt(target driver.Stmt, conn Conn, query string, newStmtQueryContextMiddleware NewStmtQueryContextMiddleware, newStmtExecContextMiddleware NewStmtExecContextMiddleware) (Stmt, error) {
	stmt := Stmt{
		target: target,
		conn:   conn,
		query:  query,
	}

	stmt.queryContextFunc = stmt.generateQueryContextFunc()
	if newStmtQueryContextMiddleware != nil {
		queryContextMiddleware, err := newStmtQueryContextMiddleware(query)
		if err != nil {
			return Stmt{}, err
		}
		stmt.queryContextFunc = queryContextMiddleware(stmt.queryContextFunc)
	}

	stmt.execContextFunc = stmt.generateExecContextFunc()
	if newStmtExecContextMiddleware != nil {
		execContextMiddleware, err := newStmtExecContextMiddleware(query)
		if err != nil {
			return Stmt{}, err
		}
		stmt.execContextFunc = execContextMiddleware(stmt.execContextFunc)
	}

	return stmt, nil
}

// Close implements Stmt.
func (stmt Stmt) Close() error {
	return stmt.target.Close()
}

// NumInput implements Stmt.
func (stmt Stmt) NumInput() int {
	return stmt.target.NumInput()
}

// Query implements Stmt.
func (stmt Stmt) Query(args []driver.Value) (driver.Rows, error) {
	return nil, errors.New("Please update Go to 1.8+ version")
}

func (stmt Stmt) generateQueryContextFunc() StmtQueryContextFunc {
	targetQueryContext, ok := stmt.target.(driver.StmtQueryContext)
	if ok {
		return targetQueryContext.QueryContext
	}

	return func(ctx context.Context, namedArg []driver.NamedValue) (driver.Rows, error) {
		return nil, errors.New("deriver not support stmt QueryContext")
	}
}

// QueryContext implements StmtQueryContext.
func (stmt Stmt) QueryContext(ctx context.Context, namedArg []driver.NamedValue) (driver.Rows, error) {
	return stmt.queryContextFunc(ctx, namedArg)
}

// Exec implements Stmt.
func (stmt Stmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, errors.New("Please update Go to 1.8+ version")
}

func (stmt Stmt) generateExecContextFunc() StmtExecContextFunc {
	targetExecContext, ok := stmt.target.(driver.StmtExecContext)
	if ok {
		return targetExecContext.ExecContext
	}

	return func(ctx context.Context, namedArg []driver.NamedValue) (driver.Result, error) {
		return nil, errors.New("driver not support stmt ExecContext")
	}
}

// ExecContext implements StmtExecContext.
func (stmt Stmt) ExecContext(ctx context.Context, namedArg []driver.NamedValue) (driver.Result, error) {
	return stmt.execContextFunc(ctx, namedArg)
}

type defaultNamedValueChecker struct {
}

// CheckNamedValue implements NamedValueChecker.
func (defaultNamedValueChecker) CheckNamedValue(nv *driver.NamedValue) error {
	var err error
	nv.Value, err = driver.DefaultParameterConverter.ConvertValue(nv.Value)
	return err
}

// CheckNamedValue implements NamedValueChecker.
func (stmt Stmt) CheckNamedValue(nv *driver.NamedValue) error {
	checker, ok := stmt.target.(driver.NamedValueChecker)
	if !ok {
		checker, ok = stmt.conn.target.(driver.NamedValueChecker)
	}
	if !ok {
		checker = defaultNamedValueChecker{}
	}

	return checker.CheckNamedValue(nv)
}
