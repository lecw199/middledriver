package fakedriver

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
)

var _ driver.DriverContext = FakeDriver{}
var _ driver.QueryerContext = FakeConn{}
var _ driver.ExecerContext = FakeConn{}
var _ driver.Stmt = FakeStmt{}
var _ driver.StmtExecContext = FakeStmt{}
var _ driver.StmtQueryContext = FakeStmt{}

type FakeDriver struct {
	ExpectedPing func(context.Context) error

	ExpectedQueryContext func(ctx context.Context, query string, namedArg []driver.NamedValue) (driver.Rows, error)

	ExpectedExecContext func(ctx context.Context, query string, namedArg []driver.NamedValue) (driver.Result, error)
}

// Open implements Driver.
func (dri FakeDriver) Open(name string) (driver.Conn, error) {
	return nil, errors.New("Please update Go to 1.10+ version")
}

// OpenConnector implements DriverContext.
func (dri FakeDriver) OpenConnector(name string) (driver.Connector, error) {
	return FakeConnector{
		driver: &dri,
	}, nil
}

type FakeConnector struct {
	driver *FakeDriver
}

// Connect implements Connector.
func (connector FakeConnector) Connect(ctx context.Context) (driver.Conn, error) {
	return FakeConn{
		driver: connector.driver,
		// connector: &connector,
	}, nil
}

// Driver implements Connector.
func (connector FakeConnector) Driver() driver.Driver {
	return connector.driver
}

type FakeConn struct {
	driver *FakeDriver

	// connector *FakeConnector
}

// Ping implements Pinger.
func (conn FakeConn) Ping(ctx context.Context) error {
	if conn.driver.ExpectedPing != nil {
		return conn.driver.ExpectedPing(ctx)
	}

	_, err := conn.QueryContext(ctx, "SELECT 1", nil)
	return err
}

// Prepare implements Conn.
func (conn FakeConn) Prepare(query string) (driver.Stmt, error) {
	return nil, errors.New("Please update Go to 1.8+ version")
}

// PrepareContext implements ConnPrepareContext.
func (conn FakeConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return FakeStmt{
		driver: conn.driver,
		query:  query,
	}, nil
}

// Close implements Conn.
func (conn FakeConn) Close() error {
	return nil
}

// Begin implements Conn.
func (conn FakeConn) Begin() (driver.Tx, error) {
	return nil, errors.New("Please update Go to 1.8+ version")
}

// BeginTx implements ConnBeginTx.
func (conn FakeConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return FakeTx{}, nil
}

// QueryContext implements QueryerContext.
func (conn FakeConn) QueryContext(ctx context.Context, query string, namedArg []driver.NamedValue) (driver.Rows, error) {
	if conn.driver.ExpectedQueryContext != nil {
		return conn.driver.ExpectedQueryContext(ctx, query, namedArg)
	}
	return nil, ErrUnimplemented
}

// ExecContext implements ExecerContext.
func (conn FakeConn) ExecContext(ctx context.Context, query string, namedArg []driver.NamedValue) (driver.Result, error) {
	if conn.driver.ExpectedExecContext != nil {
		return conn.driver.ExpectedExecContext(ctx, query, namedArg)
	}
	return nil, ErrUnimplemented
}

type FakeTx struct {
}

func (tx FakeTx) Commit() error {
	return nil
}

func (tx FakeTx) Rollback() error {
	return nil
}

type FakeStmt struct {
	driver *FakeDriver
	query  string
}

// Close implements Stmt.
func (stmt FakeStmt) Close() error {
	return nil
}

// NumInput implements Stmt.
func (stmt FakeStmt) NumInput() int {
	return -1
}

// Query implements Stmt.
func (stmt FakeStmt) Query(args []driver.Value) (driver.Rows, error) {
	return nil, errors.New("Please update Go to 1.8+ version")
}

// QueryContext implements StmtQueryContext.
func (stmt FakeStmt) QueryContext(ctx context.Context, namedArg []driver.NamedValue) (driver.Rows, error) {
	if stmt.driver.ExpectedQueryContext != nil {
		return stmt.driver.ExpectedQueryContext(ctx, stmt.query, namedArg)
	}
	return nil, ErrUnimplemented
}

// Exec implements Stmt.
func (stmt FakeStmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, errors.New("Please update Go to 1.8+ version")
}

// ExecContext implements StmtExecContext.
func (stmt FakeStmt) ExecContext(ctx context.Context, namedArg []driver.NamedValue) (driver.Result, error) {
	if stmt.driver.ExpectedExecContext != nil {
		return stmt.driver.ExpectedExecContext(ctx, stmt.query, namedArg)
	}
	return nil, ErrUnimplemented
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
func (stmt FakeStmt) CheckNamedValue(nv *driver.NamedValue) error {
	return defaultNamedValueChecker{}.CheckNamedValue(nv)
}

type FakeRows struct {
	ColumnNames []string
	Rows        [][]driver.Value
	pos         int
	NextError   error
	CloseError  error
}

// Columns returns the names of the columns. The number of
// columns of the result is inferred from the length of the
// slice. If a particular column name isn't known, an empty
// string should be returned for that entry.
func (rows FakeRows) Columns() []string {
	return rows.ColumnNames
}

// Close closes the rows iterator.
func (rows *FakeRows) Close() error {
	return rows.CloseError
}

// Next is called to populate the next row of data into
// the provided slice. The provided slice will be the same
// size as the Columns() are wide.
//
// Next should return io.EOF when there are no more rows.
//
// The dest should not be written to outside of Next. Care
// should be taken when closing Rows not to modify
// a buffer held in dest.
func (rows *FakeRows) Next(dest []driver.Value) error {
	if rows.NextError != nil {
		return rows.NextError
	}
	if rows.pos >= len(rows.Rows) {
		return io.EOF
	}

	row := rows.Rows[rows.pos]
	for idx, cell := range row {
		dest[idx] = cell
	}
	rows.pos++
	return nil
}

// FakeResult is the result of a query execution.
type FakeResult struct {
	AffectedRows int64
	InsertID     int64
}

// LastInsertId returns the database's auto-generated ID
// after, for example, an INSERT into a table with primary
// key.
func (result FakeResult) LastInsertId() (int64, error) {
	return result.InsertID, nil
}

// RowsAffected returns the number of rows affected by the
// query.
func (result FakeResult) RowsAffected() (int64, error) {
	return result.AffectedRows, nil
}
