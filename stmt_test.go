package middledriver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/wencan/middledriver/internal/fakedriver"
)

func TestStmt_QueryContext(t *testing.T) {
	IntAddr := func(n int) *int {
		return &n
	}
	StringAddr := func(s string) *string {
		return &s
	}

	testCases := []struct {
		Name                          string
		DriverName                    string
		NewStmtQueryContextMiddleware NewStmtQueryContextMiddleware
		Query                         string
		Args                          []interface{}
		WantQuery                     string
		WantNamedArg                  []driver.NamedValue
		PrepareError                  error
		ReplyRows                     driver.Rows
		ReplyError                    error
		RowsDest                      [][]interface{}
		WantColumns                   []string
		WantRows                      [][]interface{}
		WantPrepareError              error
		WantQueryError                error
	}{
		{
			Name:       "test_stmt_querycontext_select1",
			DriverName: "test_stmt_querycontext_select1",
			Query:      "SELECT 1",
			WantQuery:  "SELECT 1",
			ReplyRows: &fakedriver.FakeRows{
				ColumnNames: []string{"select1"},
				Rows:        [][]driver.Value{{1}},
			},
			RowsDest: [][]interface{}{
				{new(int)},
			},
			WantColumns: []string{"select1"},
			WantRows: [][]interface{}{
				{IntAddr(1)},
			},
		},
		{
			Name:       "test_stmt_querycontext_select_1",
			DriverName: "test_stmt_querycontext_select_1",
			Query:      "SELECT ?",
			Args: []interface{}{
				1,
			},
			WantQuery: "SELECT ?",
			WantNamedArg: []driver.NamedValue{
				{
					Ordinal: 1,
					Value:   int64(1),
				},
			},
			ReplyRows: &fakedriver.FakeRows{
				ColumnNames: []string{"select_1"},
				Rows:        [][]driver.Value{{1}},
			},
			RowsDest: [][]interface{}{
				{new(int)},
			},
			WantColumns: []string{"select_1"},
			WantRows: [][]interface{}{
				{IntAddr(1)},
			},
		},
		{
			Name:       "test_stmt_querycontext_select_named_1",
			DriverName: "test_stmt_querycontext_select_named_1",
			Query:      "SELECT @number",
			Args: []interface{}{
				sql.Named("number", 1),
			},
			WantQuery: "SELECT @number",
			WantNamedArg: []driver.NamedValue{
				{
					Name:    "number",
					Ordinal: 1,
					Value:   int64(1),
				},
			},
			ReplyRows: &fakedriver.FakeRows{
				ColumnNames: []string{"number"},
				Rows:        [][]driver.Value{{1}},
			},
			RowsDest: [][]interface{}{
				{new(int)},
			},
			WantColumns: []string{"number"},
			WantRows: [][]interface{}{
				{IntAddr(1)},
			},
		},
		{
			Name:       "test_stmt_querycontext_name",
			DriverName: "test_stmt_querycontext_name",
			Query:      "SELECT name FROM users WHERE age=?",
			Args: []interface{}{
				18,
			},
			WantQuery: "SELECT name FROM users WHERE age=?",
			WantNamedArg: []driver.NamedValue{
				{
					Ordinal: 1,
					Value:   int64(18),
				},
			},
			ReplyRows: &fakedriver.FakeRows{
				ColumnNames: []string{"name"},
				Rows:        [][]driver.Value{{"zhangsan"}, {"lisi"}},
			},
			RowsDest: [][]interface{}{
				{new(string)}, {new(string)},
			},
			WantColumns: []string{"name"},
			WantRows: [][]interface{}{
				{StringAddr("zhangsan")}, {StringAddr("lisi")},
			},
		},
		{
			Name:       "test_stmt_querycontext_name_middleware",
			DriverName: "test_stmt_querycontext_name_middleware",
			NewStmtQueryContextMiddleware: func(query string) (StmtQueryContextMiddleware, error) {
				return func(next StmtQueryContextFunc) StmtQueryContextFunc {
					return func(ctx context.Context, namedArg []driver.NamedValue) (driver.Rows, error) {
						for idx, arg := range namedArg {
							arg.Name = "age"
							arg.Value = arg.Value.(int64) - 1
							namedArg[idx] = arg
						}
						return next(ctx, namedArg)
					}
				}, nil
			},
			Query: "SELECT name FROM users WHERE age=?",
			Args: []interface{}{
				18,
			},
			WantQuery: "SELECT name FROM users WHERE age=?",
			WantNamedArg: []driver.NamedValue{
				{
					Name:    "age",
					Ordinal: 1,
					Value:   int64(17),
				},
			},
			ReplyRows: &fakedriver.FakeRows{
				ColumnNames: []string{"name"},
				Rows:        [][]driver.Value{{"zhangsan"}, {"lisi"}},
			},
			RowsDest: [][]interface{}{
				{new(string)}, {new(string)},
			},
			WantColumns: []string{"name"},
			WantRows: [][]interface{}{
				{StringAddr("zhangsan")}, {StringAddr("lisi")},
			},
		},
		{
			Name:       "test_stmt_querycontext_name_middleware_chain",
			DriverName: "test_stmt_querycontext_name_middleware_chain",
			NewStmtQueryContextMiddleware: NewStmtQueryContextMiddlewareChain(func(query string) (StmtQueryContextMiddleware, error) {
				return func(next StmtQueryContextFunc) StmtQueryContextFunc {
					return func(ctx context.Context, namedArg []driver.NamedValue) (driver.Rows, error) {
						for idx, arg := range namedArg {
							arg.Name = "age"
							namedArg[idx] = arg
						}
						return next(ctx, namedArg)
					}
				}, nil
			}, func(query string) (StmtQueryContextMiddleware, error) {
				return func(next StmtQueryContextFunc) StmtQueryContextFunc {
					return func(ctx context.Context, namedArg []driver.NamedValue) (driver.Rows, error) {
						for idx, arg := range namedArg {
							if arg.Name == "age" {
								arg.Value = arg.Value.(int64) - 1
								namedArg[idx] = arg
							}
						}
						return next(ctx, namedArg)
					}
				}, nil
			}),
			Query: "SELECT name FROM users WHERE age=?",
			Args: []interface{}{
				18,
			},
			WantQuery: "SELECT name FROM users WHERE age=?",
			WantNamedArg: []driver.NamedValue{
				{
					Name:    "age",
					Ordinal: 1,
					Value:   int64(17),
				},
			},
			ReplyRows: &fakedriver.FakeRows{
				ColumnNames: []string{"name"},
				Rows:        [][]driver.Value{{"zhangsan"}, {"lisi"}},
			},
			RowsDest: [][]interface{}{
				{new(string)}, {new(string)},
			},
			WantColumns: []string{"name"},
			WantRows: [][]interface{}{
				{StringAddr("zhangsan")}, {StringAddr("lisi")},
			},
		},
		{
			Name:       "test_stmt_querycontext_name_notfound",
			DriverName: "test_stmt_querycontext_name_notfound",
			Query:      "SELECT name FROM users WHERE age=?",
			Args: []interface{}{
				18,
			},
			WantQuery: "SELECT name FROM users WHERE age=?",
			WantNamedArg: []driver.NamedValue{
				{
					Ordinal: 1,
					Value:   int64(18),
				},
			},
			ReplyRows: &fakedriver.FakeRows{
				ColumnNames: []string{"name"},
			},
			WantColumns: []string{"name"},
		},
		{
			Name:           "test_stmt_querycontext_error",
			DriverName:     "test_stmt_querycontext_error",
			Query:          "SELECT 1",
			WantQuery:      "SELECT 1",
			ReplyError:     errors.New("test"),
			WantQueryError: errors.New("test"),
		},
		{
			Name:       "test_stmt_querycontext_name_middleware_prepare_error",
			DriverName: "test_stmt_querycontext_name_middleware_prepare_error",
			NewStmtQueryContextMiddleware: func(query string) (StmtQueryContextMiddleware, error) {
				return nil, errors.New("test")
			},
			Query: "SELECT name FROM users WHERE age=?",
			Args: []interface{}{
				18,
			},
			WantPrepareError: errors.New("test"),
		},
		{
			Name:       "test_stmt_querycontext_name_middleware_query_error",
			DriverName: "test_stmt_querycontext_name_middleware_query_error",
			NewStmtQueryContextMiddleware: func(query string) (StmtQueryContextMiddleware, error) {
				return func(next StmtQueryContextFunc) StmtQueryContextFunc {
					return func(ctx context.Context, namedArg []driver.NamedValue) (driver.Rows, error) {
						return nil, errors.New("test")
					}
				}, nil
			},
			Query: "SELECT name FROM users WHERE age=?",
			Args: []interface{}{
				18,
			},
			WantQueryError: errors.New("test"),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			dri := Driver{
				Target: fakedriver.FakeDriver{
					ExpectedQueryContext: func(ctx context.Context, query string, namedArg []driver.NamedValue) (driver.Rows, error) {
						if query != testCase.WantQuery {
							return nil, fmt.Errorf("want query %s, got %s", testCase.WantQuery, query)
						}
						if (len(namedArg)+len(testCase.WantNamedArg)) > 0 && !reflect.DeepEqual(namedArg, testCase.WantNamedArg) {
							return nil, fmt.Errorf("want namedArg %+v, got %+v", testCase.WantNamedArg, namedArg)
						}
						return testCase.ReplyRows, testCase.ReplyError
					},
				},
				MiddlewareGroup: MiddlewareGroup{
					NewStmtQueryContextMiddleware: testCase.NewStmtQueryContextMiddleware,
				},
			}
			sql.Register(testCase.DriverName, dri)

			db, err := sql.Open(testCase.DriverName, "foo")
			if err != nil {
				t.Fatal(err)
			}

			stmt, err := db.PrepareContext(context.TODO(), testCase.Query)
			if testCase.WantPrepareError != nil {
				gotError := "<nil>"
				if err != nil {
					gotError = err.Error()
				}
				if testCase.WantPrepareError.Error() != gotError {
					t.Fatalf("want prepare error %s, got %s", testCase.WantPrepareError.Error(), gotError)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer stmt.Close()

			rows, err := stmt.QueryContext(context.TODO(), testCase.Args...)
			if testCase.WantQueryError != nil {
				gotError := "<nil>"
				if err != nil {
					gotError = err.Error()
				}
				if testCase.WantQueryError.Error() != gotError {
					t.Fatalf("want error %s, got %s", testCase.WantQueryError.Error(), gotError)
				}
				return
			}

			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()

			columns, err := rows.Columns()
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(testCase.WantColumns, columns) {
				t.Fatalf("want columns %+v, got %v+", testCase.WantColumns, columns)
			}

			for _, dest := range testCase.RowsDest {
				if !rows.Next() {
					t.Fatalf("rows length error")
				}
				err = rows.Scan(dest...)
				if err != nil {
					t.Fatal(err)
				}
			}

			if !reflect.DeepEqual(testCase.RowsDest, testCase.WantRows) {
				t.Fatalf("want rows %+v, got %+v", testCase.WantRows, testCase.RowsDest)
			}
		})
	}
}

func TestStmt_ExecContext(t *testing.T) {
	testCases := []struct {
		Name                         string
		DriverName                   string
		NewStmtExecContextMiddleware NewStmtExecContextMiddleware
		Query                        string
		Args                         []interface{}
		WantQuery                    string
		WantNamedArg                 []driver.NamedValue
		ReplyResult                  driver.Result
		ReplyError                   error
		WantAffectedRows             int64
		WantLastInsertID             int64
		WantPrepareError             error
		WantExecuteError             error
	}{
		{
			Name:       "test_stmt_execcontext_update_balances",
			DriverName: "test_stmt_execcontext_update_balances",
			Query:      "UPDATE balances SET balance = balance + 10 WHERE user_id = ?",
			Args: []interface{}{
				12345,
			},
			WantQuery: "UPDATE balances SET balance = balance + 10 WHERE user_id = ?",
			WantNamedArg: []driver.NamedValue{
				{
					Ordinal: 1,
					Value:   int64(12345),
				},
			},
			ReplyResult: &fakedriver.FakeResult{
				AffectedRows: 1,
			},
			WantAffectedRows: 1,
		},
		{
			Name:       "test_stmt_execcontext_set_status",
			DriverName: "test_stmt_execcontext_set_status",
			Query:      "UPDATE users SET status = ? WHERE id = ?",
			Args: []interface{}{
				"paid",
				100,
			},
			WantQuery: "UPDATE users SET status = ? WHERE id = ?",
			WantNamedArg: []driver.NamedValue{
				{
					Ordinal: 1,
					Value:   "paid",
				},
				{
					Ordinal: 2,
					Value:   int64(100),
				},
			},
			ReplyResult: &fakedriver.FakeResult{
				AffectedRows: 2,
				InsertID:     0,
			},
			WantAffectedRows: 2,
		},
		{
			Name:       "test_stmt_execcontext_set_status_middleware",
			DriverName: "test_stmt_execcontext_set_status_middleware",
			NewStmtExecContextMiddleware: func(query string) (StmtExecContextMiddleware, error) {
				return func(next StmtExecContextFunc) StmtExecContextFunc {
					return func(ctx context.Context, namedArg []driver.NamedValue) (driver.Result, error) {
						namedArg = []driver.NamedValue{
							{
								Name:    "status",
								Ordinal: namedArg[0].Ordinal,
								Value:   namedArg[0].Value,
							},
							{
								Name:    "id",
								Ordinal: namedArg[1].Ordinal,
								Value:   namedArg[1].Value,
							},
						}
						return next(ctx, namedArg)
					}
				}, nil
			},
			Query: "UPDATE users SET status = ? WHERE id = ?",
			Args: []interface{}{
				"paid",
				100,
			},
			WantQuery: "UPDATE users SET status = ? WHERE id = ?",
			WantNamedArg: []driver.NamedValue{
				{
					Name:    "status",
					Ordinal: 1,
					Value:   "paid",
				},
				{
					Name:    "id",
					Ordinal: 2,
					Value:   int64(100),
				},
			},
			ReplyResult: &fakedriver.FakeResult{
				AffectedRows: 2,
				InsertID:     0,
			},
			WantAffectedRows: 2,
		},
		{
			Name:       "test_stmt_execcontext_set_status_middleware_chain",
			DriverName: "test_stmt_execcontext_set_status_middleware_chain",
			NewStmtExecContextMiddleware: NewStmtExecContextMiddlewareChain(func(query string) (StmtExecContextMiddleware, error) {
				return func(next StmtExecContextFunc) StmtExecContextFunc {
					return func(ctx context.Context, namedArg []driver.NamedValue) (driver.Result, error) {
						namedArg = []driver.NamedValue{
							{
								Name:    "status",
								Ordinal: namedArg[0].Ordinal,
								Value:   namedArg[0].Value,
							},
							{
								Name:    "id",
								Ordinal: namedArg[1].Ordinal,
								Value:   namedArg[1].Value,
							},
						}
						return next(ctx, namedArg)
					}
				}, nil
			}, func(query string) (StmtExecContextMiddleware, error) {
				return func(next StmtExecContextFunc) StmtExecContextFunc {
					return func(ctx context.Context, namedArg []driver.NamedValue) (driver.Result, error) {
						for idx, arg := range namedArg {
							if arg.Name == "id" {
								arg.Value = arg.Value.(int64) + 1
								namedArg[idx] = arg
							}
						}
						return next(ctx, namedArg)
					}
				}, nil
			}),
			Query: "UPDATE users SET status = ? WHERE id = ?",
			Args: []interface{}{
				"paid",
				100,
			},
			WantQuery: "UPDATE users SET status = ? WHERE id = ?",
			WantNamedArg: []driver.NamedValue{
				{
					Name:    "status",
					Ordinal: 1,
					Value:   "paid",
				},
				{
					Name:    "id",
					Ordinal: 2,
					Value:   int64(101),
				},
			},
			ReplyResult: &fakedriver.FakeResult{
				AffectedRows: 2,
				InsertID:     0,
			},
			WantAffectedRows: 2,
		},
		{
			Name:       "test_stmt_execcontext_error",
			DriverName: "test_stmt_execcontext_error",
			Query:      "UPDATE users SET status = ? WHERE id = ?",
			Args: []interface{}{
				"paid",
				100,
			},
			WantQuery: "UPDATE users SET status = ? WHERE id = ?",
			WantNamedArg: []driver.NamedValue{
				{
					Ordinal: 1,
					Value:   "paid",
				},
				{
					Ordinal: 2,
					Value:   int64(100),
				},
			},
			ReplyError:       errors.New("test"),
			WantAffectedRows: 0,
			WantExecuteError: errors.New("test"),
		},
		{
			Name:       "test_stmt_execcontext_set_status_middleware_prepare_error",
			DriverName: "test_stmt_execcontext_set_status_middleware_prepare_error",
			NewStmtExecContextMiddleware: func(query string) (StmtExecContextMiddleware, error) {
				return nil, errors.New("test")
			},
			Query: "UPDATE users SET status = ? WHERE id = ?",
			Args: []interface{}{
				"paid",
				100,
			},
			WantPrepareError: errors.New("test"),
		},
		{
			Name:       "test_stmt_execcontext_set_status_middleware_execute_error",
			DriverName: "test_stmt_execcontext_set_status_middleware_execute_error",
			NewStmtExecContextMiddleware: func(query string) (StmtExecContextMiddleware, error) {
				return func(next StmtExecContextFunc) StmtExecContextFunc {
					return func(ctx context.Context, namedArg []driver.NamedValue) (driver.Result, error) {
						return nil, errors.New("test")
					}

				}, nil
			},
			Query: "UPDATE users SET status = ? WHERE id = ?",
			Args: []interface{}{
				"paid",
				100,
			},
			WantExecuteError: errors.New("test"),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			dri := Driver{
				Target: fakedriver.FakeDriver{
					ExpectedExecContext: func(ctx context.Context, query string, namedArg []driver.NamedValue) (driver.Result, error) {
						if query != testCase.WantQuery {
							return nil, fmt.Errorf("want query %s, got %s", testCase.WantQuery, query)
						}
						if len(namedArg) > 0 && len(testCase.WantNamedArg) > 0 && !reflect.DeepEqual(namedArg, testCase.WantNamedArg) {
							return nil, fmt.Errorf("want namedArg %+v, got %+v", testCase.WantNamedArg, namedArg)
						}
						return testCase.ReplyResult, testCase.ReplyError
					},
				},
				MiddlewareGroup: MiddlewareGroup{
					NewStmtExecContextMiddleware: testCase.NewStmtExecContextMiddleware,
				},
			}
			sql.Register(testCase.DriverName, dri)

			db, err := sql.Open(testCase.DriverName, "foo")
			if err != nil {
				t.Fatal(err)
			}

			stmt, err := db.PrepareContext(context.TODO(), testCase.Query)
			if testCase.WantPrepareError != nil {
				gotError := "<nil>"
				if err != nil {
					gotError = err.Error()
				}
				if testCase.WantPrepareError.Error() != gotError {
					t.Fatalf("want prepare error %s, got %s", testCase.WantPrepareError.Error(), gotError)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer stmt.Close()

			result, err := stmt.ExecContext(context.TODO(), testCase.Args...)
			if testCase.WantExecuteError != nil {
				gotError := "<nil>"
				if err != nil {
					gotError = err.Error()
				}
				if testCase.WantExecuteError.Error() != gotError {
					t.Fatalf("want error %s, got %s", testCase.WantExecuteError, gotError)
				}
				return
			}

			if err != nil {
				t.Fatal(err)
			}

			affectedRows, err := result.RowsAffected()
			if err != nil {
				t.Error(err)
			}
			if testCase.WantAffectedRows != affectedRows {
				t.Fatalf("want affectdRows %d, got %d", testCase.WantAffectedRows, affectedRows)
			}
			lastInsertID, err := result.LastInsertId()
			if err != nil {
				t.Error(err)
			}
			if testCase.WantLastInsertID != lastInsertID {
				t.Fatalf("want lastinsertID %d, got %d", testCase.WantAffectedRows, lastInsertID)
			}
		})
	}
}
