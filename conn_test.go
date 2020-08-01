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

func TestConn_Ping(t *testing.T) {
	testCases := []struct {
		Name       string
		Target     driver.Driver
		DriverName string
		WantError  error
	}{
		{
			Name:       "test_simple_ping",
			DriverName: "test_simple_ping",
			Target: fakedriver.FakeDriver{
				ExpectedPing: func(ctx context.Context) error {
					return nil
				},
			},
		},
		{
			Name:       "test_select1_ping",
			DriverName: "test_select1_ping",
			Target: fakedriver.FakeDriver{
				ExpectedQueryContext: func(ctx context.Context, query string, namedArg []driver.NamedValue) (driver.Rows, error) {
					rows := &fakedriver.FakeRows{
						ColumnNames: []string{""},
						Rows:        [][]driver.Value{{1}},
					}
					return rows, nil
				},
			},
		},
		{
			Name:       "test_ping_error",
			DriverName: "test_ping_error",
			Target: fakedriver.FakeDriver{
				ExpectedPing: func(ctx context.Context) error {
					return errors.New("test")
				},
			},
			WantError: errors.New("test"),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			dri := Driver{
				Target: testCase.Target,
			}
			sql.Register(testCase.DriverName, dri)

			db, err := sql.Open(testCase.DriverName, "foo")
			if err != nil {
				t.Fatal(err)
			}
			err = db.PingContext(context.TODO())

			if testCase.WantError != nil {
				gotError := "<nil>"
				if err != nil {
					gotError = err.Error()
				}
				if testCase.WantError.Error() != gotError {
					t.Fatalf("want error %s, got %s", testCase.WantError.Error(), gotError)
				}
				return
			}

			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestConn_QueryContext(t *testing.T) {
	IntAddr := func(n int) *int {
		return &n
	}
	StringAddr := func(s string) *string {
		return &s
	}

	testCases := []struct {
		Name                   string
		DriverName             string
		QueryContextMiddleware QueryContextMiddleware
		Query                  string
		Args                   []interface{}
		WantQuery              string
		WantNamedArg           []driver.NamedValue
		ReplyRows              driver.Rows
		ReplyError             error
		RowsDest               [][]interface{}
		WantColumns            []string
		WantRows               [][]interface{}
		WantError              error
	}{
		{
			Name:       "test_conn_querycontext_select1",
			DriverName: "test_conn_querycontext_select1",
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
			Name:       "test_conn_querycontext_select_1",
			DriverName: "test_conn_querycontext_select_1",
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
			Name:       "test_conn_querycontext_select_named_1",
			DriverName: "test_conn_querycontext_select_named_1",
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
			Name:       "test_conn_querycontext_name",
			DriverName: "test_conn_querycontext_name",
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
			Name:       "test_conn_querycontext_name_middleware",
			DriverName: "test_conn_querycontext_name_middleware",
			QueryContextMiddleware: func(next QueryContextFunc) QueryContextFunc {
				return func(ctx context.Context, query string, namedArg []driver.NamedValue) (driver.Rows, error) {
					for idx, arg := range namedArg {
						arg.Name = "age"
						arg.Value = arg.Value.(int64) - 1
						namedArg[idx] = arg
					}
					return next(ctx, query, namedArg)
				}
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
			Name:       "test_conn_querycontext_name_middleware_chain",
			DriverName: "test_conn_querycontext_name_middleware_chain",
			QueryContextMiddleware: QueryContextMiddlewareChain(func(next QueryContextFunc) QueryContextFunc {
				return func(ctx context.Context, query string, namedArg []driver.NamedValue) (driver.Rows, error) {
					for idx, arg := range namedArg {
						arg.Name = "age"
						namedArg[idx] = arg
					}
					return next(ctx, query, namedArg)
				}
			}, func(next QueryContextFunc) QueryContextFunc {
				return func(ctx context.Context, query string, namedArg []driver.NamedValue) (driver.Rows, error) {
					for idx, arg := range namedArg {
						if arg.Name != "age" {
							return nil, fmt.Errorf("want arg name %s, got %s", "age", arg.Name)
						}
						arg.Value = arg.Value.(int64) - 1
						namedArg[idx] = arg
					}
					return next(ctx, query, namedArg)
				}
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
			Name:       "test_conn_querycontext_name_notfound",
			DriverName: "test_conn_querycontext_name_notfound",
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
			Name:       "test_conn_querycontext_error",
			DriverName: "test_conn_querycontext_error",
			Query:      "SELECT 1",
			WantQuery:  "SELECT 1",
			ReplyError: errors.New("test"),
			WantError:  errors.New("test"),
		},
		{
			Name:       "test_conn_querycontext_name_middleware_error",
			DriverName: "test_conn_querycontext_name_middleware_error",
			QueryContextMiddleware: func(next QueryContextFunc) QueryContextFunc {
				return func(ctx context.Context, query string, namedArg []driver.NamedValue) (driver.Rows, error) {
					return nil, errors.New("test")
				}
			},
			Query: "SELECT name FROM users WHERE age=?",
			Args: []interface{}{
				18,
			},
			WantError: errors.New("test"),
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
					QueryContextMiddleware: testCase.QueryContextMiddleware,
				},
			}
			sql.Register(testCase.DriverName, dri)

			db, err := sql.Open(testCase.DriverName, "foo")
			if err != nil {
				t.Fatal(err)
			}
			rows, err := db.QueryContext(context.TODO(), testCase.Query, testCase.Args...)

			if testCase.WantError != nil {
				gotError := "<nil>"
				if err != nil {
					gotError = err.Error()
				}
				if testCase.WantError.Error() != gotError {
					t.Fatalf("want error %s, got %s", testCase.WantError.Error(), gotError)
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

func TestConn_ExecContext(t *testing.T) {
	testCases := []struct {
		Name                  string
		DriverName            string
		ExecContextMiddleware ExecContextMiddleware
		Query                 string
		Args                  []interface{}
		WantQuery             string
		WantNamedArg          []driver.NamedValue
		ReplyResult           driver.Result
		ReplyError            error
		WantAffectedRows      int64
		WantLastInsertID      int64
		WantError             error
	}{
		{
			Name:       "test_conn_execcontext_update_balances",
			DriverName: "test_conn_execcontext_update_balances",
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
			Name:       "test_conn_execcontext_set_status",
			DriverName: "test_conn_execcontext_set_status",
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
			Name:       "test_conn_execcontext_set_status_middleware",
			DriverName: "test_conn_execcontext_set_status_middleware",
			ExecContextMiddleware: func(next ExecContextFunc) ExecContextFunc {
				return func(ctx context.Context, query string, namedArg []driver.NamedValue) (driver.Result, error) {
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
					return next(ctx, query, namedArg)
				}
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
			Name:       "test_conn_execcontext_set_status_middleware_chain",
			DriverName: "test_conn_execcontext_set_status_middleware_chain",
			ExecContextMiddleware: ExecContextMiddlewareChain(func(next ExecContextFunc) ExecContextFunc {
				return func(ctx context.Context, query string, namedArg []driver.NamedValue) (driver.Result, error) {
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
					return next(ctx, query, namedArg)
				}
			}, func(next ExecContextFunc) ExecContextFunc {
				return func(ctx context.Context, query string, namedArg []driver.NamedValue) (driver.Result, error) {
					for idx, arg := range namedArg {
						if arg.Name == "id" {
							arg.Value = arg.Value.(int64) + 1
							namedArg[idx] = arg
						}
					}
					return next(ctx, query, namedArg)
				}
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
			Name:       "test_conn_execcontext_error",
			DriverName: "test_conn_execcontext_error",
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
			WantError:        errors.New("test"),
		},
		{
			Name:       "test_conn_execcontext_set_status_middleware_error",
			DriverName: "test_conn_execcontext_set_status_middleware_error",
			ExecContextMiddleware: func(next ExecContextFunc) ExecContextFunc {
				return func(ctx context.Context, query string, namedArg []driver.NamedValue) (driver.Result, error) {
					return nil, errors.New("test")
				}
			},
			Query: "UPDATE users SET status = ? WHERE id = ?",
			Args: []interface{}{
				"paid",
				100,
			},
			WantError: errors.New("test"),
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
					ExecContextMiddleware: testCase.ExecContextMiddleware,
				},
			}
			sql.Register(testCase.DriverName, dri)

			db, err := sql.Open(testCase.DriverName, "foo")
			if err != nil {
				t.Fatal(err)
			}
			result, err := db.ExecContext(context.TODO(), testCase.Query, testCase.Args...)

			if testCase.WantError != nil {
				gotError := "<nil>"
				if err != nil {
					gotError = err.Error()
				}
				if testCase.WantError.Error() != gotError {
					t.Fatalf("want error %s, got %s", testCase.WantError, gotError)
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
