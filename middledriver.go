package middledriver

import (
	"context"
	"database/sql/driver"
)

// QueryContextFunc is a function that handle query from conntions.
type QueryContextFunc func(ctx context.Context, query string, namedArg []driver.NamedValue) (driver.Rows, error)

// ExecContextFunc is a function that handle execute from conntions.
type ExecContextFunc func(ctx context.Context, query string, namedArg []driver.NamedValue) (driver.Result, error)

// StmtQueryContextFunc is a function that handle execute from statement.
type StmtQueryContextFunc func(ctx context.Context, namedArg []driver.NamedValue) (driver.Rows, error)

// StmtExecContextFunc is a function that handle execute from statement.
type StmtExecContextFunc func(ctx context.Context, namedArg []driver.NamedValue) (driver.Result, error)

// QueryContextMiddleware is a function which receives an QueryContextFunc and returns another QueryContextFunc.
type QueryContextMiddleware func(next QueryContextFunc) QueryContextFunc

// ExecContextMiddleware is a function which receives an ExecContextFunc and returns another ExecContextFunc.
type ExecContextMiddleware func(next ExecContextFunc) ExecContextFunc

// StmtQueryContextMiddleware is a function which receives an StmtQueryContextFunc and returns another StmtQueryContextFunc.
type StmtQueryContextMiddleware func(next StmtQueryContextFunc) StmtQueryContextFunc

// StmtExecContextMiddleware is a function which receives an StmtExecContextFunc and returns another StmtExecContextFunc.
type StmtExecContextMiddleware func(next StmtExecContextFunc) StmtExecContextFunc

// NewStmtQueryContextMiddleware create a StmtQueryContextMiddleware base on a query statement.
type NewStmtQueryContextMiddleware func(query string) (StmtQueryContextMiddleware, error)

// NewStmtExecContextMiddleware create a StmtExecContextMiddleware base on a query statement.
type NewStmtExecContextMiddleware func(query string) (StmtExecContextMiddleware, error)

// MiddlewareGroup is a collection of middleware.
type MiddlewareGroup struct {
	QueryContextMiddleware QueryContextMiddleware

	ExecContextMiddleware ExecContextMiddleware

	NewStmtExecContextMiddleware NewStmtExecContextMiddleware

	NewStmtQueryContextMiddleware NewStmtQueryContextMiddleware
}

// QueryContextMiddlewareChain creates a single QueryContextMiddleware out of a chain of many QueryContextMiddlewares.
func QueryContextMiddlewareChain(middlewares ...QueryContextMiddleware) QueryContextMiddleware {
	return func(next QueryContextFunc) QueryContextFunc {
		for idx := len(middlewares) - 1; idx >= 0; idx-- {
			next = middlewares[idx](next)
		}
		return func(ctx context.Context, query string, namedArg []driver.NamedValue) (driver.Rows, error) {
			return next(ctx, query, namedArg)
		}
	}
}

// ExecContextMiddlewareChain creates a single ExecContextMiddleware out of a chain of many ExecContextMiddlewares.
func ExecContextMiddlewareChain(middlewares ...ExecContextMiddleware) ExecContextMiddleware {
	return func(next ExecContextFunc) ExecContextFunc {
		for idx := len(middlewares) - 1; idx >= 0; idx-- {
			next = middlewares[idx](next)
		}
		return func(ctx context.Context, query string, namedArg []driver.NamedValue) (driver.Result, error) {
			return next(ctx, query, namedArg)
		}
	}
}

// NewStmtQueryContextMiddlewareChain creates a single NewStmtQueryContextMiddleware out of a chain of many NewStmtQueryContextMiddlewares.
func NewStmtQueryContextMiddlewareChain(newMiddlewares ...NewStmtQueryContextMiddleware) NewStmtQueryContextMiddleware {
	return func(query string) (StmtQueryContextMiddleware, error) {
		middlewares := make([]StmtQueryContextMiddleware, len(newMiddlewares))
		for idx := len(newMiddlewares) - 1; idx >= 0; idx-- {
			var err error
			middlewares[idx], err = newMiddlewares[idx](query)
			if err != nil {
				return nil, err
			}
		}
		return func(next StmtQueryContextFunc) StmtQueryContextFunc {
			for idx := len(middlewares) - 1; idx >= 0; idx-- {
				next = middlewares[idx](next)
			}
			return func(ctx context.Context, namedArg []driver.NamedValue) (driver.Rows, error) {
				return next(ctx, namedArg)
			}
		}, nil
	}
}

// NewStmtExecContextMiddlewareChain creates a single NewStmtExecContextMiddleware out of a chain of many NewStmtExecContextMiddleware.
func NewStmtExecContextMiddlewareChain(newMiddlewares ...NewStmtExecContextMiddleware) NewStmtExecContextMiddleware {
	return func(query string) (StmtExecContextMiddleware, error) {
		middlewares := make([]StmtExecContextMiddleware, len(newMiddlewares))
		for idx := len(newMiddlewares) - 1; idx >= 0; idx-- {
			var err error
			middlewares[idx], err = newMiddlewares[idx](query)
			if err != nil {
				return nil, err
			}
		}
		return func(next StmtExecContextFunc) StmtExecContextFunc {
			for idx := len(middlewares) - 1; idx >= 0; idx-- {
				next = middlewares[idx](next)
			}
			return func(ctx context.Context, namedArg []driver.NamedValue) (driver.Result, error) {
				return next(ctx, namedArg)
			}
		}, nil
	}
}
