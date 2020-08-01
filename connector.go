package middledriver

import (
	"context"
	"database/sql/driver"
)

// Connector represents a driver in a fixed configuration and can create any number of equivalent Conns for use by multiple goroutines.
type Connector struct {
	name   string
	driver Driver
	target driver.Connector
}

// Connect implements Connector.
func (connector Connector) Connect(ctx context.Context) (driver.Conn, error) {
	if connector.target != nil {
		connTarget, err := connector.target.Connect(ctx)
		if err != nil {
			return nil, err
		}
		return newConn(connTarget, connector.driver, connector.driver.MiddlewareGroup.QueryContextMiddleware, connector.driver.MiddlewareGroup.ExecContextMiddleware), nil
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	connTarget, err := connector.driver.Target.Open(connector.name)
	if err != nil {
		return nil, err
	}
	return newConn(connTarget, connector.driver, connector.driver.MiddlewareGroup.QueryContextMiddleware, connector.driver.MiddlewareGroup.ExecContextMiddleware), nil
}

// Driver implements Connector.
func (connector Connector) Driver() driver.Driver {
	return connector.driver
}
