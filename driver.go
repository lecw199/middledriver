package middledriver

import (
	"database/sql/driver"
	"errors"
)

// Driver is the interface that must be implemented by a database driver.
type Driver struct {
	Target driver.Driver

	MiddlewareGroup MiddlewareGroup
}

// Open implements Driver.
func (dri Driver) Open(name string) (driver.Conn, error) {
	return nil, errors.New("Please update Go to 1.10+ version")
}

// OpenConnector implements DriverContext.
func (dri Driver) OpenConnector(name string) (driver.Connector, error) {
	driverConnext, ok := dri.Target.(driver.DriverContext)
	if ok {
		conntor, err := driverConnext.OpenConnector(name)
		if err != nil {
			return nil, err
		}
		return Connector{
			driver: dri,
			target: conntor,
		}, nil
	}

	return Connector{
		driver: dri,
	}, nil
}
