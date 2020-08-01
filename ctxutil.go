package middledriver

import (
	"database/sql/driver"
	"errors"
)

func namedValueToValue(named []driver.NamedValue) ([]driver.Value, error) {
	dargs := make([]driver.Value, len(named))
	for n, param := range named {
		if len(param.Name) > 0 {
			return nil, errors.New("not support the use of Named Parameters")
		}
		dargs[n] = param.Value
	}
	return dargs, nil
}
