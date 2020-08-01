package middledriver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/mattn/go-sqlite3"
)

func Example_sqlite3() {
	driver := Driver{
		Target: &sqlite3.SQLiteDriver{},
		MiddlewareGroup: MiddlewareGroup{
			QueryContextMiddleware: func(next QueryContextFunc) QueryContextFunc {
				return func(ctx context.Context, query string, namedArg []driver.NamedValue) (driver.Rows, error) {
					fmt.Printf("Query: %s, args: %+v\n", query, namedArg)
					return next(ctx, query, namedArg)
				}
			},
			ExecContextMiddleware: func(next ExecContextFunc) ExecContextFunc {
				return func(ctx context.Context, query string, namedArg []driver.NamedValue) (driver.Result, error) {
					fmt.Printf("Execute: %s, args: %+v\n", query, namedArg)
					return next(ctx, query, namedArg)
				}
			},
		},
	}

	sql.Register("sqlite3_wrap", driver)

	db, _ := sql.Open("sqlite3_wrap", ":memory:")

	tx, _ := db.BeginTx(context.Background(), nil)
	tx.ExecContext(context.Background(), `CREATE TABLE Persons (ID int, Name varchar(255), Address varchar(255), Age int)`)
	tx.ExecContext(context.Background(), `INSERT INTO Persons (ID, Name, Address, Age) Values (100, "Tom", "ShenZhen", 18)`)
	tx.Commit()

	var id, age int
	var name, address string
	db.QueryRowContext(context.Background(), "SELECT ID, Name, Address, Age FROM Persons WHERE ID = ? AND Name = ?", 100, "Tom").Scan(&id, &name, &address, &age)

	fmt.Printf("Result: ID: %d, Name: %s, Address: %s, Age: %d\n", id, name, address, age)

	// Output:
	// Execute: CREATE TABLE Persons (ID int, Name varchar(255), Address varchar(255), Age int), args: []
	// Execute: INSERT INTO Persons (ID, Name, Address, Age) Values (100, "Tom", "ShenZhen", 18), args: []
	// Query: SELECT ID, Name, Address, Age FROM Persons WHERE ID = ? AND Name = ?, args: [{Name: Ordinal:1 Value:100} {Name: Ordinal:2 Value:Tom}]
	// Result: ID: 100, Name: Tom, Address: ShenZhen, Age: 18
}
