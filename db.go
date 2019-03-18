package numbersix

import (
	"database/sql"

	// register sqlite3 for database/sql
	_ "github.com/mattn/go-sqlite3"
)

// DB stores triples.
type DB struct {
	db   *sql.DB
	name string
}

// Open returns a new triple store DB writing to a sqlite database at the path
// given.
func Open(path string) (*DB, error) {
	sqlite, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}

	return For(sqlite, "triples")
}

// For returns a triple store wrapping the sql database table named.
func For(db *sql.DB, name string) (*DB, error) {
	return &DB{db: db, name: name}, migrate(db, name)
}

// Close the underlying sqlite database.
func (d *DB) Close() error {
	return d.db.Close()
}

func migrate(db *sql.DB, name string) error {
	_, err := db.Exec(`
    CREATE TABLE IF NOT EXISTS ` + name + ` (
      subject   TEXT,
      predicate TEXT,
      value     TEXT,
      PRIMARY KEY (subject, predicate, value)
    );
  `)

	return err
}
