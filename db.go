package numbersix

import (
	"database/sql"

	// register sqlite3 for database/sql
	_ "github.com/mattn/go-sqlite3"
)

// DB stores triples.
type DB struct {
	db *sql.DB
}

// Open returns a new triple store DB writing to a sqlite database at the path
// given.
func Open(path string) (*DB, error) {
	sqlite, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}

	return &DB{db: sqlite}, migrate(sqlite)
}

func migrate(db *sql.DB) error {
	_, err := db.Exec(`
    CREATE TABLE IF NOT EXISTS triples (
      subject   TEXT,
      predicate TEXT,
      value     TEXT,
      PRIMARY KEY (subject, predicate, value)
    );
  `)

	return err
}
