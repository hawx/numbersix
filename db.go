package numbersix

import (
	"database/sql"
	"reflect"

	// register sqlite3 for database/sql
	_ "github.com/mattn/go-sqlite3"
)

// DB stores arbitrary triples, with the type of the value.
type DB struct {
	db *sql.DB
}

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

func (d *DB) Insert(subject, predicate string, value interface{}, more ...interface{}) error {
	if len(more) > 0 {
		return d.InsertMany(subject, predicate, append(more, value))
	}

	v, err := marshal(value)
	if err != nil {
		return err
	}

	_, err = d.db.Exec(`INSERT OR REPLACE INTO triples(subject, predicate, value) VALUES(?, ?, ?)`,
		subject,
		predicate,
		v)

	return err
}

func (d *DB) InsertMany(subject, predicate string, values interface{}) error {
	rv := reflect.ValueOf(values)
	if rv.Len() == 0 {
		return nil
	}

	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare(`INSERT OR REPLACE INTO triples(subject, predicate, value) VALUES(?, ?, ?)`)
	if err != nil {
		return err
	}

	for i := 0; i < rv.Len(); i++ {
		v, _ := marshal(rv.Index(i).Interface())
		_, err = stmt.Exec(subject, predicate, v)
	}

	if err != nil {
		terr := tx.Rollback()
		if terr != nil {
			return terr
		}
		return err
	}

	return tx.Commit()
}

// TODO: change this to take criteria and be called First?
func (d *DB) Get(subject, predicate string) (Triple, bool) {
	row := d.db.QueryRow(`SELECT subject, predicate, value FROM triples WHERE subject = ? AND predicate = ?`,
		subject,
		predicate)

	var triple Triple
	if err := row.Scan(&triple.Subject, &triple.Predicate, &triple.v); err != nil {
		return triple, false
	}

	return triple, true
}

func (d *DB) List(query QueryBuilder) (results []Triple, err error) {
	qs, args := query.Build()

	rows, err := d.db.Query(qs, args...)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var triple Triple
		if err = rows.Scan(&triple.Subject, &triple.Predicate, &triple.v); err != nil {
			return
		}
		results = append(results, triple)
	}

	return
}
