package numbersix

import (
	"errors"
	"reflect"
)

// Set the value(s) for a subject and predicate. Only unique values are stored
// per (subject, predicate) combination, and insertion order is not retained.
func (d *DB) Set(subject, predicate string, value interface{}, more ...interface{}) error {
	if len(more) > 0 {
		return d.SetMany(subject, predicate, append(more, value))
	}

	v, err := marshal(value)
	if err != nil {
		return err
	}

	_, err = d.db.Exec("INSERT OR REPLACE INTO triples(subject, predicate, value) VALUES(?, ?, ?)",
		subject,
		predicate,
		v)

	return err
}

// SetMany is the same as Set, but takes a slice of values to set.
func (d *DB) SetMany(subject, predicate string, values interface{}) error {
	rv := reflect.ValueOf(values)
	if rv.Kind() != reflect.Slice {
		return errors.New("SetMany expected a slice of values")
	}
	if rv.Len() == 0 {
		return nil
	}

	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare("INSERT OR REPLACE INTO triples(subject, predicate, value) VALUES(?, ?, ?)")
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

// SetProperties is the same as Set, but takes a map of predicates and values to
// set.
func (d *DB) SetProperties(subject string, properties map[string][]interface{}) error {
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare("INSERT OR REPLACE INTO triples(subject, predicate, value) VALUES(?, ?, ?)")
	if err != nil {
		return err
	}

	for predicate, values := range properties {
		for _, value := range values {
			v, _ := marshal(value)
			_, err = stmt.Exec(subject, predicate, v)
		}
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
