package numbersix

import (
	"database/sql"
)

// List returns all triples that match the query provided.
func (d *DB) List(query Query) (results []Triple, err error) {
	qs, args := query.build(d.name)

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

// Any returns true if there exists a triple matching the query provided.
func (d *DB) Any(query AnyQuery) (ok bool, err error) {
	qs, args := query.buildAny(d.name)

	row := d.db.QueryRow(qs, args...)

	var i int
	if err = row.Scan(&i); err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}

		return false, err
	}

	return i == 1, nil
}

// Query defines conditions for triples that List should return.
type Query interface {
	build(name string) (string, []interface{})
}

// AnyQuery defines conditions for triples that Any should match.
type AnyQuery interface {
	buildAny(name string) (string, []interface{})
}

type AllQuery struct{}

// All is a query that returns all triples.
func All() *AllQuery {
	return &AllQuery{}
}

func (q *AllQuery) build(name string) (qs string, args []interface{}) {
	return "SELECT subject, predicate, value FROM " + name + " ORDER BY subject, predicate", []interface{}{}
}

type whereClause struct{ predicate, value string }

type AboutQuery struct {
	subject string
	wheres  []whereClause
}

// About is a query that returns all triples with a particular subject.
func About(subject string) *AboutQuery {
	return &AboutQuery{subject: subject}
}

// Where adds a condition to the query so that only triples for subjects that
// have the predicate and value are returned.
func (q *AboutQuery) Where(predicate string, value interface{}) *AboutQuery {
	v, _ := marshal(value)

	q.wheres = append(q.wheres, whereClause{
		predicate: predicate,
		value:     v,
	})

	return q
}

func (q *AboutQuery) build(name string) (qs string, args []interface{}) {
	if len(q.wheres) > 0 {
		subjects := "WITH subjects(found) AS ( "

		for i, where := range q.wheres {
			if i > 0 {
				subjects += " INTERSECT "
			}
			subjects += "SELECT DISTINCT subject FROM " + name + " WHERE predicate = ? AND value = ?"
			args = append(args, where.predicate, where.value)
		}

		subjects += " ) "

		return subjects +
			"SELECT subject, predicate, value FROM " + name +
			" INNER JOIN subjects ON subject = subjects.found" +
			" WHERE subject = ?", append(args, q.subject)
	}

	return "SELECT subject, predicate, value FROM " + name + " WHERE subject = ? ORDER BY predicate", []interface{}{q.subject}
}

func (q *AboutQuery) buildAny(name string) (qs string, args []interface{}) {
	if len(q.wheres) > 0 {
		subjects := "WITH subjects(found) AS ( "

		for i, where := range q.wheres {
			if i > 0 {
				subjects += " INTERSECT "
			}
			subjects += "SELECT DISTINCT subject FROM " + name + " WHERE predicate = ? AND value = ?"
			args = append(args, where.predicate, where.value)
		}

		subjects += " ) "

		return subjects +
			"SELECT 1 FROM " + name +
			" INNER JOIN subjects ON subject = subjects.found" +
			" WHERE subject = ?", append(args, q.subject)
	}

	return "SELECT 1 FROM " + name + " WHERE subject = ?", []interface{}{q.subject}
}

type WhereQuery struct {
	begins   whereClause
	wheres   []whereClause
	has      []string
	withouts []string
}

// Where is a query that returns all triples with a particular predicate-value.
func Where(predicate string, value interface{}) *WhereQuery {
	q := &WhereQuery{}

	return q.Where(predicate, value)
}

// Begins is a query that returns all triples with a particular predicate that
// begins with the value.
func Begins(predicate string, value interface{}) *WhereQuery {
	v, _ := marshal(value)

	q := &WhereQuery{
		begins: whereClause{
			predicate: predicate,
			value:     v,
		},
	}

	return q
}

// Where adds a condition to the query so that only triples for subjects that
// have the predicate and value are returned.
func (q *WhereQuery) Where(predicate string, value interface{}) *WhereQuery {
	v, _ := marshal(value)

	q.wheres = append(q.wheres, whereClause{
		predicate: predicate,
		value:     v,
	})

	return q
}

// Has adds a condition to the query so that only triples for subjects that have
// the predicate (with any value) are returned.
func (q *WhereQuery) Has(predicate string) *WhereQuery {
	q.has = append(q.has, predicate)

	return q
}

// Without adds a condition to the query so that only triples for subjects that
// do not have the predicate are returned.
func (q *WhereQuery) Without(predicate string) *WhereQuery {
	q.withouts = append(q.withouts, predicate)

	return q
}

func (q *WhereQuery) build(name string) (qs string, args []interface{}) {
	qs = "WITH subjects(found) AS ( "

	if len(q.wheres) > 0 {
		for i, where := range q.wheres {
			if i > 0 {
				qs += " INTERSECT "
			}
			qs += "SELECT DISTINCT subject FROM " + name + " WHERE predicate = ? AND value = ?"
			args = append(args, where.predicate, where.value)
		}
	} else {
		qs += "SELECT DISTINCT subject FROM " + name + " WHERE predicate = ? AND value LIKE ?"
		args = append(args, q.begins.predicate, q.begins.value[:len(q.begins.value)-1]+"%")
	}

	for _, has := range q.has {
		qs += " INTERSECT SELECT DISTINCT subject FROM " + name + " WHERE predicate = ?"
		args = append(args, has)
	}

	if len(q.withouts) > 0 {
		qs += " ), excluded_subjects(found) AS ( "

		for i, without := range q.withouts {
			if i > 0 {
				qs += " INTERSECT "
			}
			qs += "SELECT DISTINCT subject FROM " + name + " WHERE predicate = ?"
			args = append(args, without)
		}
	}

	qs += " ) SELECT subject, predicate, value FROM " + name + " INNER JOIN subjects ON subject = subjects.found"

	if len(q.withouts) > 0 {
		qs += " LEFT JOIN excluded_subjects ON subject = excluded_subjects.found WHERE excluded_subjects.found IS NULL"
	}

	return
}

type BoundOrderedQuery struct {
	predicate, value string
	ascending        bool
	limitCount       int
	wheres           []whereClause
	withouts         []string
}

// After is a query that returns triples for a subject having a triple with the
// predicate and a value after that provided. The returned triples will be
// ordered such that the subjects are ascending by the predicate's value. For
// example, if we had triples:
//
//    ("a", "name", "John")
//    ("a", "age", 20)
//    ("b", "name", "Jane")
//    ("b", "age", 24)
//    ("c", "name", "Kevin")
//    ("c", "age", 23)
//
// Then a query of After("age", 22), would give us triples:
//
//    ("c", "age", 23)
//    ("c", "name", "Kevin")
//    ("b", "age", 24)
//    ("b", "name", "Jane")
func After(predicate string, value interface{}) *BoundOrderedQuery {
	v, _ := marshal(value)

	return &BoundOrderedQuery{
		predicate: predicate,
		value:     v,
		ascending: true,
	}
}

// Before is like After, but the triples returned will have values less than the
// value given, and will be ordered descending on the predicate.
func Before(predicate string, value interface{}) *BoundOrderedQuery {
	v, _ := marshal(value)

	return &BoundOrderedQuery{
		predicate: predicate,
		value:     v,
	}
}

// Limit adds a condition to the query so that only triples for count subjects
// are returned.
func (q *BoundOrderedQuery) Limit(count int) *BoundOrderedQuery {
	q.limitCount = count
	return q
}

// Where adds a condition to the query so that only triples for subjects that
// have the predicate and value are returned.
func (q *BoundOrderedQuery) Where(predicate string, value interface{}) *BoundOrderedQuery {
	v, _ := marshal(value)

	q.wheres = append(q.wheres, whereClause{
		predicate: predicate,
		value:     v,
	})

	return q
}

// Without adds a condition to the query so that only triples for subjects that
// do not have the predicate are returned.
func (q *BoundOrderedQuery) Without(predicate string) *BoundOrderedQuery {
	q.withouts = append(q.withouts, predicate)

	return q
}

func (q *BoundOrderedQuery) build(name string) (qs string, args []interface{}) {
	var subjects string
	if len(q.wheres) > 0 {
		subjects = "subjects(found) AS ( "

		for i, where := range q.wheres {
			if i > 0 {
				subjects += " INTERSECT "
			}
			subjects += "SELECT DISTINCT subject FROM " + name + " WHERE predicate = ? AND value = ?"
			args = append(args, where.predicate, where.value)
		}

		subjects += " ), "
	}

	var excludedSubjects string
	if len(q.withouts) > 0 {
		excludedSubjects = "excluded_subjects(found) AS ( "

		for i, without := range q.withouts {
			if i > 0 {
				excludedSubjects += " INTERSECT "
			}
			excludedSubjects += "SELECT DISTINCT subject FROM " + name + " WHERE predicate = ?"
			args = append(args, without)
		}

		excludedSubjects += " ), "
	}

	var orderedSubjects string
	{
		orderedSubjects = "ordered_subjects(found, ordering) AS ( SELECT DISTINCT subject, value FROM " + name + " "
		if len(q.wheres) > 0 {
			orderedSubjects += "INNER JOIN subjects ON subject = subjects.found "
		}

		where := "WHERE"
		if len(q.withouts) > 0 {
			orderedSubjects += "LEFT JOIN excluded_subjects ON subject = excluded_subjects.found WHERE excluded_subjects.found IS NULL "
			where = "AND"
		}

		if q.ascending {
			orderedSubjects += where + " predicate = ? AND value > ? ORDER BY value "
		} else {
			orderedSubjects += where + " predicate = ? AND value < ? ORDER BY value DESC "
		}
		args = append(args, q.predicate, q.value)

		if q.limitCount > 0 {
			orderedSubjects += "LIMIT ? "
			args = append(args, q.limitCount)
		}

		orderedSubjects += ") "
	}

	qs = "SELECT subject, predicate, value FROM ( WITH " +
		subjects +
		excludedSubjects +
		orderedSubjects +
		`SELECT subject, predicate, value, ordering FROM ` + name + `
INNER JOIN ordered_subjects ON subject = ordered_subjects.found
ORDER BY ordering`
	if !q.ascending {
		qs += " DESC "
	}
	qs += ")"
	return
}

type OrderedQuery struct {
	predicate string
	ascending bool
	wheres    []whereClause
}

func Ascending(on string) *OrderedQuery {
	return &OrderedQuery{
		ascending: true,
		predicate: on,
	}
}

func Descending(on string) *OrderedQuery {
	return &OrderedQuery{
		predicate: on,
	}
}

// Where adds a condition to the query so that only triples for subjects that
// have the predicate and value are returned.
func (q *OrderedQuery) Where(predicate string, value interface{}) *OrderedQuery {
	v, _ := marshal(value)

	q.wheres = append(q.wheres, whereClause{
		predicate: predicate,
		value:     v,
	})

	return q
}

func (q *OrderedQuery) build(name string) (qs string, args []interface{}) {
	var subjects string
	if len(q.wheres) > 0 {
		subjects = "subjects(found) AS ( "

		for i, where := range q.wheres {
			if i > 0 {
				subjects += " INTERSECT "
			}
			subjects += "SELECT DISTINCT subject FROM " + name + " WHERE predicate = ? AND value = ?"
			args = append(args, where.predicate, where.value)
		}

		subjects += " ), "
	}

	var orderedSubjects string
	{
		orderedSubjects = "ordered_subjects(found, ordering) AS ( SELECT DISTINCT subject, value FROM " + name + " "
		if len(q.wheres) > 0 {
			orderedSubjects += "INNER JOIN subjects ON subject = subjects.found "
		}
		if q.ascending {
			orderedSubjects += "WHERE predicate = ? ORDER BY value "
		} else {
			orderedSubjects += "WHERE predicate = ? ORDER BY value DESC "
		}
		args = append(args, q.predicate)

		orderedSubjects += ") "
	}

	qs = "SELECT subject, predicate, value FROM ( WITH " +
		subjects +
		orderedSubjects +
		`SELECT subject, predicate, value, ordering FROM ` + name + `
INNER JOIN ordered_subjects ON subject = ordered_subjects.found
ORDER BY ordering`
	if !q.ascending {
		qs += " DESC "
	}
	qs += ")"
	return
}
