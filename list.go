package numbersix

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

// Query defines conditions for triples that List should return.
type Query interface {
	build(name string) (string, []interface{})
}

type AllQuery struct{}

// All is a query that returns all triples.
func All() *AllQuery {
	return &AllQuery{}
}

func (q *AllQuery) build(name string) (qs string, args []interface{}) {
	return "SELECT subject, predicate, value FROM " + name + " ORDER BY subject, predicate", []interface{}{}
}

type AboutQuery struct {
	subject string
}

// About is a query that returns all triples with a particular subject.
func About(subject string) *AboutQuery {
	return &AboutQuery{subject: subject}
}

func (q *AboutQuery) build(name string) (qs string, args []interface{}) {
	return "SELECT subject, predicate, value FROM " + name + " WHERE subject = ? ORDER BY predicate", []interface{}{q.subject}
}

type whereClause struct{ predicate, value string }

type BoundOrderedQuery struct {
	predicate, value string
	ascending        bool
	limitCount       int
	wheres           []whereClause
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

	var orderedSubjects string
	{
		orderedSubjects = "ordered_subjects(found, ordering) AS ( SELECT DISTINCT subject, value FROM " + name + " "
		if len(q.wheres) > 0 {
			orderedSubjects += "INNER JOIN subjects ON subject = subjects.found "
		}
		if q.ascending {
			orderedSubjects += "WHERE predicate = ? AND value > ? ORDER BY value "
		} else {
			orderedSubjects += "WHERE predicate = ? AND value < ? ORDER BY value DESC "
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
