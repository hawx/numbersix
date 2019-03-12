package numbersix

type QueryBuilder interface {
	Build() (string, []interface{})
}

type allQuery struct{}

func All() *allQuery {
	return &allQuery{}
}

func (q *allQuery) Build() (qs string, args []interface{}) {
	return `SELECT subject, predicate, value FROM triples ORDER BY subject, predicate`, []interface{}{}
}

type aboutQuery struct {
	subject string
}

func About(subject string) *aboutQuery {
	return &aboutQuery{subject: subject}
}

func (q *aboutQuery) Build() (qs string, args []interface{}) {
	return `SELECT subject, predicate, value FROM triples WHERE subject = ? ORDER BY predicate`, []interface{}{q.subject}
}

type whereClause struct{ predicate, value string }

type orderedQuery struct {
	predicate, value string
	ascending        bool
	limitCount       int
	wheres           []whereClause
}

func After(predicate string, value interface{}) *orderedQuery {
	v, _ := marshal(value)

	return &orderedQuery{
		predicate: predicate,
		value:     v,
		ascending: true,
	}
}

func Before(predicate string, value interface{}) *orderedQuery {
	v, _ := marshal(value)

	return &orderedQuery{
		predicate: predicate,
		value:     v,
	}
}

func (q *orderedQuery) Limit(count int) *orderedQuery {
	q.limitCount = count
	return q
}

func (q *orderedQuery) Where(predicate string, value interface{}) *orderedQuery {
	v, _ := marshal(value)

	q.wheres = append(q.wheres, whereClause{
		predicate: predicate,
		value:     v,
	})

	return q
}

func (q *orderedQuery) Build() (qs string, args []interface{}) {
	var subjects string
	if len(q.wheres) > 0 {
		subjects = "subjects(found) AS ( "

		for i, where := range q.wheres {
			if i > 0 {
				subjects += " INTERSECT "
			}
			subjects += "SELECT DISTINCT subject FROM triples WHERE predicate = ? AND value = ?"
			args = append(args, where.predicate, where.value)
		}

		subjects += " ), "
	}

	var orderedSubjects string
	{
		orderedSubjects = "ordered_subjects(found, ordering) AS ( SELECT DISTINCT subject, value FROM triples "
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
		`SELECT subject, predicate, value, ordering FROM triples
INNER JOIN ordered_subjects ON subject = ordered_subjects.found
ORDER BY ordering`
	if !q.ascending {
		qs += " DESC "
	}
	qs += `)`
	return
}
