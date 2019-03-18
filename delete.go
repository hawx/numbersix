package numbersix

// DeleteValue removes the triple for the (subject, predicate, value) given. If
// none exist, then this does nothing.
func (d *DB) DeleteValue(subject, predicate string, value interface{}) error {
	v, err := marshal(value)
	if err != nil {
		return err
	}

	_, err = d.db.Exec("DELETE FROM "+d.name+" WHERE subject = ? AND predicate = ? AND value = ?",
		subject,
		predicate,
		v)

	return err
}

// DeletePredicate removes all triples with the subject and predicate given. If
// none exist, then this does nothing.
func (d *DB) DeletePredicate(subject, predicate string) error {
	_, err := d.db.Exec("DELETE FROM "+d.name+" WHERE subject = ? AND predicate = ?",
		subject,
		predicate)

	return err
}

// DeleteSubject removes all triples for the subject given. If none exist, then
// this does nothing.
func (d *DB) DeleteSubject(subject string) error {
	_, err := d.db.Exec("DELETE FROM "+d.name+" WHERE subject = ?",
		subject)

	return err
}
