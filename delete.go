package numbersix

func (d *DB) Delete(subject, predicate string, value interface{}) error {
	v, err := marshal(value)
	if err != nil {
		return err
	}

	_, err = d.db.Exec(`DELETE FROM triples WHERE subject = ? AND predicate = ? AND value = ?`,
		subject,
		predicate,
		v)

	return err
}

func (d *DB) DeleteSubject(subject string) error {
	_, err := d.db.Exec(`DELETE FROM triples WHERE subject = ?`,
		subject)

	return err
}
