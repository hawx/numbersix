package numbersix

// A Triple has a subject, predicate and value.
type Triple struct {
	Subject   string
	Predicate string
	v         string
}

// Value will set the value to the pointer provided.
func (t Triple) Value(v interface{}) error {
	return unmarshal(t.v, &v)
}

// A Group contains all predicate-values for a subject.
type Group struct {
	Subject    string
	Properties map[string][]interface{}
}

// Grouped takes a list of triples and turns them in to groups. If the triples
// are not sorted by subject it will produce a Group for each set.
func Grouped(triples []Triple) (groups []Group) {
	var group Group

	for _, triple := range triples {
		if group.Subject != triple.Subject {
			if group.Subject != "" {
				groups = append(groups, group)
			}

			group = Group{
				Subject:    triple.Subject,
				Properties: map[string][]interface{}{},
			}
		}

		var value interface{}
		triple.Value(&value)
		group.Properties[triple.Predicate] = append(group.Properties[triple.Predicate], value)
	}

	if group.Subject != "" {
		groups = append(groups, group)
	}

	return
}
