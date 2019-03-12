package numbersix

type Triple struct {
	Subject   string
	Predicate string
	v         string
}

func (t Triple) Value(v interface{}) error {
	return unmarshal(t.v, &v)
}
