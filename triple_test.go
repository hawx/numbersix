package numbersix

import (
	"testing"

	"hawx.me/code/assert"
)

func TestGrouped(t *testing.T) {
	assert := assert.New(t)

	m := func(value interface{}) string {
		v, _ := marshal(value)
		return v
	}

	triples := []Triple{
		{"a", "size", m(1)},
		{"a", "name", m("cool")},
		{"a", "tag", m("what")},
		{"a", "tag", m("test")},
		{"b", "size", m(4)},
		{"b", "name", m("bbbb")},
		{"c", "age", m(23)},
	}

	groups := Grouped(triples)

	if assert.Len(groups, 3) {
		a := groups[0]
		assert.Equal("a", a.Subject)
		assert.Equal(1, int(a.Properties["size"][0].(float64)))
		assert.Equal("cool", a.Properties["name"][0])
		assert.Equal("what", a.Properties["tag"][0])
		assert.Equal("test", a.Properties["tag"][1])

		b := groups[1]
		assert.Equal("b", b.Subject)
		assert.Equal(4, int(b.Properties["size"][0].(float64)))
		assert.Equal("bbbb", b.Properties["name"][0])

		c := groups[2]
		assert.Equal("c", c.Subject)
		assert.Equal(23, int(c.Properties["age"][0].(float64)))
	}
}
