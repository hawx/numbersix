package numbersix

import (
	"testing"
	"time"

	"hawx.me/code/assert"
)

func BenchmarkSimpleSqliteSet(b *testing.B) {
	db, _ := Open("file::memory:")

	for i := 0; i < b.N; i++ {
		db.db.Exec("INSERT OR REPLACE INTO triples(subject, predicate, value) VALUES('a', 'b', 'c')")
	}
}

func BenchmarkSet(b *testing.B) {
	db, _ := Open("file::memory:")

	for i := 0; i < b.N; i++ {
		db.Set("a", "b", "c")
	}
}

func TestSet(t *testing.T) {
	db, _ := Open("file::memory:")

	t.Run("string", func(t *testing.T) {
		assert := assert.New(t)

		assert.Nil(db.Set("string", "value", "hey"))

		triples, err := db.List(About("string"))
		assert.Nil(err)

		if assert.Len(triples, 1) {
			var v string
			assert.Nil(triples[0].Value(&v))
			assert.Equal("hey", v)
		}
	})

	t.Run("int", func(t *testing.T) {
		assert := assert.New(t)

		assert.Nil(db.Set("int", "value", 2))

		triples, err := db.List(About("int"))
		assert.Nil(err)

		if assert.Len(triples, 1) {
			var v int
			assert.Nil(triples[0].Value(&v))
			assert.Equal(2, v)
		}
	})

	t.Run("float", func(t *testing.T) {
		assert := assert.New(t)

		assert.Nil(db.Set("float", "value", 2.5))

		triples, err := db.List(About("float"))
		assert.Nil(err)

		if assert.Len(triples, 1) {
			var v float64
			assert.Nil(triples[0].Value(&v))
			assert.Equal(2.5, v)
		}
	})

	t.Run("bool", func(t *testing.T) {
		assert := assert.New(t)

		assert.Nil(db.Set("bool", "value", true))

		triples, err := db.List(About("bool"))
		assert.Nil(err)

		if assert.Len(triples, 1) {
			var v bool
			assert.Nil(triples[0].Value(&v))
			assert.Equal(true, v)
		}
	})

	t.Run("time.Time", func(t *testing.T) {
		assert := assert.New(t)

		now := time.Now().UTC()
		assert.Nil(db.Set("time.Time", "value", now))

		triples, err := db.List(About("time.Time"))
		assert.Nil(err)

		if assert.Len(triples, 1) {
			var v time.Time
			assert.Nil(triples[0].Value(&v))
			assert.Equal(now, v)
		}
	})

	t.Run("[]string", func(t *testing.T) {
		assert := assert.New(t)

		assert.Nil(db.Set("[]string", "value", []string{"z", "b", "c"}))

		triples, err := db.List(About("[]string"))
		assert.Nil(err)

		if assert.Len(triples, 1) {
			var v []string
			assert.Nil(triples[0].Value(&v))
			assert.Equal([]string{"z", "b", "c"}, v)
		}
	})
}

func TestSetVariadic(t *testing.T) {
	assert := assert.New(t)

	db, _ := Open("file::memory:")
	assert.Nil(db.Set("object-1", "size", 5, 2, "what", 4))

	triples, err := db.List(About("object-1"))
	assert.Nil(err)

	if assert.Len(triples, 4) {
		var s string
		assert.Nil(triples[0].Value(&s))
		assert.Equal("what", s)

		var i int
		assert.Nil(triples[1].Value(&i))
		assert.Equal(2, i)
		assert.Nil(triples[2].Value(&i))
		assert.Equal(4, i)
		assert.Nil(triples[3].Value(&i))
		assert.Equal(5, i)
	}
}

func TestSetMany(t *testing.T) {
	assert := assert.New(t)

	db, _ := Open("file::memory:")
	assert.Nil(db.SetMany("object-1", "size", []int{5, 2, 4}))

	triples, err := db.List(About("object-1"))
	assert.Nil(err)

	if assert.Len(triples, 3) {
		var i int
		assert.Nil(triples[0].Value(&i))
		assert.Equal(2, i)
		assert.Nil(triples[1].Value(&i))
		assert.Equal(4, i)
		assert.Nil(triples[2].Value(&i))
		assert.Equal(5, i)
	}
}

func TestSetManyWithNonSlice(t *testing.T) {
	assert := assert.New(t)

	db, _ := Open("file::memory:")
	assert.NotNil(db.SetMany("object-1", "size", 1))
	assert.NotNil(db.SetMany("object-1", "name", "hey"))
	assert.NotNil(db.SetMany("object-1", "props", map[string]string{"a": "b"}))
}

func TestSetProperties(t *testing.T) {
	assert := assert.New(t)

	db, _ := Open("file::memory:")

	assert.Nil(db.SetProperties("thing", map[string][]interface{}{
		"size": {1},
		"name": {"hey"},
		"tags": {"cool", "test"},
	}))

	triples, err := db.List(All())
	assert.Nil(err)

	if assert.Len(triples, 4) {
		var name string
		assert.Equal("thing", triples[0].Subject)
		assert.Equal("name", triples[0].Predicate)
		assert.Nil(triples[0].Value(&name))
		assert.Equal("hey", name)

		var size int
		assert.Equal("thing", triples[1].Subject)
		assert.Equal("size", triples[1].Predicate)
		assert.Nil(triples[1].Value(&size))
		assert.Equal(1, size)

		var tag string
		assert.Equal("thing", triples[2].Subject)
		assert.Equal("tags", triples[2].Predicate)
		assert.Nil(triples[2].Value(&tag))
		assert.Equal("cool", tag)

		assert.Equal("thing", triples[3].Subject)
		assert.Equal("tags", triples[3].Predicate)
		assert.Nil(triples[3].Value(&tag))
		assert.Equal("test", tag)
	}
}
