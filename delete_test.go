package numbersix

import (
	"testing"
	"time"

	"hawx.me/code/assert"
)

func TestDeleteValue(t *testing.T) {
	assert := assert.New(t)

	db, _ := Open("file::memory:")
	assert.Nil(db.Set("abc", "label", "test", 1, time.Date(2019, time.January, 1, 12, 0, 0, 0, time.UTC), true))

	triples, err := db.List(All())
	assert.Nil(err)

	if assert.Len(triples, 4) {
		var t time.Time
		assert.Nil(triples[0].Value(&t))
		assert.Equal(time.Date(2019, time.January, 1, 12, 0, 0, 0, time.UTC), t)

		var s string
		assert.Nil(triples[1].Value(&s))
		assert.Equal("test", s)

		var i int
		assert.Nil(triples[2].Value(&i))
		assert.Equal(1, i)

		var b bool
		assert.Nil(triples[3].Value(&b))
		assert.Equal(true, b)
	}

	assert.Nil(db.DeleteValue("abc", "label", 1))
	assert.Nil(db.DeleteValue("abc", "label", time.Date(2019, time.January, 1, 12, 0, 0, 0, time.UTC)))

	triples, err = db.List(All())
	assert.Nil(err)

	if assert.Len(triples, 2) {
		var s string
		assert.Nil(triples[0].Value(&s))
		assert.Equal("test", s)

		var b bool
		assert.Nil(triples[1].Value(&b))
		assert.Equal(true, b)
	}
}

func TestDeletePredicate(t *testing.T) {
	assert := assert.New(t)

	db, _ := Open("file::memory:")
	assert.Nil(db.Set("abc", "label", "test"))
	assert.Nil(db.Set("abc", "tag", "test"))
	assert.Nil(db.Set("abc", "tag", "other"))
	assert.Nil(db.Set("def", "label", "test"))
	assert.Nil(db.Set("def", "tag", "test"))

	triples, err := db.List(All())
	assert.Nil(err)

	if assert.Len(triples, 5) {
		assert.Equal("abc", triples[0].Subject)
		assert.Equal("label", triples[0].Predicate)
		assert.Equal("abc", triples[1].Subject)
		assert.Equal("tag", triples[1].Predicate)
		assert.Equal("abc", triples[2].Subject)
		assert.Equal("tag", triples[2].Predicate)

		assert.Equal("def", triples[3].Subject)
		assert.Equal("label", triples[3].Predicate)
		assert.Equal("def", triples[4].Subject)
		assert.Equal("tag", triples[4].Predicate)
	}

	assert.Nil(db.DeletePredicate("abc", "tag"))

	triples, err = db.List(All())
	assert.Nil(err)

	if assert.Len(triples, 3) {
		assert.Equal("abc", triples[0].Subject)
		assert.Equal("label", triples[0].Predicate)

		assert.Equal("def", triples[1].Subject)
		assert.Equal("label", triples[1].Predicate)
		assert.Equal("def", triples[2].Subject)
		assert.Equal("tag", triples[2].Predicate)
	}
}

func TestDeleteSubject(t *testing.T) {
	assert := assert.New(t)

	db, _ := Open("file::memory:")
	assert.Nil(db.Set("abc", "label", "test"))
	assert.Nil(db.Set("def", "label", "test"))
	assert.Nil(db.Set("ghi", "label", "test"))

	triples, err := db.List(All())
	assert.Nil(err)

	if assert.Len(triples, 3) {
		assert.Equal("abc", triples[0].Subject)
		assert.Equal("def", triples[1].Subject)
		assert.Equal("ghi", triples[2].Subject)
	}

	assert.Nil(db.DeleteSubject("def"))
	assert.Nil(db.DeleteSubject("abc"))

	triples, err = db.List(All())
	assert.Nil(err)

	if assert.Len(triples, 1) {
		assert.Equal("ghi", triples[0].Subject)
	}
}
