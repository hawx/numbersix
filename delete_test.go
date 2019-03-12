package numbersix

import (
	"testing"
	"time"

	"hawx.me/code/assert"
)

func TestDelete(t *testing.T) {
	assert := assert.New(t)

	db, _ := Open("file::memory:")
	assert.Nil(db.Insert("abc", "label", "test", 1, time.Date(2019, time.January, 1, 12, 0, 0, 0, time.UTC), true))

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

	assert.Nil(db.Delete("abc", "label", 1))
	assert.Nil(db.Delete("abc", "label", time.Date(2019, time.January, 1, 12, 0, 0, 0, time.UTC)))

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

func TestDeleteSubject(t *testing.T) {
	assert := assert.New(t)

	db, _ := Open("file::memory:")
	assert.Nil(db.Insert("abc", "label", "test"))
	assert.Nil(db.Insert("def", "label", "test"))
	assert.Nil(db.Insert("ghi", "label", "test"))

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
