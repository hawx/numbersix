package numbersix

import (
	"testing"
	"time"

	"hawx.me/code/assert"
)

func TestGet(t *testing.T) {
	assert := assert.New(t)

	db, _ := Open("file::memory:")
	assert.Nil(db.Insert("john@doe.com", "age", 25))
	assert.Nil(db.Insert("jane@doe.com", "age", 23))

	_, ok := db.Get("what", "age")
	assert.False(ok)
	_, ok = db.Get("john@doe.com", "what")
	assert.False(ok)

	triple, ok := db.Get("john@doe.com", "age")
	assert.True(ok)

	var age int
	assert.Nil(triple.Value(&age))
	assert.Equal(25, age)
}

func TestList(t *testing.T) {
	assert := assert.New(t)

	db, _ := Open("file::memory:")
	assert.Nil(db.Insert("john@doe.com", "age", 25))
	assert.Nil(db.Insert("jane@doe.com", "age", 23))

	type Person struct {
		Email string
		Age   int
	}

	triples, err := db.List(All())
	assert.Nil(err)

	var people []Person
	for _, triple := range triples {
		var person Person
		person.Email = triple.Subject
		assert.Nil(triple.Value(&person.Age))
		people = append(people, person)
	}

	if assert.Len(people, 2) {
		assert.Equal("jane@doe.com", people[0].Email)
		assert.Equal(23, people[0].Age)

		assert.Equal("john@doe.com", people[1].Email)
		assert.Equal(25, people[1].Age)
	}
}

func TestInsertVariadic(t *testing.T) {
	assert := assert.New(t)

	db, _ := Open("file::memory:")
	assert.Nil(db.Insert("object-1", "size", 5, 2, "what", 4))

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

func TestMicroformat(t *testing.T) {
	assert := assert.New(t)

	type Microformat struct {
		Type       []string
		Properties map[string][]interface{}
	}

	microformat := Microformat{
		Type: []string{"h-entry"},
		Properties: map[string][]interface{}{
			"content":  {"test content"},
			"category": {"cool", "tag"},
		},
	}

	postID := "some-uuid"

	db, _ := Open("file::memory:")

	assert.Nil(db.InsertMany(postID, "type", microformat.Type))
	for key, value := range microformat.Properties {
		assert.Nil(db.InsertMany(postID, "property."+key, value))
	}

	triples, err := db.List(About(postID))
	assert.Nil(err)

	properties := map[string][]interface{}{}
	for _, triple := range triples {
		key := triple.Predicate

		var value interface{}
		assert.Nil(triple.Value(&value))

		properties[key] = append(properties[key], value)
	}

	assert.Equal("h-entry", properties["type"][0])
	assert.Equal("test content", properties["property.content"][0])
	assert.Equal("cool", properties["property.category"][0])
	assert.Equal("tag", properties["property.category"][1])
}

func TestPossibleUsage(t *testing.T) {
	assert := assert.New(t)

	db, _ := Open("file::memory:")

	insertPost := func(postID, title string, createdAt time.Time) {
		assert.Nil(db.Insert(postID, "title", title))
		assert.Nil(db.Insert(postID, "createdAt", createdAt))
	}

	insertPost("1", "My first post", time.Date(2019, time.January, 1, 12, 0, 0, 0, time.UTC))
	insertPost("2", "My third post", time.Date(2019, time.March, 1, 12, 0, 0, 0, time.UTC))
	insertPost("3", "My second post", time.Date(2019, time.February, 1, 12, 0, 0, 0, time.UTC))
	insertPost("4", "My final post", time.Date(2019, time.April, 1, 12, 0, 0, 0, time.UTC))

	type Post struct {
		Title     string
		CreatedAt time.Time
	}

	triples, err := db.List(
		After("createdAt", time.Date(2019, time.January, 5, 12, 0, 0, 0, time.UTC)).Limit(2),
	)
	assert.Nil(err)

	posts := map[string]Post{}
	for _, triple := range triples {
		post, ok := posts[triple.Subject]
		if !ok {
			post = Post{}
		}

		switch triple.Predicate {
		case "title":
			assert.Nil(triple.Value(&post.Title))
		case "createdAt":
			assert.Nil(triple.Value(&post.CreatedAt))
		}

		posts[triple.Subject] = post
	}

	_, ok := posts["1"]
	assert.False(ok)
	_, ok = posts["4"]
	assert.False(ok)

	second, ok := posts["3"]
	assert.True(ok)
	assert.Equal("My second post", second.Title)
	assert.Equal(time.Date(2019, time.February, 1, 12, 0, 0, 0, time.UTC), second.CreatedAt)

	third, ok := posts["2"]
	assert.True(ok)
	assert.Equal("My third post", third.Title)
	assert.Equal(time.Date(2019, time.March, 1, 12, 0, 0, 0, time.UTC), third.CreatedAt)
}
