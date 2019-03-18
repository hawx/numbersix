package numbersix

import (
	"database/sql"
	"testing"
	"time"

	"hawx.me/code/assert"
)

func TestFor(t *testing.T) {
	assert := assert.New(t)

	sqlite, _ := sql.Open("sqlite3", "file::memory:")

	a, err := For(sqlite, "a")
	assert.Nil(err)

	b, err := For(sqlite, "b")
	assert.Nil(err)

	assert.Nil(a.Set("x", "y", "z"))
	assert.Nil(b.Set("x", "y", "q"))

	atriples, err := a.List(About("x"))
	assert.Nil(err)
	btriples, err := b.List(About("x"))
	assert.Nil(err)

	var avalue string
	atriples[0].Value(&avalue)
	assert.Equal("z", avalue)

	var bvalue string
	btriples[0].Value(&bvalue)
	assert.Equal("q", bvalue)
}

func insertMap(db *DB, id string, properties map[string][]interface{}) error {
	for key, value := range properties {
		if err := db.SetMany(id, key, value); err != nil {
			return err
		}
	}

	return nil
}

func triplesToMap(triples []Triple) (map[string][]interface{}, error) {
	properties := map[string][]interface{}{}
	for _, triple := range triples {
		var value interface{}
		if err := triple.Value(&value); err != nil {
			return properties, err
		}

		properties[triple.Predicate] = append(properties[triple.Predicate], value)
	}

	return properties, nil
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

	assert.Nil(db.SetMany(postID, "type", microformat.Type))
	assert.Nil(insertMap(db, postID, microformat.Properties))

	triples, err := db.List(About(postID))
	assert.Nil(err)

	properties, err := triplesToMap(triples)
	assert.Nil(err)

	assert.Equal("h-entry", properties["type"][0])
	assert.Equal("test content", properties["content"][0])
	assert.Equal("cool", properties["category"][0])
	assert.Equal("tag", properties["category"][1])
}

func TestPossibleUsage(t *testing.T) {
	assert := assert.New(t)

	db, _ := Open("file::memory:")

	insertPost := func(postID, title string, createdAt time.Time) {
		assert.Nil(db.Set(postID, "title", title))
		assert.Nil(db.Set(postID, "createdAt", createdAt))
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
