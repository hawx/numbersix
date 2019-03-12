package numbersix

import (
	"testing"
	"time"

	"hawx.me/code/assert"
)

type pair struct{ s, p string }

func assertTriples(t *testing.T, triples []Triple, pairs []pair) bool {
	if !assert.Len(t, triples, len(pairs)) {
		return false
	}

	for i := range triples {
		triple := triples[i]
		pair := pairs[i]

		if !assert.Equal(t, pair.s, triple.Subject) || !assert.Equal(t, pair.p, triple.Predicate) {
			t.Logf("failed at %v\n", i)
			return false
		}
	}

	return true
}

func TestQuery(t *testing.T) {
	assert := assert.New(t)

	db, _ := Open("file::memory:")

	db.Insert("1", "name", "John")
	db.Insert("1", "age", 25)
	db.Insert("2", "name", "Jane")
	db.Insert("2", "age", 23)

	triples, err := db.List(All())
	assert.Nil(err)
	assert.Len(triples, 4)

	assertTriples(t, triples, []pair{
		{"1", "age"},
		{"1", "name"},
		{"2", "age"},
		{"2", "name"},
	})
}

func TestQueryAbout(t *testing.T) {
	assert := assert.New(t)

	db, _ := Open("file::memory:")

	db.Insert("1", "name", "John")
	db.Insert("1", "age", 25)
	db.Insert("2", "name", "Jane")
	db.Insert("2", "age", 23)

	triples, err := db.List(About("1"))
	assert.Nil(err)
	assert.Len(triples, 2)

	assertTriples(t, triples, []pair{
		{"1", "age"},
		{"1", "name"},
	})
}

func TestOrdered(t *testing.T) {
	db, _ := Open("file::memory:")

	db.Insert("2", "age", 25)
	db.Insert("2", "tag", "cool")

	db.Insert("4", "age", 19)

	db.Insert("8", "age", 18)
	db.Insert("8", "tag", "uncool")

	db.Insert("9", "age", 17)
	db.Insert("9", "tag", "cool")
	db.Insert("9", "tag", "cooler")

	db.Insert("7", "age", 22)
	db.Insert("7", "tag", "cool")

	db.Insert("3", "age", 21)
	db.Insert("3", "tag", "uncool")

	db.Insert("1", "age", 24)

	db.Insert("5", "age", 23)

	db.Insert("6", "age", 20)
	db.Insert("6", "tag", "who")
	db.Insert("6", "tag", "cooler")
	db.Insert("6", "tag", "uncool")

	t.Run("After", func(t *testing.T) {
		triples, err := db.List(After("age", 22))
		assert.Nil(t, err)

		assertTriples(t, triples, []pair{
			{"5", "age"}, // 23
			{"1", "age"}, // 24
			{"2", "age"}, // 25
			{"2", "tag"},
		})
	})

	t.Run("After with Where", func(t *testing.T) {
		triples, err := db.List(After("age", 19).Where("tag", "cool"))
		assert.Nil(t, err)

		assertTriples(t, triples, []pair{
			{"7", "age"}, // 22
			{"7", "tag"},
			{"2", "age"}, // 25
			{"2", "tag"},
		})
	})

	t.Run("After with Wheres", func(t *testing.T) {
		triples, err := db.List(After("age", 11).Where("tag", "cool").Where("tag", "cooler"))
		assert.Nil(t, err)

		assertTriples(t, triples, []pair{
			{"9", "age"}, // 17
			{"9", "tag"},
			{"9", "tag"},
		})
	})

	t.Run("After with Where and Limit", func(t *testing.T) {
		triples, err := db.List(After("age", 19).Where("tag", "cool").Limit(1))
		assert.Nil(t, err)

		assertTriples(t, triples, []pair{
			{"7", "age"}, // 22
			{"7", "tag"},
		})
	})

	t.Run("After with Limit", func(t *testing.T) {
		triples, err := db.List(After("age", 20).Limit(3))
		assert.Nil(t, err)

		assertTriples(t, triples, []pair{
			{"3", "age"}, // 21
			{"3", "tag"},
			{"7", "age"}, // 22
			{"7", "tag"},
			{"5", "age"}, // 23
		})
	})

	t.Run("Before", func(t *testing.T) {
		triples, err := db.List(Before("age", 20))
		assert.Nil(t, err)

		assertTriples(t, triples, []pair{
			{"4", "age"}, // 19
			{"8", "age"}, // 18
			{"8", "tag"},
			{"9", "age"}, // 17
			{"9", "tag"},
			{"9", "tag"},
		})
	})

	t.Run("Before with Limit", func(t *testing.T) {
		triples, err := db.List(Before("age", 22).Limit(3))
		assert.Nil(t, err)

		assertTriples(t, triples, []pair{
			{"3", "age"}, // 21
			{"3", "tag"},
			{"6", "age"}, // 20
			{"6", "tag"},
			{"6", "tag"},
			{"6", "tag"},
			{"4", "age"}, // 19
		})
	})
}

func TestAfterWhereLimit(t *testing.T) {
	assert := assert.New(t)

	db, _ := Open("file::memory:")

	db.Insert("0", "title", "A null post")
	db.Insert("0", "tag", "good")
	db.Insert("0", "published", time.Date(2019, time.January, 3, 12, 0, 0, 0, time.UTC))
	db.Insert("0", "content", "<nope>")

	// out of order
	db.Insert("1", "title", "A post")
	db.Insert("1", "tag", "good")
	db.Insert("1", "tag", "other")
	db.Insert("1", "published", time.Date(2019, time.January, 7, 12, 0, 0, 0, time.UTC))
	db.Insert("1", "content", "Hey this is a ...")

	db.Insert("2", "title", "Another post")
	db.Insert("2", "tag", "good")
	db.Insert("2", "published", time.Date(2019, time.January, 5, 12, 0, 0, 0, time.UTC))
	db.Insert("2", "content", "Hey this is another ...")

	// no tag
	db.Insert("3", "title", "A bad post")
	db.Insert("3", "tag", "bad")
	db.Insert("3", "published", time.Date(2019, time.January, 8, 12, 0, 0, 0, time.UTC))
	db.Insert("3", "content", "Bad ...")

	// too old
	db.Insert("4", "title", "A good old post")
	db.Insert("4", "tag", "good")
	db.Insert("4", "published", time.Date(2018, time.January, 8, 12, 0, 0, 0, time.UTC))
	db.Insert("4", "content", "Old ...")

	// after date we are searching for
	db.Insert("5", "title", "A good new post")
	db.Insert("5", "tag", "good")
	db.Insert("5", "published", time.Date(2019, time.January, 18, 12, 0, 0, 0, time.UTC))
	db.Insert("5", "content", "New ...")

	triples, err := db.List(
		Before("published", time.Date(2019, time.January, 10, 0, 0, 0, 0, time.UTC)).
			Where("tag", "good").
			Limit(3),
	)

	assert.Nil(err)
	if assert.Len(triples, 13) {

		type Post struct {
			ID        string
			Title     string
			Tags      []string
			Published time.Time
			Content   string
		}

		var posts []Post
		post := Post{ID: triples[0].Subject}

		for _, triple := range triples {
			if triple.Subject != post.ID {
				posts = append(posts, post)
				post = Post{ID: triple.Subject}
			}

			switch triple.Predicate {
			case "title":
				triple.Value(&post.Title)
			case "tag":
				var tag string
				triple.Value(&tag)
				post.Tags = append(post.Tags, tag)
			case "published":
				triple.Value(&post.Published)
			case "content":
				triple.Value(&post.Content)
			}
		}
		posts = append(posts, post)

		if assert.Len(posts, 3) {
			assert.Equal(Post{
				ID:        "1",
				Title:     "A post",
				Tags:      []string{"good", "other"},
				Published: time.Date(2019, time.January, 7, 12, 0, 0, 0, time.UTC),
				Content:   "Hey this is a ...",
			}, posts[0])

			assert.Equal(Post{
				ID:        "2",
				Title:     "Another post",
				Tags:      []string{"good"},
				Published: time.Date(2019, time.January, 5, 12, 0, 0, 0, time.UTC),
				Content:   "Hey this is another ...",
			}, posts[1])

			assert.Equal(Post{
				ID:        "0",
				Title:     "A null post",
				Tags:      []string{"good"},
				Published: time.Date(2019, time.January, 3, 12, 0, 0, 0, time.UTC),
				Content:   "<nope>",
			}, posts[2])
		}
	}
}
