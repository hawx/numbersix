package numbersix

import (
	"testing"
	"time"

	"hawx.me/code/assert"
)

func BenchmarkSimpleSqliteGet(b *testing.B) {
	db, _ := Open("file::memory:")
	db.Set("a", "b", "c")

	for i := 0; i < b.N; i++ {
		rows, _ := db.db.Query("SELECT subject, predicate, value FROM triples WHERE subject = 'a'")
		defer rows.Close()

		for rows.Next() {
			var subject, predicate, value string
			rows.Scan(&subject, &predicate, &value)
		}
	}
}

func BenchmarkListAbout(b *testing.B) {
	db, _ := Open("file::memory:")
	db.Set("a", "b", "c")

	for i := 0; i < b.N; i++ {
		triples, _ := db.List(About("a"))
		var s string
		triples[0].Value(&s)
	}
}

func TestList(t *testing.T) {
	assert := assert.New(t)

	db, _ := Open("file::memory:")
	assert.Nil(db.Set("john@doe.com", "age", 25))
	assert.Nil(db.Set("jane@doe.com", "age", 23))

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

func TestListAll(t *testing.T) {
	assert := assert.New(t)

	db, _ := Open("file::memory:")

	db.Set("1", "name", "John")
	db.Set("1", "age", 25)
	db.Set("2", "name", "Jane")
	db.Set("2", "age", 23)

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

func TestListAbout(t *testing.T) {
	assert := assert.New(t)

	db, _ := Open("file::memory:")

	db.Set("1", "name", "John")
	db.Set("1", "age", 25)
	db.Set("2", "name", "Jane")
	db.Set("2", "age", 23)

	triples, err := db.List(About("1"))
	assert.Nil(err)
	assert.Len(triples, 2)

	assertTriples(t, triples, []pair{
		{"1", "age"},
		{"1", "name"},
	})
}

func TestListAboutAndWhere(t *testing.T) {
	assert := assert.New(t)

	db, _ := Open("file::memory:")

	db.Set("1", "name", "John")
	db.Set("1", "age", 25)
	db.Set("2", "name", "Jane")
	db.Set("2", "age", 23)

	triples, err := db.List(About("1").Where("age", 24))
	assert.Nil(err)
	assert.Len(triples, 0)

	triples, err = db.List(About("1").Where("age", 25))
	assert.Nil(err)
	assert.Len(triples, 2)

	assertTriples(t, triples, []pair{
		{"1", "age"},
		{"1", "name"},
	})
}

func TestQueryWhere(t *testing.T) {
	assert := assert.New(t)

	db, _ := Open("file::memory:")

	db.Set("1", "name", "John")
	db.Set("1", "age", 25)
	db.Set("2", "name", "Jane")
	db.Set("2", "age", 23)

	triples, err := db.List(Where("age", 25))
	assert.Nil(err)
	assert.Len(triples, 2)

	assertTriples(t, triples, []pair{
		{"1", "age"},
		{"1", "name"},
	})
}

func TestQueryBegins(t *testing.T) {
	assert := assert.New(t)

	db, _ := Open("file::memory:")

	db.Set("1", "name", "John")
	db.Set("1", "age", 25)
	db.Set("2", "name", "Jane")
	db.Set("2", "age", 23)
	db.Set("3", "name", "George")
	db.Set("3", "age", 26)

	triples, err := db.List(Begins("name", "J"))
	assert.Nil(err)
	assert.Len(triples, 4)

	assertTriples(t, triples, []pair{
		{"1", "age"},
		{"1", "name"},
		{"2", "age"},
		{"2", "name"},
	})
}

func TestQueryBeginsAndHas(t *testing.T) {
	assert := assert.New(t)

	db, _ := Open("file::memory:")

	db.Set("1", "name", "John")
	db.Set("2", "name", "Jane")
	db.Set("2", "age", 23)
	db.Set("3", "name", "George")
	db.Set("3", "age", 26)

	triples, err := db.List(Begins("name", "J").Has("age"))
	assert.Nil(err)
	assert.Len(triples, 2)

	assertTriples(t, triples, []pair{
		{"2", "age"},
		{"2", "name"},
	})
}

func TestAnyAbout(t *testing.T) {
	assert := assert.New(t)

	db, _ := Open("file::memory:")

	db.Set("1", "name", "John")
	db.Set("1", "age", 25)
	db.Set("2", "name", "Jane")
	db.Set("2", "age", 23)

	ok, err := db.Any(About("1"))
	assert.Nil(err)
	assert.True(ok)

	ok, err = db.Any(About("3"))
	assert.Nil(err)
	assert.False(ok)
}

func TestAnyAboutAndWhere(t *testing.T) {
	assert := assert.New(t)

	db, _ := Open("file::memory:")

	db.Set("1", "name", "John")
	db.Set("1", "age", 25)
	db.Set("2", "name", "Jane")
	db.Set("2", "age", 23)

	ok, err := db.Any(About("1").Where("age", 24))
	assert.Nil(err)
	assert.False(ok)

	ok, err = db.Any(About("1").Where("age", 25))
	assert.Nil(err)
	assert.True(ok)
}

func TestListOrdered(t *testing.T) {
	db, _ := Open("file::memory:")

	db.Set("2", "age", 25)
	db.Set("2", "tag", "cool")

	db.Set("4", "age", 19)

	db.Set("8", "age", 18)
	db.Set("8", "tag", "uncool")

	db.Set("9", "age", 17)
	db.Set("9", "tag", "cool")
	db.Set("9", "tag", "cooler")

	db.Set("7", "age", 22)
	db.Set("7", "tag", "cool")

	db.Set("3", "age", 21)
	db.Set("3", "tag", "uncool")
	db.Set("3", "deleted", true)

	db.Set("1", "age", 24)

	db.Set("5", "age", 23)

	db.Set("6", "age", 20)
	db.Set("6", "tag", "who")
	db.Set("6", "tag", "cooler")
	db.Set("6", "tag", "uncool")

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
			{"3", "deleted"},
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
			{"3", "deleted"},
			{"3", "tag"},
			{"6", "age"}, // 20
			{"6", "tag"},
			{"6", "tag"},
			{"6", "tag"},
			{"4", "age"}, // 19
		})
	})

	t.Run("Before with Limit and Without", func(t *testing.T) {
		triples, err := db.List(Before("age", 22).Limit(3).Without("deleted"))
		assert.Nil(t, err)

		assertTriples(t, triples, []pair{
			{"6", "age"}, // 20
			{"6", "tag"},
			{"6", "tag"},
			{"6", "tag"},
			{"4", "age"}, // 19
			{"8", "age"}, // 18
			{"8", "tag"},
		})
	})
}

func TestListAfterWhereLimit(t *testing.T) {
	assert := assert.New(t)

	db, _ := Open("file::memory:")

	db.Set("0", "title", "A null post")
	db.Set("0", "tag", "good")
	db.Set("0", "published", time.Date(2019, time.January, 3, 12, 0, 0, 0, time.UTC))
	db.Set("0", "content", "<nope>")

	// out of order
	db.Set("1", "title", "A post")
	db.Set("1", "tag", "good")
	db.Set("1", "tag", "other")
	db.Set("1", "published", time.Date(2019, time.January, 7, 12, 0, 0, 0, time.UTC))
	db.Set("1", "content", "Hey this is a ...")

	db.Set("2", "title", "Another post")
	db.Set("2", "tag", "good")
	db.Set("2", "published", time.Date(2019, time.January, 5, 12, 0, 0, 0, time.UTC))
	db.Set("2", "content", "Hey this is another ...")

	// no tag
	db.Set("3", "title", "A bad post")
	db.Set("3", "tag", "bad")
	db.Set("3", "published", time.Date(2019, time.January, 8, 12, 0, 0, 0, time.UTC))
	db.Set("3", "content", "Bad ...")

	// too old
	db.Set("4", "title", "A good old post")
	db.Set("4", "tag", "good")
	db.Set("4", "published", time.Date(2018, time.January, 8, 12, 0, 0, 0, time.UTC))
	db.Set("4", "content", "Old ...")

	// after date we are searching for
	db.Set("5", "title", "A good new post")
	db.Set("5", "tag", "good")
	db.Set("5", "published", time.Date(2019, time.January, 18, 12, 0, 0, 0, time.UTC))
	db.Set("5", "content", "New ...")

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

func TestListAscending(t *testing.T) {
	assert := assert.New(t)

	db, _ := Open("file::memory:")

	db.Set("0", "title", "A null post")
	db.Set("0", "tag", "good")
	db.Set("0", "published", time.Date(2019, time.January, 3, 12, 0, 0, 0, time.UTC))

	// out of order
	db.Set("1", "title", "A post")
	db.Set("1", "tag", "good")
	db.Set("1", "tag", "other")
	db.Set("1", "published", time.Date(2019, time.January, 7, 12, 0, 0, 0, time.UTC))

	db.Set("2", "title", "Another post")
	db.Set("2", "tag", "good")
	db.Set("2", "published", time.Date(2019, time.January, 5, 12, 0, 0, 0, time.UTC))

	// no tag
	db.Set("3", "title", "A bad post")
	db.Set("3", "tag", "bad")
	db.Set("3", "published", time.Date(2019, time.January, 8, 12, 0, 0, 0, time.UTC))

	triples, err := db.List(
		Ascending("published").
			Where("tag", "good"),
	)

	assert.Nil(err)
	if assert.Len(triples, 10) {

		type Post struct {
			ID        string
			Title     string
			Tags      []string
			Published time.Time
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
			}
		}
		posts = append(posts, post)

		if assert.Len(posts, 3) {
			assert.Equal(Post{
				ID:        "1",
				Title:     "A post",
				Tags:      []string{"good", "other"},
				Published: time.Date(2019, time.January, 7, 12, 0, 0, 0, time.UTC),
			}, posts[2])

			assert.Equal(Post{
				ID:        "2",
				Title:     "Another post",
				Tags:      []string{"good"},
				Published: time.Date(2019, time.January, 5, 12, 0, 0, 0, time.UTC),
			}, posts[1])

			assert.Equal(Post{
				ID:        "0",
				Title:     "A null post",
				Tags:      []string{"good"},
				Published: time.Date(2019, time.January, 3, 12, 0, 0, 0, time.UTC),
			}, posts[0])
		}
	}
}

func TestListDescending(t *testing.T) {
	assert := assert.New(t)

	db, _ := Open("file::memory:")

	db.Set("0", "title", "A null post")
	db.Set("0", "tag", "good")
	db.Set("0", "published", time.Date(2019, time.January, 3, 12, 0, 0, 0, time.UTC))

	// out of order
	db.Set("1", "title", "A post")
	db.Set("1", "tag", "good")
	db.Set("1", "tag", "other")
	db.Set("1", "published", time.Date(2019, time.January, 7, 12, 0, 0, 0, time.UTC))

	db.Set("2", "title", "Another post")
	db.Set("2", "tag", "good")
	db.Set("2", "published", time.Date(2019, time.January, 5, 12, 0, 0, 0, time.UTC))

	// no tag
	db.Set("3", "title", "A bad post")
	db.Set("3", "tag", "bad")
	db.Set("3", "published", time.Date(2019, time.January, 8, 12, 0, 0, 0, time.UTC))

	triples, err := db.List(
		Descending("published").
			Where("tag", "good"),
	)

	assert.Nil(err)
	if assert.Len(triples, 10) {

		type Post struct {
			ID        string
			Title     string
			Tags      []string
			Published time.Time
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
			}
		}
		posts = append(posts, post)

		if assert.Len(posts, 3) {
			assert.Equal(Post{
				ID:        "1",
				Title:     "A post",
				Tags:      []string{"good", "other"},
				Published: time.Date(2019, time.January, 7, 12, 0, 0, 0, time.UTC),
			}, posts[0])

			assert.Equal(Post{
				ID:        "2",
				Title:     "Another post",
				Tags:      []string{"good"},
				Published: time.Date(2019, time.January, 5, 12, 0, 0, 0, time.UTC),
			}, posts[1])

			assert.Equal(Post{
				ID:        "0",
				Title:     "A null post",
				Tags:      []string{"good"},
				Published: time.Date(2019, time.January, 3, 12, 0, 0, 0, time.UTC),
			}, posts[2])
		}
	}
}
