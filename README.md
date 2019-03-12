# numbersix

A sqlite based triple store, for storing information. Information.

It is being built for storing micropub data, but may be useful in other
contexts. Data is stored in sqlite, with values marshaled using the
`encoding/json` package.


## Limitations

- Values are stored as text, so don't expect `After`/`Before` to give sensible
  results if using on integer values.

This probably isn't super performant, but shouldn't be too terrible. Here are
some really weak comparisons with a database/sql based implementation.

```
BenchmarkSimpleSqliteGet-4        100000             12952 ns/op             928 B/op         40 allocs/op
BenchmarkListAbout-4              100000             14636 ns/op            1264 B/op         49 allocs/op
BenchmarkSimpleSqliteSet-4        200000              8431 ns/op             160 B/op          9 allocs/op
BenchmarkSet-4                    200000             10154 ns/op             504 B/op         19 allocs/op
```
