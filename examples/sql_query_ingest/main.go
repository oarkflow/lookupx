package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/oarkflow/lookupx/lookup"
)

func main() {
	ix, err := lookup.New(lookup.Config{
		DisableSource:   true,
		InitialCapacity: 1_000_000,
		Clock:           lookup.StaticClock{T: time.Unix(1700000000, 0)},
		Schema:          lookup.TupleLookupSchema(),
	})
	if err != nil {
		log.Fatal(err)
	}

	term := ix.FieldID("term")
	group := ix.FieldID("group_id")
	date_key := ix.FieldID("date_key")
	partition := ix.FieldID("partition_id")

	// 1) Build a parameterized SQL query for targeted indexing/search rebuilds.
	query, args, err := lookup.TupleSQLQuery(lookup.TupleSQLQueryOptions{
		Dialect:    lookup.SQLDialectPostgres,
		Table:      "record_lookup",
		GroupID: 4,
		DateKey:        "2026-01-01",
		Term:       "key-special",
		Limit:      100000,
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(query)
	fmt.Println(args)

	// Production raw SQL query source. This can be a join, CTE, view, WHERE query,
	// or any database/sql query. It is not executed in this example so it can run
	// without a database driver.
	_ = lookup.SQLQuerySource{
		// DB: db,
		Query: `
SELECT e.id, c.code AS term, e.group_id, e.date_key, e.partition_id
FROM records e
JOIN record_codes c ON c.id = e.code_id
WHERE e.deleted_at IS NULL AND e.id > $1
ORDER BY e.id ASC
LIMIT $2`,
		Args:      []any{0, 100000},
		IDColumn:  "id",
		SeqColumn: "id",
		Columns: []lookup.SQLColumn{
			{Column: "term", Field: term, Kind: lookup.ValueKeyword, Normalized: true},
			{Column: "group_id", Field: group, Kind: lookup.ValueKeyword, Normalized: true},
			{Column: "date_key", Field: date_key, Kind: lookup.ValueKeyword, Normalized: true},
			{Column: "partition_id", Field: partition, Kind: lookup.ValueKeyword, Normalized: true},
		},
	}

	// 2) Build a keyset-paginated SQL query source for 100M+ rows from a custom query.
	_ = lookup.PagedSQLQuerySource{
		// DB: db,
		PageSize: 100000,
		Page: lookup.TuplePagedSQLQuery(
			lookup.SQLDialectPostgres,
			"record_lookup",
			[]string{"id", "term", "group_id", "date_key", "partition_id"},
			"id",
			[]lookup.SQLWhere{{Column: "partition_id", Op: "=", Value: 200}},
		),
		IDColumn:  "id",
		SeqColumn: "id",
		Columns: []lookup.SQLColumn{
			{Column: "term", Field: term, Kind: lookup.ValueKeyword, Normalized: true},
			{Column: "group_id", Field: group, Kind: lookup.ValueKeyword, Normalized: true},
			{Column: "date_key", Field: date_key, Kind: lookup.ValueKeyword, Normalized: true},
			{Column: "partition_id", Field: partition, Kind: lookup.ValueKeyword, Normalized: true},
		},
	}

	// Runnable mini-ingest using SliceSource so this example validates end-to-end.
	r := lookup.SourceRecord{ID: "enc-1", Seq: 1}
	r.AddKeyword(term, "key-special", true)
	r.AddKeyword(group, "4", true)
	r.AddKeyword(date_key, "2026-01-01", true)
	r.AddKeyword(partition, "200", true)
	_, err = ix.IndexFrom(context.Background(), lookup.SliceSource{Records: []lookup.SourceRecord{r}}, lookup.BulkOptions{BatchSize: 1})
	if err != nil {
		log.Fatal(err)
	}
	_, hits := ix.SearchInto(lookup.SearchRequest{Query: lookup.ParseLookupQuery("term=key-special&group_id=4&date_key=2026-01-01"), Limit: 10}, nil)
	fmt.Println("hits", len(hits))
}
