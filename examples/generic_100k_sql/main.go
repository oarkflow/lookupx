package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/oarkflow/lookupx/lookup"
)

const totalDatasetRows = 100_000

func main() {
	registerDatasetDriver()

	db, err := sql.Open("lookupx-dataset_a-100k", "")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	ix, err := lookup.New(lookup.Config{
		DisableSource:   true,
		InitialCapacity: totalDatasetRows,
		Clock:           lookup.StaticClock{T: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)},
		Schema:          lookup.TupleLookupSchema(),
	})
	if err != nil {
		log.Fatal(err)
	}

	term := ix.FieldID("term")
	group := ix.FieldID("group_id")
	date_key := ix.FieldID("date_key")
	partition := ix.FieldID("partition_id")

	// This page function demonstrates how to index a 100K Dataset database table/query.
	// Replace this SQL with your real database query, join, view, or CTE.
	page := func(lastSeq uint64, limit int) (string, []any) {
		return `
SELECT id, seq, term, group_id, date_key, partition_id
FROM record_dataset_a_lookup
WHERE seq > ?
ORDER BY seq ASC
LIMIT ?`, []any{lastSeq, limit}
	}

	src := lookup.PagedSQLQuerySource{
		DB:        db,
		PageSize:  10_000,
		Page:      page,
		IDColumn:  "id",
		SeqColumn: "seq",
		Columns: []lookup.SQLColumn{
			{Column: "term", Field: term, Kind: lookup.ValueKeyword, Normalized: false},
			{Column: "group_id", Field: group, Kind: lookup.ValueKeyword, Normalized: true},
			{Column: "date_key", Field: date_key, Kind: lookup.ValueKeyword, Normalized: true},
			{Column: "partition_id", Field: partition, Kind: lookup.ValueKeyword, Normalized: true},
		},
	}

	started := time.Now()
	stats, err := ix.IndexFrom(context.Background(), src, lookup.BulkOptions{
		Name:            "dataset_a-100k-demo",
		BatchSize:       10_000,
		CheckpointEvery: 10_000,
		Checkpoint:      lookup.NewMemoryCheckpoint(),
		Resume:          true,
	})
	if err != nil {
		log.Fatal(err)
	}
	idxTook := time.Since(started)
	fmt.Printf("indexed=%d skipped=%d took=%s ns_per_row=%.1f rows_per_sec=%.0f\n", stats.Indexed, stats.Skipped, idxTook.Round(time.Millisecond), float64(idxTook.Nanoseconds())/float64(stats.Indexed), float64(stats.Indexed)/idxTook.Seconds())

	runQuery(ix, "term=key-0013&group_id=4&date_key=2026-01-01", 1000)
	runQuery(ix, "term=key-special&group_id=4&date_key=2026-01-01", 1000)
	runQuery(ix, "term=key-0014&group_id=4&date_key=2026-01-04", 1000)
}

func runQuery(ix *lookup.Index, raw string, loops int) {
	q := lookup.ParseLookupQuery(raw)
	var hits []lookup.Hit
	started := time.Now()
	for i := 0; i < loops; i++ {
		_, hits = ix.SearchInto(lookup.SearchRequest{Query: q, Limit: 5}, hits[:0])
	}
	took := time.Since(started)
	fmt.Printf("query=%q hits=%d avg_query_ns=%d loops=%d", raw, len(hits), took.Nanoseconds()/int64(loops), loops)
	if len(hits) > 0 {
		fmt.Printf(" first=%s", hits[0].ID)
	}
	fmt.Println()
}

var dataset_aDriverRegistered atomic.Bool

func registerDatasetDriver() {
	if dataset_aDriverRegistered.CompareAndSwap(false, true) {
		sql.Register("lookupx-dataset_a-100k", dataset_aDriver{})
	}
}

type dataset_aDriver struct{}

func (dataset_aDriver) Open(name string) (driver.Conn, error) { return dataset_aConn{}, nil }

type dataset_aConn struct{}

func (dataset_aConn) Prepare(query string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (dataset_aConn) Close() error                              { return nil }
func (dataset_aConn) Begin() (driver.Tx, error)                 { return nil, driver.ErrSkip }

func (dataset_aConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	last := uint64(0)
	limit := totalDatasetRows
	if len(args) > 0 {
		last = toUint64(args[0].Value)
	}
	if len(args) > 1 {
		limit = int(toUint64(args[1].Value))
	}
	start := int(last) + 1
	end := start + limit - 1
	if end > totalDatasetRows {
		end = totalDatasetRows
	}
	return &dataset_aRows{next: start, end: end}, nil
}

func (dataset_aConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	last := uint64(0)
	limit := totalDatasetRows
	if len(args) > 0 {
		last = toUint64(args[0])
	}
	if len(args) > 1 {
		limit = int(toUint64(args[1]))
	}
	start := int(last) + 1
	end := start + limit - 1
	if end > totalDatasetRows {
		end = totalDatasetRows
	}
	return &dataset_aRows{next: start, end: end}, nil
}

type dataset_aRows struct {
	next int
	end  int
}

func (r *dataset_aRows) Columns() []string {
	return []string{"id", "seq", "term", "group_id", "date_key", "partition_id"}
}

func (r *dataset_aRows) Close() error { return nil }

func (r *dataset_aRows) Next(dest []driver.Value) error {
	if r.next > r.end {
		return io.EOF
	}
	seq := r.next
	r.next++

	term, group, date_key, partition := dataset_aRecord(seq)
	dest[0] = fmt.Sprintf("enc-%06d", seq)
	dest[1] = strconv.Itoa(seq)
	dest[2] = term
	dest[3] = group
	dest[4] = date_key
	dest[5] = partition
	return nil
}

func dataset_aRecord(i int) (term, group, date_key, partition string) {
	// Most rows look like ordinary Dataset record rows.
	codes := [...]string{"key-0011", "key-0012", "key-0013", "key-0014", "key-0015", "key-3000", "key-0053", "key-5025", "key-6415", "key-1046"}
	term = codes[i%len(codes)]
	group = strconv.Itoa((i % 10) + 1)
	date_key = fmt.Sprintf("2026-01-%02d", (i%28)+1)
	partition = strconv.Itoa((i % 200) + 1)

	// Deterministic rows for example lookups.
	if i%1000 == 123 {
		return "key-0013", "4", "2026-01-01", partition
	}
	if i%1000 == 456 {
		return "key-special", "4", "2026-01-01", partition
	}
	return term, group, date_key, partition
}

func toUint64(v any) uint64 {
	switch x := v.(type) {
	case int:
		return uint64(x)
	case int64:
		return uint64(x)
	case uint64:
		return x
	case string:
		u, _ := strconv.ParseUint(strings.TrimSpace(x), 10, 64)
		return u
	case []byte:
		u, _ := strconv.ParseUint(strings.TrimSpace(string(x)), 10, 64)
		return u
	default:
		return 0
	}
}
