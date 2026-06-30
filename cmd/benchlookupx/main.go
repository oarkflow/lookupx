package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/oarkflow/lookupx/lookup"
)

type row struct {
	ID     string            `json:"id"`
	Fields map[string]string `json:"fields"`
}
type query struct {
	Name   string            `json:"name"`
	Fields map[string]string `json:"fields"`
	Limit  int               `json:"limit"`
}
type result struct {
	Engine          string    `json:"engine"`
	Rows            int       `json:"rows"`
	IndexNanos      int64     `json:"index_nanos"`
	IndexRowsPerSec float64   `json:"index_rows_per_sec"`
	Queries         []qresult `json:"queries"`
}
type qresult struct {
	Name     string `json:"name"`
	AvgNanos int64  `json:"avg_nanos"`
	Hits     int    `json:"hits"`
	Loops    int    `json:"loops"`
}

func main() {
	dataPath := flag.String("data", "benchdata/dataset.jsonl", "dataset JSONL")
	queryPath := flag.String("queries", "benchdata/queries.jsonl", "queries JSONL")
	fieldsCSV := flag.String("fields", "field_a,field_b,field_c", "comma-separated indexed fields")
	compositeID := flag.String("composite", "main", "composite id")
	loops := flag.Int("loops", 1000, "query loops")
	flag.Parse()
	fields := split(*fieldsCSV)
	schemaFields := map[string]lookup.FieldKind{}
	for _, f := range fields {
		schemaFields[f] = lookup.FieldKeyword
	}
	ix, err := lookup.New(lookup.Config{Schema: lookup.GenericLookupSchema(schemaFields), DisableSource: true, InitialCapacity: 1024, Clock: lookup.StaticClock{T: time.Unix(0, 0)}})
	if err != nil {
		panic(err)
	}
	compFields := make([]lookup.CompositeField, len(fields))
	for i, f := range fields {
		compFields[i] = lookup.CompositeField{Name: f, ID: ix.FieldID(f)}
	}
	ix.EnableComposite(lookup.CompositeDefinition{ID: *compositeID, Fields: compFields})

	f, err := os.Open(*dataPath)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	start := time.Now()
	rows := 0
	src := jsonlSource{r: bufio.NewScanner(f), fields: fields, fieldIDs: fieldIDs(ix, fields)}
	stats, err := ix.IndexFrom(context.Background(), src, lookup.BulkOptions{Name: "bench", BatchSize: 65536, SkipBadRecords: true})
	if err != nil {
		panic(err)
	}
	rows = int(stats.Indexed)
	idxNanos := time.Since(start).Nanoseconds()

	qs := readQueries(*queryPath)
	qr := make([]qresult, 0, len(qs))
	dst := make([]lookup.Hit, 0, 16)
	for _, q := range qs {
		vals := make([]string, len(fields))
		for i, f := range fields {
			vals[i] = strings.ToLower(q.Fields[f])
		}
		if q.Limit <= 0 {
			q.Limit = 10
		}
		start := time.Now()
		hits := 0
		for i := 0; i < *loops; i++ {
			dst = ix.CompositeLookup(*compositeID, vals, q.Limit, dst)
			hits = len(dst)
		}
		qr = append(qr, qresult{Name: q.Name, AvgNanos: time.Since(start).Nanoseconds() / int64(*loops), Hits: hits, Loops: *loops})
	}
	out := result{Engine: "lookupx", Rows: rows, IndexNanos: idxNanos, IndexRowsPerSec: float64(rows) / (float64(idxNanos) / 1e9), Queries: qr}
	b, _ := json.MarshalIndent(out, "", "  ")
	fmt.Println(string(b))
}

func split(s string) []string {
	parts := strings.Split(s, ",")
	out := parts[:0]
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
func fieldIDs(ix *lookup.Index, fields []string) []lookup.FieldID {
	ids := make([]lookup.FieldID, len(fields))
	for i, f := range fields {
		ids[i] = ix.FieldID(f)
	}
	return ids
}

type jsonlSource struct {
	r        *bufio.Scanner
	fields   []string
	fieldIDs []lookup.FieldID
	err      error
}

func (s jsonlSource) Open(ctx context.Context) (lookup.Cursor, error) {
	s.r.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	return &s, nil
}
func (s *jsonlSource) Next(ctx context.Context, dst *lookup.SourceRecord) bool {
	if !s.r.Scan() {
		s.err = s.r.Err()
		return false
	}
	var rw row
	if err := json.Unmarshal(s.r.Bytes(), &rw); err != nil {
		s.err = err
		return false
	}
	dst.Reset()
	dst.ID = rw.ID
	for i, f := range s.fields {
		dst.AddKeyword(s.fieldIDs[i], strings.ToLower(rw.Fields[f]), true)
	}
	return true
}
func (s *jsonlSource) Err() error   { return s.err }
func (s *jsonlSource) Close() error { return nil }
func readQueries(path string) []query {
	f, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	var out []query
	for sc.Scan() {
		var q query
		if err := json.Unmarshal(sc.Bytes(), &q); err != nil {
			panic(err)
		}
		out = append(out, q)
	}
	return out
}
