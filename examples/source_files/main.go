package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/oarkflow/lookupx/lookup"
)

func main() {
	ix, err := lookup.New(lookup.Config{DisableSource: true, InitialCapacity: 1000, Clock: lookup.StaticClock{T: time.Unix(1700000000, 0)}, Schema: lookup.TupleLookupSchema()})
	if err != nil {
		log.Fatal(err)
	}
	term := ix.FieldID("term")
	work := ix.FieldID("group_id")
	date_key := ix.FieldID("date_key")

	csvData := "id,term,group_id,date_key\n1,key-special,4,2026-01-01\n2,key-special,5,2026-01-01\n"
	src := lookup.CSVSource{R: strings.NewReader(csvData), IDColumn: "id", Bindings: []lookup.CSVBinding{
		{Column: "term", Field: term, Kind: lookup.ValueKeyword, Normalized: true},
		{Column: "group_id", Field: work, Kind: lookup.ValueKeyword, Normalized: true},
		{Column: "date_key", Field: date_key, Kind: lookup.ValueKeyword, Normalized: true},
	}}
	if _, err := ix.IndexFrom(context.Background(), src, lookup.BulkOptions{Name: "csv", BatchSize: 1024}); err != nil {
		log.Fatal(err)
	}
	_, hits := ix.SearchInto(lookup.SearchRequest{Query: lookup.TupleQuery("key-special", "4", "2026-01-01"), Limit: 5}, nil)
	fmt.Println("hits", len(hits), hits[0].ID)
}
