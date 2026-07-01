package main

import (
	"context"
	"fmt"
	"log"
	"time"

	lookup "github.com/oarkflow/lookupx/pkg"
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

	// This example uses SliceSource so it runs without a database driver.
	// For production DB ingestion, use SQLSource below with your existing *sql.DB.
	records := make([]lookup.SourceRecord, 0, 4)
	for i, r := range []struct {
		id, term, groupID, date_key, partitionID string
	}{
		{"enc-1", "key-special", "4", "2026-01-01", "200"},
		{"enc-2", "key-special", "5", "2026-01-01", "200"},
		{"enc-3", "key-special", "4", "2026-01-02", "201"},
		{"enc-4", "ab99", "4", "2026-01-01", "200"},
	} {
		rec := lookup.SourceRecord{ID: r.id, Seq: uint64(i + 1)}
		rec.AddKeyword(term, r.term, true)
		rec.AddKeyword(group, r.groupID, true)
		rec.AddKeyword(date_key, r.date_key, true)
		rec.AddKeyword(partition, r.partitionID, true)
		records = append(records, rec)
	}

	stats, err := ix.IndexFrom(context.Background(), lookup.SliceSource{Records: records}, lookup.BulkOptions{
		Name:            "records",
		BatchSize:       2,
		CheckpointEvery: 2,
		Checkpoint:      lookup.NewMemoryCheckpoint(),
		Resume:          true,
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("indexed=%d skipped=%d\n", stats.Indexed, stats.Skipped)

	// Equivalent user input: term=key-special&group_id=4&date_key=2026-01-01
	query := lookup.ParseLookupQuery("term=key-special&group_id=4&date_key=2026-01-01")
	_, hits := ix.SearchInto(lookup.SearchRequest{Query: query, Limit: 10}, nil)
	for _, h := range hits {
		fmt.Println("hit", h.ID)
	}

	// Production SQLSource shape:
	// src := lookup.SQLSource{
	//     DB: db,
	//     Query: `SELECT id, term, group_id, date_key, partition_id FROM record_lookup WHERE id > ? ORDER BY id LIMIT 100000`,
	//     Args: []any{lastID},
	//     IDColumn: "id",
	//     SeqColumn: "id",
	//     Columns: []lookup.SQLColumn{
	//         {Column: "term", Field: term, Kind: lookup.ValueKeyword, Normalized: true},
	//         {Column: "group_id", Field: group, Kind: lookup.ValueKeyword, Normalized: true},
	//         {Column: "date_key", Field: date_key, Kind: lookup.ValueKeyword, Normalized: true},
	//         {Column: "partition_id", Field: partition, Kind: lookup.ValueKeyword, Normalized: true},
	//     },
	// }
	// _, _ = ix.IndexFrom(context.Background(), src, lookup.BulkOptions{BatchSize: 65536, Checkpoint: lookup.FileCheckpoint{Path:"./checkpoint.json"}, Resume:true})
}
