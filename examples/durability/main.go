package main

import (
	"fmt"
	"os"
	"path/filepath"

	lookup "github.com/oarkflow/lookupx/pkg"
)

func main() {
	dir, _ := os.MkdirTemp("", "lookupx-example-")
	defer os.RemoveAll(dir)
	schema := lookup.Schema{Fields: map[string]lookup.FieldOptions{"title": {Kind: lookup.FieldText, Indexed: true, Lookup: true, Lowercase: true}, "sku": {Kind: lookup.FieldKeyword, Lookup: true, Unique: true, Lowercase: true}}}
	ix, _ := lookup.New(lookup.Config{Schema: schema, EnableWAL: true, WALPath: filepath.Join(dir, "wal.jsonl"), SnapshotPath: filepath.Join(dir, "snap.json")})
	_ = ix.Upsert("1", lookup.Document{"title": "durable lookup", "sku": "d-1"})
	_ = ix.SaveSnapshot(filepath.Join(dir, "snap.json"))
	_ = ix.SaveSegment(filepath.Join(dir, "seg.lxs"))
	ix2, _ := lookup.New(lookup.Config{Schema: schema})
	_ = ix2.LoadSegment(filepath.Join(dir, "seg.lxs"))
	fmt.Println("loaded", ix2.Count(lookup.Term{Field: "title", Value: "durable"}))
}
