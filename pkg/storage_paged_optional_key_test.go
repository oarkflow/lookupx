package pkg

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	_ "modernc.org/sqlite"
)

func TestPagedSQLSourceUsesOffsetAndGeneratedIDsWithoutKey(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE TABLE records (name TEXT); INSERT INTO records VALUES ('alpha'), ('beta'), ('gamma')`); err != nil {
		t.Fatal(err)
	}
	ix, err := New(Config{Schema: Schema{Fields: map[string]FieldOptions{
		"name": {Kind: FieldKeyword, Indexed: true, Lookup: true},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	defer ix.Close()
	source := PagedSQLSource{
		DB: db, Table: "records", PageSize: 2,
		ColumnBindings: []SQLColumn{{Column: "name", Field: ix.FieldID("name"), Kind: ValueKeyword}},
	}
	stats, err := ix.IndexFrom(context.Background(), source, BulkOptions{BatchSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Indexed != 3 {
		t.Fatalf("indexed %d rows, want 3", stats.Indexed)
	}
	for i, name := range []string{"alpha", "beta", "gamma"} {
		id := fmt.Sprintf("row-%d", i+1)
		q, err := ix.CompileDatasourceLookup("name=" + name)
		if err != nil {
			t.Fatal(err)
		}
		_, hits := ix.SearchInto(SearchRequest{Query: q, Limit: 1, WithDocs: true}, nil)
		if len(hits) != 1 || hits[0].ID != id {
			t.Fatalf("record %q should be stored with generated ID %q; hits=%#v", name, id, hits)
		}
	}
}
