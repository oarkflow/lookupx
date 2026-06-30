package lookup

import (
	"strings"
	"testing"
)

func TestSQLSelectBuildDialects(t *testing.T) {
	q, args, err := SQLSelect{
		Dialect: SQLDialectPostgres,
		Table:   "record_lookup",
		Columns: []string{"id", "term", "group_id", "date_key"},
		Where: []SQLWhere{
			{Column: "group_id", Value: 4},
			{Column: "date_key", Value: "2026-01-01"},
		},
		OrderBy: "id ASC",
		Limit:   1000,
	}.Build()
	if err != nil {
		t.Fatal(err)
	}
	want := "SELECT id, term, group_id, date_key FROM record_lookup WHERE group_id = $1 AND date_key = $2 ORDER BY id ASC LIMIT 1000"
	if q != want {
		t.Fatalf("query mismatch\nwant: %s\n got: %s", want, q)
	}
	if len(args) != 2 || args[0] != 4 || args[1] != "2026-01-01" {
		t.Fatalf("bad args: %#v", args)
	}
}

func TestTupleSQLQuery(t *testing.T) {
	q, args, err := TupleSQLQuery(TupleSQLQueryOptions{
		Dialect:    SQLDialectQuestion,
		GroupID: 4,
		DateKey:        "2026-01-01",
		Term:       "key-special",
		Limit:      50,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, part := range []string{"SELECT id, term, group_id, date_key, partition_id", "FROM record_lookup", "group_id = ?", "date_key = ?", "term = ?", "ORDER BY id", "LIMIT 50"} {
		if !strings.Contains(q, part) {
			t.Fatalf("query %q missing %q", q, part)
		}
	}
	if len(args) != 3 {
		t.Fatalf("expected 3 args, got %#v", args)
	}
}

func TestTuplePagedSQLQuery(t *testing.T) {
	page := TuplePagedSQLQuery(SQLDialectPostgres, "record_lookup", nil, "id", []SQLWhere{{Column: "partition_id", Value: 12}})
	q, args := page(100, 1000)
	if !strings.Contains(q, "partition_id = $1") || !strings.Contains(q, "id > $2") || !strings.Contains(q, "LIMIT 1000") {
		t.Fatalf("bad page query: %s args=%#v", q, args)
	}
	if len(args) != 2 || args[0] != 12 || args[1] != uint64(100) {
		t.Fatalf("bad args: %#v", args)
	}
}

func TestSQLQuerySourceRequiresQuery(t *testing.T) {
	_, err := SQLQuerySource{}.Open(nil)
	if err == nil {
		t.Fatal("expected error")
	}
}
