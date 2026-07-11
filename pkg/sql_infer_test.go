package pkg

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Minimal fake database/sql driver for exercising InferSQLColumns without a
// real database. It always returns the same fixed column/type/row set for
// any query, which is all schema inference needs (query text is opaque to it).
// ---------------------------------------------------------------------------

type fakeColSpec struct {
	name   string
	dbType string
}

type fakeDataset struct {
	cols []fakeColSpec
	rows [][]driver.Value
}

type fakeRows struct {
	ds  *fakeDataset
	pos int
}

func (r *fakeRows) Columns() []string {
	out := make([]string, len(r.ds.cols))
	for i, c := range r.ds.cols {
		out[i] = c.name
	}
	return out
}
func (r *fakeRows) Close() error { return nil }
func (r *fakeRows) Next(dest []driver.Value) error {
	if r.pos >= len(r.ds.rows) {
		return io.EOF
	}
	copy(dest, r.ds.rows[r.pos])
	r.pos++
	return nil
}
func (r *fakeRows) ColumnTypeDatabaseTypeName(i int) string { return r.ds.cols[i].dbType }

type fakeStmt struct{ ds *fakeDataset }

func (s *fakeStmt) Close() error  { return nil }
func (s *fakeStmt) NumInput() int { return -1 }
func (s *fakeStmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, fmt.Errorf("exec not supported")
}

// Query emulates "WHERE id > args[0] ORDER BY id LIMIT args[1]" against the
// fixed dataset so keyset-paginated callers (AutoPagedSQLQuery) terminate
// instead of re-reading the same page forever.
func (s *fakeStmt) Query(args []driver.Value) (driver.Rows, error) {
	rows := s.ds.rows
	if len(args) >= 1 {
		lastSeq := toInt64(args[0])
		filtered := make([][]driver.Value, 0, len(rows))
		for _, r := range rows {
			if toInt64(r[0]) > lastSeq {
				filtered = append(filtered, r)
			}
		}
		rows = filtered
	}
	if len(args) >= 2 {
		if limit := toInt64(args[1]); limit >= 0 && int64(len(rows)) > limit {
			rows = rows[:limit]
		}
	}
	return &fakeRows{ds: &fakeDataset{cols: s.ds.cols, rows: rows}}, nil
}

func toInt64(v driver.Value) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case uint64:
		return int64(x)
	case int:
		return int64(x)
	default:
		return 0
	}
}

type fakeConn struct{ ds *fakeDataset }

func (c *fakeConn) Prepare(query string) (driver.Stmt, error) { return &fakeStmt{ds: c.ds}, nil }
func (c *fakeConn) Close() error                              { return nil }
func (c *fakeConn) Begin() (driver.Tx, error)                 { return nil, fmt.Errorf("tx not supported") }

type fakeDriver struct{ ds *fakeDataset }

func (d *fakeDriver) Open(name string) (driver.Conn, error) { return &fakeConn{ds: d.ds}, nil }

var fakeDriverSeq int64

// openFakeDB registers a fresh driver for ds and opens a *sql.DB backed by it.
func openFakeDB(t *testing.T, ds *fakeDataset) *sql.DB {
	t.Helper()
	name := fmt.Sprintf("lookupx_fake_%d", atomic.AddInt64(&fakeDriverSeq, 1))
	sql.Register(name, &fakeDriver{ds: ds})
	db, err := sql.Open(name, "fake")
	if err != nil {
		t.Fatalf("open fake db: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestPostgresShapedDatasourceLookupReturnsSelectedRecord(t *testing.T) {
	columns := []fakeColSpec{
		{name: "id", dbType: "BIGINT"},
		{name: "ld", dbType: "TEXT"},
		{name: "cpt_code", dbType: "TEXT"},
		{name: "work_item", dbType: "INT8"},
		{name: "effective_date", dbType: "TIMESTAMPTZ"},
		{name: "end_effective_date", dbType: "TIMESTAMPTZ"},
		{name: "charge_type", dbType: "TEXT"},
		{name: "charge_amt", dbType: "NUMERIC"},
		{name: "provider_category", dbType: "TEXT"},
		{name: "patient_status", dbType: "INT4"},
	}
	row := []driver.Value{
		int64(943843), "943846", "99213", int64(37),
		"2018-01-01T00:00:00Z", "2018-12-31T00:00:00Z",
		"professional", "125.50", "physician", int64(1),
	}
	db := openFakeDB(t, &fakeDataset{cols: columns, rows: [][]driver.Value{row}})
	ix, src, err := AutoSQLQuery(context.Background(), Config{}, db,
		"SELECT id, ld, cpt_code, work_item, effective_date, end_effective_date, charge_type, charge_amt, provider_category, patient_status FROM charge_master",
		nil, "id", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ix.IndexFrom(context.Background(), src, BulkOptions{}); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{"ld=943846", "work_item=37", "ld=943846&work_item=37"} {
		q, err := ix.CompileDatasourceLookup(raw)
		if err != nil {
			t.Fatalf("compile %q: %v", raw, err)
		}
		_, hits := ix.SearchInto(SearchRequest{Query: q, Limit: 10, WithDocs: true}, nil)
		if len(hits) != 1 {
			t.Fatalf("lookup %q returned %d hits", raw, len(hits))
		}
		doc := hits[0].Doc
		for _, field := range []string{"ld", "cpt_code", "work_item", "effective_date", "end_effective_date", "charge_type", "charge_amt", "provider_category", "patient_status"} {
			if _, ok := doc[field]; !ok {
				t.Fatalf("lookup %q missing selected field %q in %#v", raw, field, doc)
			}
		}
		if doc["effective_date"] != "2018-01-01T00:00:00Z" {
			t.Fatalf("datasource timestamp was rewritten: %#v", doc)
		}
	}
}

func chargeMasterFakeDataset() *fakeDataset {
	return &fakeDataset{
		cols: []fakeColSpec{
			{"id", "INT8"},
			{"ld", "VARCHAR"},
			{"cpt_code", "VARCHAR"},
			{"effective_date", "DATE"},
			{"charge_type", "VARCHAR"},
			{"charge_amt", "NUMERIC"},
			{"is_active", "BOOL"},
		},
		rows: [][]driver.Value{
			{int64(1), "Office or other outpatient visit, established patient", "99213", "2024-01-02T00:00:00Z", "professional", 125.50, true},
			{int64(2), "Comprehensive metabolic panel", "80053", "2024-02-15T00:00:00Z", "professional", 42.00, true},
			{int64(3), "Chest x-ray, two views", "71046", "2024-03-01T00:00:00Z", "technical", 88.25, false},
		},
	}
}

// ---------------------------------------------------------------------------
// classifyColumn (pure heuristics, no DB)
// ---------------------------------------------------------------------------

func TestClassifyColumn_Integer(t *testing.T) {
	c := classifyColumn("charge_master_id", "INT8", []string{"1", "2", "3"})
	if c.Kind != ValueNumber || c.Options.Kind != FieldInt || !c.Options.Sortable {
		t.Fatalf("unexpected classification: %+v", c)
	}
}

func TestClassifyColumn_Float(t *testing.T) {
	c := classifyColumn("charge_amt", "NUMERIC", []string{"125.50", "42.00"})
	if c.Kind != ValueNumber || c.Options.Kind != FieldFloat {
		t.Fatalf("unexpected classification: %+v", c)
	}
}

func TestClassifyColumn_KeywordCode(t *testing.T) {
	c := classifyColumn("cpt_code", "VARCHAR", []string{"99213", "80053", "71046"})
	if c.Kind != ValueKeyword {
		t.Fatalf("expected keyword, got %+v", c)
	}
	if !c.Options.Lookup || !c.Options.Prefix || c.Options.MinPrefix != 3 {
		t.Fatalf("expected code-like keyword options: %+v", c.Options)
	}
}

func TestClassifyColumn_FreeText(t *testing.T) {
	c := classifyColumn("ld", "VARCHAR", []string{"Office or other outpatient visit, established patient", "Comprehensive metabolic panel"})
	if c.Kind != ValueText || c.Options.Kind != FieldText || !c.Options.Indexed {
		t.Fatalf("expected free text classification, got %+v", c)
	}
}

func TestClassifyColumn_TextByNameHint(t *testing.T) {
	c := classifyColumn("client_proc_desc", "VARCHAR", []string{"x"})
	if c.Kind != ValueText {
		t.Fatalf("expected name hint to force text, got %+v", c)
	}
}

func TestClassifyColumn_DateRFC3339(t *testing.T) {
	c := classifyColumn("effective_date", "DATE", []string{"2024-01-02T00:00:00Z", "2024-02-15T00:00:00Z"})
	if c.Kind != ValueTimeUnix || c.Layout != time.RFC3339Nano || c.Options.Kind != FieldTime {
		t.Fatalf("unexpected date classification: %+v", c)
	}
}

func TestClassifyColumn_DatePlainLayout(t *testing.T) {
	c := classifyColumn("effective_date", "DATE", []string{"2024-01-02", "2024-02-15"})
	if c.Kind != ValueTimeUnix || c.Layout != "2006-01-02" {
		t.Fatalf("unexpected date classification: %+v", c)
	}
}

func TestClassifyColumn_DateUnparseableFallsBackToKeyword(t *testing.T) {
	c := classifyColumn("effective_date", "DATE", []string{"not-a-date", "also-not"})
	if c.Kind != ValueKeyword {
		t.Fatalf("expected fallback to keyword, got %+v", c)
	}
}

func TestClassifyColumn_Bool(t *testing.T) {
	c := classifyColumn("is_active", "BOOL", []string{"true", "false"})
	if c.Kind != ValueKeyword || c.Options.Kind != FieldKeyword {
		t.Fatalf("unexpected bool classification: %+v", c)
	}
}

// ---------------------------------------------------------------------------
// InferSQLColumns / AutoSchema / BindSQLColumns (fake DB)
// ---------------------------------------------------------------------------

func TestInferSQLColumns_ExcludesIDAndSeq(t *testing.T) {
	db := openFakeDB(t, chargeMasterFakeDataset())
	cols, err := InferSQLColumns(context.Background(), db, "SELECT * FROM charge_master", nil, "id", "id", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(cols) != 6 {
		t.Fatalf("expected 6 columns (excluding id), got %d: %+v", len(cols), cols)
	}
	for _, c := range cols {
		if c.Column == "id" {
			t.Fatalf("id column should have been excluded: %+v", cols)
		}
	}
}

func TestAutoPagedSQLQuery_EndToEnd(t *testing.T) {
	db := openFakeDB(t, chargeMasterFakeDataset())
	page := func(lastSeq uint64, limit int) (string, []any) {
		return "SELECT * FROM charge_master WHERE id > ? ORDER BY id LIMIT ?", []any{lastSeq, limit}
	}

	ix, src, err := AutoPagedSQLQuery(context.Background(), Config{InitialCapacity: 16}, db, page, "id", "id", 100)
	if err != nil {
		t.Fatalf("AutoPagedSQLQuery: %v", err)
	}
	defer ix.Close()

	if len(src.Columns) != 6 {
		t.Fatalf("expected 6 bound columns, got %d: %+v", len(src.Columns), src.Columns)
	}
	for _, c := range src.Columns {
		if c.Field == InvalidFieldID {
			t.Fatalf("column %q did not resolve to a schema field", c.Column)
		}
	}

	stats, err := ix.IndexFrom(context.Background(), src, BulkOptions{Name: "charge_master", BatchSize: 10})
	if err != nil {
		t.Fatalf("IndexFrom: %v", err)
	}
	if stats.Indexed != 3 {
		t.Fatalf("expected 3 indexed rows, got %+v", stats)
	}

	_, hits := ix.SearchInto(SearchRequest{Query: Term{Field: "cpt_code", Value: "99213"}, Limit: 10}, nil)
	if len(hits) != 1 {
		t.Fatalf("expected 1 hit for cpt_code=99213, got %d", len(hits))
	}

	_, hits = ix.SearchInto(SearchRequest{Query: Simple("ld", "metabolic panel"), Limit: 10}, nil)
	if len(hits) != 1 {
		t.Fatalf("expected 1 hit for free-text search on ld, got %d", len(hits))
	}
}

func TestAutoPagedSQLQuery_RespectsExplicitSchemaOverride(t *testing.T) {
	db := openFakeDB(t, chargeMasterFakeDataset())
	page := func(lastSeq uint64, limit int) (string, []any) {
		return "SELECT * FROM charge_master WHERE id > ? ORDER BY id LIMIT ?", []any{lastSeq, limit}
	}
	cfg := Config{
		InitialCapacity: 16,
		Schema: Schema{Fields: map[string]FieldOptions{
			// Force cpt_code to plain keyword lookup without prefix expansion.
			"cpt_code": {Kind: FieldKeyword, Lookup: true},
		}},
	}
	ix, src, err := AutoPagedSQLQuery(context.Background(), cfg, db, page, "id", "id", 100)
	if err != nil {
		t.Fatalf("AutoPagedSQLQuery: %v", err)
	}
	defer ix.Close()

	var found bool
	for _, c := range src.Columns {
		if c.Column == "cpt_code" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected cpt_code binding to still be present: %+v", src.Columns)
	}
}

func TestAutoSQLQuery_EndToEnd(t *testing.T) {
	db := openFakeDB(t, chargeMasterFakeDataset())
	ix, src, err := AutoSQLQuery(context.Background(), Config{InitialCapacity: 16}, db, "SELECT * FROM charge_master", nil, "id", "id")
	if err != nil {
		t.Fatalf("AutoSQLQuery: %v", err)
	}
	defer ix.Close()

	stats, err := ix.IndexFrom(context.Background(), src, BulkOptions{Name: "charge_master", BatchSize: 10})
	if err != nil {
		t.Fatalf("IndexFrom: %v", err)
	}
	if stats.Indexed != 3 {
		t.Fatalf("expected 3 indexed rows, got %+v", stats)
	}
}
