package pkg

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// ---------------------------------------------------------------------------
// Registry tests
// ---------------------------------------------------------------------------

func TestDatasourceRegistryRegisterAndGet(t *testing.T) {
	r := &DatasourceRegistry{factories: make(map[string]DatasourceFactory)}
	factory := func(config map[string]any, params map[string]any) (Datasource, error) {
		return nil, nil
	}
	if err := r.Register("test_type", factory); err != nil {
		t.Fatalf("register: %v", err)
	}
	if !r.Has("test_type") {
		t.Fatal("expected Has to return true")
	}
	if r.Has("other_type") {
		t.Fatal("expected Has to return false for unregistered type")
	}
	f, ok := r.Get("test_type")
	if !ok || f == nil {
		t.Fatal("expected factory to be returned")
	}
}

func TestDatasourceRegistryCaseInsensitive(t *testing.T) {
	r := &DatasourceRegistry{factories: make(map[string]DatasourceFactory)}
	factory := func(config map[string]any, params map[string]any) (Datasource, error) {
		return nil, nil
	}
	if err := r.Register("SQL_Table", factory); err != nil {
		t.Fatalf("register: %v", err)
	}
	if !r.Has("sql_table") {
		t.Fatal("expected case-insensitive lookup to work")
	}
	if !r.Has("SQL_TABLE") {
		t.Fatal("expected case-insensitive lookup to work")
	}
}

func TestDatasourceRegistryDuplicate(t *testing.T) {
	r := &DatasourceRegistry{factories: make(map[string]DatasourceFactory)}
	factory := func(config map[string]any, params map[string]any) (Datasource, error) {
		return nil, nil
	}
	if err := r.Register("dup", factory); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := r.Register("dup", factory); err == nil {
		t.Fatal("expected error on duplicate registration")
	}
}

func TestDatasourceRegistryTypes(t *testing.T) {
	r := &DatasourceRegistry{factories: make(map[string]DatasourceFactory)}
	factory := func(config map[string]any, params map[string]any) (Datasource, error) {
		return nil, nil
	}
	r.Register("zebra", factory)
	r.Register("alpha", factory)
	r.Register("middle", factory)
	types := r.Types()
	if len(types) != 3 {
		t.Fatalf("expected 3 types, got %d", len(types))
	}
	if types[0] != "alpha" || types[1] != "middle" || types[2] != "zebra" {
		t.Fatalf("expected sorted types, got %v", types)
	}
}

func TestDatasourceRegistryCreateUnknown(t *testing.T) {
	r := &DatasourceRegistry{factories: make(map[string]DatasourceFactory)}
	_, err := r.Create("nonexistent", nil, nil)
	if err == nil {
		t.Fatal("expected error for unknown type")
	}
}

// ---------------------------------------------------------------------------
// Config helper tests
// ---------------------------------------------------------------------------

func TestConfigHelpers(t *testing.T) {
	m := map[string]any{
		"str":    "hello",
		"num":    42,
		"numf":   3.14,
		"flag":   true,
		"nested": map[string]any{"key": "val"},
		"list":   []any{"a", "b"},
	}

	s, err := ConfigString(m, "str")
	if err != nil || s != "hello" {
		t.Fatalf("ConfigString: %v %q", err, s)
	}
	_, err = ConfigString(m, "missing")
	if err == nil {
		t.Fatal("expected error for missing key")
	}

	n, err := ConfigInt(m, "num")
	if err != nil || n != 42 {
		t.Fatalf("ConfigInt: %v %d", err, n)
	}
	nf, err := ConfigInt(m, "numf")
	if err != nil || nf != 3 {
		t.Fatalf("ConfigInt from float: %v %d", err, nf)
	}

	b, err := ConfigBool(m, "flag")
	if err != nil || !b {
		t.Fatalf("ConfigBool: %v %v", err, b)
	}

	sub, err := ConfigMap(m, "nested")
	if err != nil || sub["key"] != "val" {
		t.Fatalf("ConfigMap: %v %v", err, sub)
	}

	l, err := ConfigList(m, "list")
	if err != nil || len(l) != 2 {
		t.Fatalf("ConfigList: %v %v", err, l)
	}

	// Or variants
	if ConfigStringOr(m, "missing", "default") != "default" {
		t.Fatal("ConfigStringOr default failed")
	}
	if ConfigIntOr(m, "missing", 99) != 99 {
		t.Fatal("ConfigIntOr default failed")
	}
	if ConfigBoolOr(m, "missing", true) != true {
		t.Fatal("ConfigBoolOr default failed")
	}
}

func TestApplyParams(t *testing.T) {
	config := map[string]any{"a": 1, "b": 2}
	params := map[string]any{"b": 99, "c": 100}
	merged := ApplyParams(config, params)
	if merged["a"] != 1 {
		t.Fatal("expected a=1")
	}
	if merged["b"] != 99 {
		t.Fatal("expected b=99 (overridden)")
	}
	if merged["c"] != 100 {
		t.Fatal("expected c=100 (added)")
	}
}

func TestInterpolateParams(t *testing.T) {
	s := "SELECT * FROM t WHERE date >= '${start}' AND date <= '${end}'"
	params := map[string]any{"start": "2025-01-01", "end": "2025-12-31"}
	result := interpolateParams(s, params)
	expected := "SELECT * FROM t WHERE date >= '2025-01-01' AND date <= '2025-12-31'"
	if result != expected {
		t.Fatalf("interpolation mismatch:\ngot:  %s\nwant: %s", result, expected)
	}
}

// ---------------------------------------------------------------------------
// Datasource type validation tests
// ---------------------------------------------------------------------------

func TestSQLTableDatasourceValidation(t *testing.T) {
	factory := func(config map[string]any, params map[string]any) (Datasource, error) {
		return newSQLTableDatasource(config, params)
	}

	// Missing driver
	_, err := factory(map[string]any{
		"id":    "test",
		"kind":  "sql_table",
		"dsn":   "localhost",
		"table": "t",
	}, nil)
	if err == nil {
		t.Fatal("expected error for missing driver")
	}

	// Missing dsn
	_, err = factory(map[string]any{
		"id":     "test",
		"kind":   "sql_table",
		"driver": "postgres",
		"table":  "t",
	}, nil)
	if err == nil {
		t.Fatal("expected error for missing dsn")
	}

	// Missing table
	_, err = factory(map[string]any{
		"id":     "test",
		"kind":   "sql_table",
		"driver": "postgres",
		"dsn":    "localhost",
	}, nil)
	if err == nil {
		t.Fatal("expected error for missing table")
	}

	// Valid config
	ds, err := factory(map[string]any{
		"id":     "test",
		"kind":   "sql_table",
		"driver": "postgres",
		"dsn":    "localhost",
		"table":  "products",
	}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ds.ID() != "test" {
		t.Fatalf("expected ID=test, got %s", ds.ID())
	}
	if ds.Type() != "sql_table" {
		t.Fatalf("expected Type=sql_table, got %s", ds.Type())
	}
}

func TestSQLViewDatasourceValidation(t *testing.T) {
	factory := func(config map[string]any, params map[string]any) (Datasource, error) {
		return newSQLViewDatasource(config, params)
	}

	_, err := factory(map[string]any{
		"id":     "test_view",
		"kind":   "sql_view",
		"driver": "mysql",
		"dsn":    "localhost",
	}, nil)
	if err == nil {
		t.Fatal("expected error for missing view")
	}

	ds, err := factory(map[string]any{
		"id":     "test_view",
		"kind":   "sql_view",
		"driver": "mysql",
		"dsn":    "localhost",
		"view":   "v_summary",
	}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ds.Type() != "sql_view" {
		t.Fatalf("expected Type=sql_view, got %s", ds.Type())
	}
}

func TestSQLQueryDatasourceValidation(t *testing.T) {
	factory := func(config map[string]any, params map[string]any) (Datasource, error) {
		return newSQLQueryDatasource(config, params)
	}

	_, err := factory(map[string]any{
		"id":     "test_q",
		"kind":   "sql_query",
		"driver": "pgx",
		"dsn":    "localhost",
	}, nil)
	if err == nil {
		t.Fatal("expected error for missing query")
	}

	ds, err := factory(map[string]any{
		"id":     "test_q",
		"kind":   "sql_query",
		"driver": "pgx",
		"dsn":    "localhost",
		"query":  "SELECT 1",
	}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ds.Type() != "sql_query" {
		t.Fatalf("expected Type=sql_query, got %s", ds.Type())
	}
}

func TestSQLFileDatasourceValidation(t *testing.T) {
	factory := func(config map[string]any, params map[string]any) (Datasource, error) {
		return newSQLFileDatasource(config, params)
	}

	_, err := factory(map[string]any{
		"id":     "test_f",
		"kind":   "sql_file",
		"driver": "sqlite3",
		"dsn":    ":memory:",
	}, nil)
	if err == nil {
		t.Fatal("expected error for missing file")
	}

	ds, err := factory(map[string]any{
		"id":     "test_f",
		"kind":   "sql_file",
		"driver": "sqlite3",
		"dsn":    ":memory:",
		"file":   "./test.sql",
	}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ds.Type() != "sql_file" {
		t.Fatalf("expected Type=sql_file, got %s", ds.Type())
	}
}

func TestCSVFileDatasourceValidation(t *testing.T) {
	factory := func(config map[string]any, params map[string]any) (Datasource, error) {
		return newCSVFileDatasource(config, params)
	}

	_, err := factory(map[string]any{
		"id": "test_csv",
	}, nil)
	if err == nil {
		t.Fatal("expected error for missing file")
	}

	ds, err := factory(map[string]any{
		"id":       "test_csv",
		"kind":     "csv",
		"file":     "./data.csv",
		"id_column": "id",
	}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ds.Type() != "csv" {
		t.Fatalf("expected Type=csv, got %s", ds.Type())
	}
}

func TestJSONLFileDatasourceValidation(t *testing.T) {
	factory := func(config map[string]any, params map[string]any) (Datasource, error) {
		return newJSONLFileDatasource(config, params)
	}

	ds, err := factory(map[string]any{
		"id":      "test_jsonl",
		"kind":    "jsonl",
		"file":    "./data.jsonl",
		"id_field": "event_id",
	}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ds.Type() != "jsonl" {
		t.Fatalf("expected Type=jsonl, got %s", ds.Type())
	}
}

func TestHTTPDatasourceValidation(t *testing.T) {
	factory := func(config map[string]any, params map[string]any) (Datasource, error) {
		return newHTTPDatasource(config, params)
	}

	_, err := factory(map[string]any{
		"id": "test_http",
	}, nil)
	if err == nil {
		t.Fatal("expected error for missing url")
	}

	ds, err := factory(map[string]any{
		"id":      "test_http",
		"kind":    "http",
		"url":     "https://api.example.com/data",
		"id_field": "id",
	}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ds.Type() != "http" {
		t.Fatalf("expected Type=http, got %s", ds.Type())
	}
}

// ---------------------------------------------------------------------------
// Global registry tests
// ---------------------------------------------------------------------------

func TestGlobalRegistryHasBuiltinTypes(t *testing.T) {
	expected := []string{"sql_table", "sql_view", "sql_query", "sql_file", "csv", "jsonl", "http"}
	for _, typ := range expected {
		if !GlobalRegistry.Has(typ) {
			t.Errorf("expected GlobalRegistry to have type %q", typ)
		}
	}
}

// ---------------------------------------------------------------------------
// BCL config parsing test (with minimal BCL input)
// ---------------------------------------------------------------------------

func TestParseBCLBytesMinimal(t *testing.T) {
	src := []byte(`
addr "localhost:9090"
data_dir "./data"

datasource "test_ds" {
  kind "csv"
  file "./test.csv"
  id_column "id"

  column "id" {
    field "id"
    kind "keyword"
  }
  column "name" {
    field "name"
    kind "text"
  }
}

index "test_idx" {
  schema "record"
  datasource "test_ds"

  bulk {
    batch_size 1024
  }
}
`)
	cfg, err := ParseBCLBytes(src, nil)
	if err != nil {
		t.Fatalf("ParseBCLBytes: %v", err)
	}
	if cfg.Addr != "localhost:9090" {
		t.Fatalf("expected addr=localhost:9090, got %s", cfg.Addr)
	}
	if cfg.DataDir != "./data" {
		t.Fatalf("expected data_dir=./data, got %s", cfg.DataDir)
	}
	if len(cfg.Datasources) != 1 {
		t.Fatalf("expected 1 datasource, got %d", len(cfg.Datasources))
	}
	ds := cfg.Datasources[0]
	if ds.ID != "test_ds" {
		t.Fatalf("expected datasource ID=test_ds, got %s", ds.ID)
	}
	if ds.Kind != "csv" {
		t.Fatalf("expected datasource Kind=csv, got %s", ds.Kind)
	}
	if ds.File != "./test.csv" {
		t.Fatalf("expected file=./test.csv, got %s", ds.File)
	}
	if len(cfg.Indexes) != 1 {
		t.Fatalf("expected 1 index, got %d", len(cfg.Indexes))
	}
	idx := cfg.Indexes[0]
	if idx.ID != "test_idx" {
		t.Fatalf("expected index ID=test_idx, got %s", idx.ID)
	}
	if idx.DatasourceID != "test_ds" {
		t.Fatalf("expected index datasource=test_ds, got %s", idx.DatasourceID)
	}
	if idx.Bulk.BatchSize != 1024 {
		t.Fatalf("expected bulk.batch_size=1024, got %d", idx.Bulk.BatchSize)
	}
}

func TestParseBCLBytesMultipleDatasources(t *testing.T) {
	src := []byte(`
datasource "ds_a" {
  kind "csv"
  file "./a.csv"
  id_column "id"
}
datasource "ds_b" {
  kind "jsonl"
  file "./b.jsonl"
  id_field "event_id"
}
index "idx_a" {
  datasource "ds_a"
}
index "idx_b" {
  datasource "ds_b"
}
`)
	cfg, err := ParseBCLBytes(src, nil)
	if err != nil {
		t.Fatalf("ParseBCLBytes: %v", err)
	}
	if len(cfg.Datasources) != 2 {
		t.Fatalf("expected 2 datasources, got %d", len(cfg.Datasources))
	}
	if len(cfg.Indexes) != 2 {
		t.Fatalf("expected 2 indexes, got %d", len(cfg.Indexes))
	}
}

func TestParseBCLBytesWithEnvInterpolation(t *testing.T) {
	t.Setenv("TEST_DSN", "postgres://testhost/db")
	src := []byte(`
datasource "env_ds" {
  kind "sql_table"
  driver "postgres"
  dsn env("TEST_DSN")
  table "test_table"
  id_column "id"
}
index "env_idx" {
  datasource "env_ds"
}
`)
	cfg, err := ParseBCLBytes(src, nil)
	if err != nil {
		t.Fatalf("ParseBCLBytes: %v", err)
	}
	if len(cfg.Datasources) != 1 {
		t.Fatalf("expected 1 datasource, got %d", len(cfg.Datasources))
	}
	if cfg.Datasources[0].DSN != "postgres://testhost/db" {
		t.Fatalf("expected DSN interpolated, got %s", cfg.Datasources[0].DSN)
	}
}

// ---------------------------------------------------------------------------
// DatasourceDef.ToMap conversion test
// ---------------------------------------------------------------------------

func TestDatasourceDefToMap(t *testing.T) {
	ds := DatasourceDef{
		ID:         "products",
		Kind:       "sql_table",
		Driver:     "postgres",
		DSN:        "localhost",
		Table:      "products",
		IDColumn:   "sku",
		PageSize:   50000,
		Columns: []ColumnDef{
			{Column: "sku", Field: "sku", Kind: "keyword"},
			{Column: "price", Field: "price", Kind: "number"},
		},
	}
	m := ds.ToMap()
	if m["id"] != "products" {
		t.Fatal("expected id=products")
	}
	if m["kind"] != "sql_table" {
		t.Fatal("expected kind=sql_table")
	}
	if m["table"] != "products" {
		t.Fatal("expected table=products")
	}
	cols, ok := m["columns"].([]any)
	if !ok || len(cols) != 2 {
		t.Fatalf("expected 2 columns, got %v", m["columns"])
	}
}

// ---------------------------------------------------------------------------
// BulkDef conversion test
// ---------------------------------------------------------------------------

func TestBulkDefToBulkOptions(t *testing.T) {
	bd := BulkDef{
		BatchSize:       8192,
		CheckpointEvery: 1000,
		CheckpointPath:  "./cp.json",
		Resume:          true,
	}
	opt := bd.ToBulkOptions()
	if opt.BatchSize != 8192 {
		t.Fatalf("expected BatchSize=8192, got %d", opt.BatchSize)
	}
	if opt.CheckpointEvery != 1000 {
		t.Fatalf("expected CheckpointEvery=1000, got %d", opt.CheckpointEvery)
	}
	if opt.Resume != true {
		t.Fatal("expected Resume=true")
	}
	if _, ok := opt.Checkpoint.(FileCheckpoint); !ok {
		t.Fatal("expected FileCheckpoint")
	}

	// Default values
	bd2 := BulkDef{}
	opt2 := bd2.ToBulkOptions()
	if opt2.BatchSize != 65536 {
		t.Fatalf("expected default BatchSize=65536, got %d", opt2.BatchSize)
	}
	if opt2.CheckpointEvery != 65536 {
		t.Fatalf("expected default CheckpointEvery=65536, got %d", opt2.CheckpointEvery)
	}
}

// ---------------------------------------------------------------------------
// ConfigBuilder test
// ---------------------------------------------------------------------------

func TestConfigBuilder(t *testing.T) {
	cfg := NewConfigBuilder().
		Server(":8080", "./data", []string{"key1"}).
		AddDatasource("ds1", "csv", map[string]any{
			"file":      "./data.csv",
			"id_column": "id",
		}).
		AddIndex("idx1", "record", "ds1", WithInitialCapacity(1000)).
		Build()

	if cfg.Addr != ":8080" {
		t.Fatalf("expected addr=:8080, got %s", cfg.Addr)
	}
	if cfg.DataDir != "./data" {
		t.Fatalf("expected data_dir=./data, got %s", cfg.DataDir)
	}
	if len(cfg.Datasources) != 1 {
		t.Fatalf("expected 1 datasource, got %d", len(cfg.Datasources))
	}
	if len(cfg.Indexes) != 1 {
		t.Fatalf("expected 1 index, got %d", len(cfg.Indexes))
	}
	if cfg.Indexes[0].InitialCapacity != 1000 {
		t.Fatalf("expected capacity=1000, got %d", cfg.Indexes[0].InitialCapacity)
	}
}

// ---------------------------------------------------------------------------
// BCL file parsing test (requires real BCL library)
// ---------------------------------------------------------------------------

func TestLoadBCLConfigFile(t *testing.T) {
	tmpDir := t.TempDir()
	bclPath := filepath.Join(tmpDir, "test.bcl")
	content := `
datasource "file_ds" {
  kind "csv"
  file "./test.csv"
  id_column "id"

  column "id" {
    field "id"
    kind "keyword"
  }
}
index "file_idx" {
  schema "record"
  datasource "file_ds"
}
`
	if err := os.WriteFile(bclPath, []byte(content), 0644); err != nil {
		t.Fatalf("write bcl: %v", err)
	}

	cfg, err := LoadBCLConfig(bclPath, nil)
	if err != nil {
		t.Fatalf("LoadBCLConfig: %v", err)
	}
	if len(cfg.Datasources) != 1 {
		t.Fatalf("expected 1 datasource, got %d", len(cfg.Datasources))
	}
	if cfg.Datasources[0].ID != "file_ds" {
		t.Fatalf("expected ID=file_ds, got %s", cfg.Datasources[0].ID)
	}
}

// ---------------------------------------------------------------------------
// BuildFromBCLConfig integration test
// ---------------------------------------------------------------------------

func TestBuildFromBCLConfig(t *testing.T) {
	src := []byte(`
datasource "mem_ds" {
  kind "csv"
  file "./test.csv"
  id_column "id"
}
index "mem_idx" {
  schema "record"
  datasource "mem_ds"
  initial_capacity 100
}
`)
	cfg, err := ParseBCLBytes(src, nil)
	if err != nil {
		t.Fatalf("ParseBCLBytes: %v", err)
	}
	mgr, err := BuildFromBCLConfig(context.Background(), cfg, nil)
	if err != nil {
		t.Fatalf("BuildFromBCLConfig: %v", err)
	}
	if mgr == nil {
		t.Fatal("expected non-nil manager")
	}
	list := mgr.List()
	if len(list) != 1 {
		t.Fatalf("expected 1 index, got %d", len(list))
	}
	if list[0].ID != "mem_idx" {
		t.Fatalf("expected index ID=mem_idx, got %s", list[0].ID)
	}
}
