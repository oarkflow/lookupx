package pkg

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	_ "github.com/oarkflow/squealx/drivers/sqlite"
)

func TestSQLIntegrationsCreateIndexesWithoutPrimaryKey(t *testing.T) {
	dsn := filepath.Join(t.TempDir(), "source.db")
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE records (name TEXT, quantity INTEGER)`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO records VALUES ('alpha', 1), ('beta', 2), ('gamma', 3)`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE keyed_records (code TEXT PRIMARY KEY, name TEXT); INSERT INTO keyed_records VALUES ('a', 'alpha'), ('b', 'beta'), ('c', 'gamma')`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name      string
		id        string
		path      string
		body      map[string]any
		count     uint64
		wantDocID string
	}{
		{
			name: "SQL query", id: "query_rows", path: "reload-sql", count: 3,
			body: map[string]any{"driver": "sqlite3", "dsn": dsn, "query": "SELECT name, quantity FROM records"},
		},
		{
			name: "SQL table with offset paging", id: "table_rows", path: "reload-table", count: 3,
			body: map[string]any{"driver": "sqlite3", "dsn": dsn, "table": "records", "page_size": 2},
		},
		{
			name: "SQL table with text primary key", id: "keyed_rows", path: "reload-table", count: 3, wantDocID: "c",
			body: map[string]any{"driver": "sqlite3", "dsn": dsn, "table": "keyed_records", "id_column": "code", "page_size": 2},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			payload, err := json.Marshal(tc.body)
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(http.MethodPost, "/v1/indexes/"+tc.id+"/"+tc.path, bytes.NewReader(payload))
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			manager := NewMultiIndexManager()
			serveFHTest(t, response, &MultiServer{Manager: manager}, request)
			if response.Code != http.StatusOK {
				t.Fatalf("reload returned %d: %s", response.Code, response.Body.String())
			}
			var result struct {
				Stats BulkStats `json:"stats"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
			if result.Stats.Indexed != tc.count {
				t.Fatalf("indexed %d rows, want %d", result.Stats.Indexed, tc.count)
			}
			ix, ok := manager.Get(tc.id)
			if !ok {
				t.Fatal("integration did not create the named index")
			}
			q, err := ix.CompileDatasourceLookup("name=gamma")
			if err != nil {
				t.Fatal(err)
			}
			_, hits := ix.SearchInto(SearchRequest{Query: q, Limit: 1, WithDocs: true}, nil)
			if len(hits) != 1 || hits[0].ID == "" || hits[0].Doc["name"] != "gamma" {
				t.Fatalf("keyless row is not searchable: %#v", hits)
			}
			if tc.wantDocID != "" && hits[0].ID != tc.wantDocID {
				t.Fatalf("record ID=%q, want %q", hits[0].ID, tc.wantDocID)
			}
		})
	}
}
