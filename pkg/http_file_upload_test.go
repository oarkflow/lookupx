package pkg

import (
	"bytes"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xuri/excelize/v2"
)

func TestHTTPReloadFileSupportedFormats(t *testing.T) {
	xlsx := excelize.NewFile()
	defer xlsx.Close()
	if err := xlsx.SetSheetRow("Sheet1", "A1", &[]any{"id", "name", "quantity"}); err != nil {
		t.Fatal(err)
	}
	if err := xlsx.SetSheetRow("Sheet1", "A2", &[]any{"one", "alpha", 10}); err != nil {
		t.Fatal(err)
	}
	xlsxBytes, err := xlsx.WriteToBuffer()
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name     string
		filename string
		format   string
		data     []byte
		idField  string
		existing bool
		search   bool
	}{
		{name: "CSV existing index with primary key", filename: "records.csv", format: "csv", data: []byte("id,name,quantity\none,alpha,10\ntwo,beta,20\n"), idField: "id", existing: true, search: true},
		{name: "TSV", filename: "records.tsv", format: "tsv", data: []byte("id\tname\tquantity\none\talpha\t10\ntwo\tbeta\t20\n")},
		{name: "JSON object", filename: "record.json", format: "json", data: []byte(`{"id":"one","name":"alpha","quantity":10}`), idField: "id", search: true},
		{name: "JSON array without primary key", filename: "records.json", format: "json", data: []byte(`[{"name":"alpha","quantity":10},{"name":"beta","quantity":20}]`), search: true},
		{name: "Primary key only", filename: "key.csv", format: "csv", data: []byte("id\none\n"), idField: "id"},
		{name: "JSON Lines", filename: "records.jsonl", format: "jsonl", data: []byte("{\"id\":\"one\",\"name\":\"alpha\",\"quantity\":10}\n{\"id\":\"two\",\"name\":\"beta\",\"quantity\":20}\n")},
		{name: "XLSX", filename: "records.xlsx", format: "xlsx", data: xlsxBytes.Bytes()},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mgr := NewMultiIndexManager()
			if tc.existing {
				_, err := mgr.Register(IndexDefinition{ID: "records", Config: Config{Schema: Schema{Fields: map[string]FieldOptions{"placeholder": {Kind: FieldKeyword, Indexed: true}}}}})
				if err != nil {
					t.Fatal(err)
				}
			}
			body := &bytes.Buffer{}
			writer := multipart.NewWriter(body)
			part, err := writer.CreateFormFile("file", tc.filename)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := part.Write(tc.data); err != nil {
				t.Fatal(err)
			}
			_ = writer.WriteField("format", tc.format)
			_ = writer.WriteField("id_field", tc.idField)
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}

			req := httptest.NewRequest(http.MethodPost, "/v1/indexes/records/reload-file", body)
			req.Header.Set("Content-Type", writer.FormDataContentType())
			res, err := NewFHApp(&MultiServer{Manager: mgr}).Test(req)
			if err != nil {
				t.Fatal(err)
			}
			defer res.Body.Close()
			responseBody, err := io.ReadAll(res.Body)
			if err != nil {
				t.Fatal(err)
			}
			if res.StatusCode != http.StatusOK {
				t.Fatalf("upload returned %d: %s", res.StatusCode, responseBody)
			}
			var response struct {
				Stats BulkStats `json:"stats"`
			}
			if err := json.Unmarshal(responseBody, &response); err != nil {
				t.Fatal(err)
			}
			want := uint64(2)
			if tc.name == "JSON object" || tc.name == "XLSX" || tc.name == "Primary key only" {
				want = 1
			}
			if response.Stats.Indexed != want {
				t.Fatalf("indexed %d rows, want %d", response.Stats.Indexed, want)
			}
			ix, ok := mgr.Get("records")
			if !ok {
				t.Fatal("index disappeared after import")
			}
			if !tc.search {
				return
			}
			q, err := ix.CompileDatasourceLookup("name=alpha")
			if err != nil {
				t.Fatal(err)
			}
			result, hits := ix.SearchInto(SearchRequest{Query: q, Limit: 10, WithDocs: true}, nil)
			if result.Total != 1 || len(hits) != 1 || hits[0].Doc["name"] != "alpha" {
				t.Fatalf("imported record was not searchable: total=%d hits=%#v", result.Total, hits)
			}
		})
	}
}
