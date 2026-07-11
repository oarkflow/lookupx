package pkg

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestMultiServerSearchReturnsBulkDatasourceRecordsByDefault(t *testing.T) {
	mgr := NewMultiIndexManager()
	ix, err := New(Config{Schema: TupleLookupSchema()})
	if err != nil {
		t.Fatal(err)
	}
	r := SourceRecord{ID: "row-1", Seq: 1}
	r.AddKeyword(ix.FieldID("term"), "alpha", true)
	r.AddKeyword(ix.FieldID("group_id"), "4", true)
	r.AddKeyword(ix.FieldID("date_key"), "2026-01-01", true)
	if _, err := ix.IndexFrom(context.Background(), SliceSource{Records: []SourceRecord{r}}, BulkOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := mgr.AddIndex("records", ix); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/indexes/records/search", strings.NewReader(`{"type":"term","field":"term","value":"alpha"}`))
	res := httptest.NewRecorder()
	(&MultiServer{Manager: mgr}).ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", res.Code, res.Body.String())
	}
	var body struct {
		Result Result `json:"result"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Result.Hits) != 1 || body.Result.Hits[0].Doc["term"] != "alpha" {
		t.Fatalf("expected populated record, got %#v", body.Result.Hits)
	}
}

func TestReloadWithConfigReplacesOldDatasourceSchema(t *testing.T) {
	mgr := NewMultiIndexManager()
	if _, err := mgr.Register(IndexDefinition{ID: "cpt", Config: Config{Schema: TupleLookupSchema()}}); err != nil {
		t.Fatal(err)
	}
	cfg := Config{Schema: Schema{Fields: map[string]FieldOptions{
		"ld":             {Kind: FieldText, Indexed: true, Stored: true},
		"cpt_code":       {Kind: FieldKeyword, Indexed: true, Stored: true, Lookup: true},
		"effective_date": {Kind: FieldTime, Indexed: true, Stored: true},
	}}}
	_, err := mgr.ReloadWithConfig(context.Background(), "cpt", cfg, func(ix *Index) (Source, error) {
		r := SourceRecord{ID: "943843", Seq: 1}
		r.AddText(ix.FieldID("ld"), "Office visit", false)
		r.AddKeyword(ix.FieldID("cpt_code"), "99213", false)
		r.AddUnixTime(ix.FieldID("effective_date"), 1514764800)
		return SliceSource{Records: []SourceRecord{r}}, nil
	}, BulkOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ix, _ := mgr.Get("cpt")
	fields := ix.SchemaFields()
	if _, old := fields["term"]; old {
		t.Fatalf("old tuple schema survived datasource reload: %#v", fields)
	}
	for _, name := range []string{"ld", "cpt_code", "effective_date"} {
		if _, ok := fields[name]; !ok {
			t.Fatalf("missing datasource field %q: %#v", name, fields)
		}
	}
	_, hits := ix.SearchInto(SearchRequest{Query: Term{Field: "cpt_code", Value: "99213"}, Limit: 1, WithDocs: true}, nil)
	if len(hits) != 1 || hits[0].Doc["ld"] != "Office visit" {
		t.Fatalf("unexpected datasource result: %#v", hits)
	}
}

func TestLookupFiltersDatasourceTextAndNumericFields(t *testing.T) {
	mgr := NewMultiIndexManager()
	ix, err := New(Config{Schema: Schema{Fields: map[string]FieldOptions{
		"ld":        {Kind: FieldText, Indexed: true, Stored: true, Lowercase: true, Prefix: true, Suffix: true, Ngram: true, Fuzzy: true, MinGram: 3, MaxGram: 3},
		"work_item": {Kind: FieldInt, Indexed: true, Stored: true, Sortable: true},
		"cpt_code":  {Kind: FieldKeyword, Indexed: true, Stored: true, Lookup: true, Lowercase: true},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	records := []SourceRecord{
		{ID: "943846", Seq: 1, Values: []SourceValue{
			{Field: ix.FieldID("ld"), Kind: ValueText, String: "943846"},
			{Field: ix.FieldID("work_item"), Kind: ValueNumber, Number: 37},
			{Field: ix.FieldID("cpt_code"), Kind: ValueKeyword, String: "99213"},
		}},
		{ID: "other", Seq: 2, Values: []SourceValue{
			{Field: ix.FieldID("ld"), Kind: ValueText, String: "different record"},
			{Field: ix.FieldID("work_item"), Kind: ValueNumber, Number: 99},
		}},
		{ID: "zero", Seq: 3, Values: []SourceValue{
			{Field: ix.FieldID("work_item"), Kind: ValueNumber, Number: 0},
		}},
	}
	if _, err := ix.IndexFrom(context.Background(), SliceSource{Records: records}, BulkOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := mgr.AddIndex("cpt", ix); err != nil {
		t.Fatal(err)
	}
	server := &MultiServer{Manager: mgr}
	for _, filter := range []string{"ld=943846", "work_item=37", "ld=943846&work_item=37"} {
		req := httptest.NewRequest(http.MethodGet, "/v1/indexes/cpt/lookup?"+filter, nil)
		res := httptest.NewRecorder()
		server.ServeHTTP(res, req)
		if res.Code != http.StatusOK {
			t.Fatalf("filter %q: status=%d body=%s", filter, res.Code, res.Body.String())
		}
		var body struct {
			Hits []Hit `json:"hits"`
		}
		if err := json.Unmarshal(res.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if len(body.Hits) != 1 || body.Hits[0].ID != "943846" || body.Hits[0].Doc["work_item"] != float64(37) {
			t.Fatalf("filter %q returned %#v", filter, body.Hits)
		}
	}
}

func TestLookupDatasourceOperators(t *testing.T) {
	ix, err := New(Config{Schema: Schema{Fields: map[string]FieldOptions{
		"name":   {Kind: FieldKeyword, Indexed: true, Stored: true, Lowercase: true, Prefix: true, Suffix: true, Ngram: true, Fuzzy: true, MinGram: 3, MaxGram: 3},
		"amount": {Kind: FieldFloat, Indexed: true, Stored: true, Sortable: true},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	records := []SourceRecord{
		{ID: "a", Values: []SourceValue{{Field: ix.FieldID("name"), Kind: ValueKeyword, String: "Alpha service"}, {Field: ix.FieldID("amount"), Kind: ValueNumber, Number: 0}}},
		{ID: "b", Values: []SourceValue{{Field: ix.FieldID("name"), Kind: ValueKeyword, String: "Beta service"}, {Field: ix.FieldID("amount"), Kind: ValueNumber, Number: 37}}},
		{ID: "c", Values: []SourceValue{{Field: ix.FieldID("name"), Kind: ValueKeyword, String: "Gamma plan"}, {Field: ix.FieldID("amount"), Kind: ValueNumber, Number: 99}}},
		{ID: "d"},
	}
	if _, err := ix.IndexFrom(context.Background(), SliceSource{Records: records}, BulkOptions{}); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		raw  string
		want int
	}{
		{"name__eq=Alpha+service", 1}, {"name__ne=Alpha+service", 3},
		{"name__contains=service", 2}, {"name__not_contains=service", 2},
		{"name__starts_with=beta", 1}, {"name__ends_with=plan", 1},
		{"name__in=Alpha+service%2CGamma+plan", 2}, {"name__not_in=Alpha+service%2CGamma+plan", 2},
		{"name__exists=", 3}, {"name__missing=", 1},
		{"amount__not_zero=", 2}, {"amount__gt=37", 1}, {"amount__gte=37", 2},
		{"amount__lt=37", 1}, {"amount__lte=37", 2}, {"amount__between=30%2C40", 1},
	}
	for _, tc := range tests {
		q, err := ix.CompileDatasourceLookup(tc.raw)
		if err != nil {
			t.Fatalf("%s: %v", tc.raw, err)
		}
		if got := ix.Count(q); got != tc.want {
			t.Errorf("%s: got %d, want %d", tc.raw, got, tc.want)
		}
	}
}

func TestLookupRejectsUnknownDatasourceField(t *testing.T) {
	ix, err := New(Config{Schema: Schema{Fields: map[string]FieldOptions{"ld": {Kind: FieldText, Indexed: true}}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ix.CompileDatasourceLookup("group_id=4"); err == nil {
		t.Fatal("expected unknown old-schema field to be rejected")
	}
}

func TestMultiIndexReloadAndLookup(t *testing.T) {
	mgr := NewMultiIndexManager()
	if err := RegisterDemoIndexes(mgr, 1000); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"dataset_a", "dataset_b", "dataset_c"} {
		_, err := mgr.ReloadFromFactory(context.Background(), id, func(indexID string) SourceFactory {
			return func(ix *Index) (Source, error) { return testDemoSource(ix, indexID, 1000), nil }
		}(id), BulkOptions{Name: id, BatchSize: 512})
		if err != nil {
			t.Fatalf("reload %s: %v", id, err)
		}
		ix, ok := mgr.Get(id)
		if !ok {
			t.Fatalf("missing %s", id)
		}
		if ix.Count(ParseLookupQuery("term="+testFirstTerm(id)+"&group_id=4&date_key=2026-01-01")) == 0 {
			t.Fatalf("expected hits for %s", id)
		}
	}
}

func testDemoSource(ix *Index, indexID string, rows int) Source {
	term := ix.FieldID("term")
	work := ix.FieldID("group_id")
	date_key := ix.FieldID("date_key")
	partition := ix.FieldID("partition_id")
	records := make([]SourceRecord, rows)
	for i := 1; i <= rows; i++ {
		code := testFirstTerm(indexID)
		wi, day := strconv.Itoa((i%10)+1), fmt.Sprintf("2026-01-%02d", (i%28)+1)
		if i%100 == 23 {
			wi, day = "4", "2026-01-01"
		}
		records[i-1].ID = fmt.Sprintf("%s-%06d", indexID, i)
		records[i-1].Seq = uint64(i)
		records[i-1].Values = append(records[i-1].Values,
			SourceValue{Field: term, Kind: ValueKeyword, String: strings.ToLower(code), Normalized: true},
			SourceValue{Field: work, Kind: ValueKeyword, String: wi, Normalized: true},
			SourceValue{Field: date_key, Kind: ValueKeyword, String: day, Normalized: true},
			SourceValue{Field: partition, Kind: ValueKeyword, String: "1", Normalized: true},
		)
	}
	return SliceSource{Records: records}
}
func testFirstTerm(id string) string {
	if id == "dataset_b" {
		return "E11.9"
	}
	if id == "dataset_c" {
		return "A0428"
	}
	return "key-0013"
}

func TestLatencySummary(t *testing.T) {
	if LatencySummary(100, time.Millisecond) == "" {
		t.Fatal("empty")
	}
}
