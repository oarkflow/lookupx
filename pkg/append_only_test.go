package pkg

import "testing"

func TestAppendOnlySearchKeepsResultIDs(t *testing.T) {
	ix, err := New(Config{AppendOnly: true, DisableSource: true, InitialCapacity: 1024, Schema: Schema{Fields: map[string]FieldOptions{
		"code": {Kind: FieldKeyword, Lookup: true, Lowercase: true},
		"body": {Kind: FieldText, Indexed: true, Lowercase: true},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	code := ix.FieldID("code")
	body := ix.FieldID("body")
	if err := ix.UpsertFast("row-1", func(w *RowWriter) {
		w.Keyword(code, "ABC")
		w.Text(body, "office visit")
	}); err != nil {
		t.Fatal(err)
	}
	_, hits := ix.SearchInto(SearchRequest{Query: Term{Field: "code", Value: "abc"}, Limit: 10}, nil)
	if len(hits) != 1 || hits[0].ID != "row-1" {
		t.Fatalf("unexpected hits: %+v", hits)
	}
	if _, ok := ix.Get("row-1"); ok {
		t.Fatal("append-only Get should be disabled")
	}
}

func TestBitmapPromotionDelayedUntilDenseIsSmaller(t *testing.T) {
	b := NewBitmap()
	for i := 1; i <= sparseBitmapLimit+1; i++ {
		b.Add(DocID(i * 100))
	}
	if b.isDense() {
		t.Fatal("medium-frequency sparse posting promoted too early")
	}
	if got := b.Count(); got != sparseBitmapLimit+1 {
		t.Fatalf("count=%d", got)
	}
}
