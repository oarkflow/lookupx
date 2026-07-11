package pkg

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

type Query interface{ eval(*Index) *Bitmap }

type MatchAll struct{}

func (MatchAll) eval(ix *Index) *Bitmap {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	return ix.liveBitmapLocked()
}

type Term struct{ Field, Value string }

func (q Term) eval(ix *Index) *Bitmap { return ix.termBitmap(q.Field, q.Value) }

type Terms struct {
	Field  string
	Values []string
}

func (q Terms) eval(ix *Index) *Bitmap {
	r := NewBitmap()
	for _, v := range q.Values {
		r = r.OrInPlace(ix.termBitmap(q.Field, v))
	}
	return r
}

type Prefix struct{ Field, Value string }

func (q Prefix) eval(ix *Index) *Bitmap { return ix.prefixBitmap(q.Field, q.Value) }

type Suffix struct{ Field, Value string }

func (q Suffix) eval(ix *Index) *Bitmap { return ix.suffixBitmap(q.Field, q.Value) }

type Contains struct{ Field, Value string }

func (q Contains) eval(ix *Index) *Bitmap { return ix.ngramBitmap(q.Field, q.Value) }

// GlobalTerm performs indexed full-text retrieval across all searchable string
// fields. Each word must match at least one field; optional fuzzy expansion is
// bounded by each field's fuzzy term index.
type GlobalTerm struct {
	Words []string
	Fuzzy bool
}

func (q GlobalTerm) eval(ix *Index) *Bitmap {
	var result *Bitmap
	for _, word := range q.Words {
		perWord := NewBitmap()
		for field, opt := range ix.cfg.Schema.Fields {
			if opt.Kind != FieldText && opt.Kind != FieldKeyword && opt.Kind != FieldBool {
				continue
			}
			perWord = perWord.OrInPlace(ix.termBitmap(field, word))
			if q.Fuzzy && opt.Fuzzy {
				perWord = perWord.OrInPlace(ix.fuzzyBitmap(field, word, 1, 128))
			}
		}
		if result == nil {
			result = perWord
		} else {
			result = result.AndInPlace(perWord)
		}
		if result.Empty() {
			break
		}
	}
	if result == nil {
		return NewBitmap()
	}
	return result
}

type sourceStringFilter struct {
	Field, Value, Operator string
}

func (q sourceStringFilter) eval(ix *Index) *Bitmap {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	opt, ok := ix.cfg.Schema.Fields[q.Field]
	if !ok {
		return NewBitmap()
	}
	want := normalize(q.Value, opt.Lowercase)
	out := NewBitmapCap(ix.nextDocID)
	match := func(got string) bool {
		switch q.Operator {
		case "contains":
			return strings.Contains(got, want)
		case "starts_with":
			return strings.HasPrefix(got, want)
		case "ends_with":
			return strings.HasSuffix(got, want)
		case "fuzzy":
			return levenshtein(got, want, 1) <= 1
		}
		return false
	}
	// Stored string columns remain available when source documents are disabled
	// and preserve the complete value (unlike analyzed text postings).
	if col := ix.strings[q.Field]; col != nil {
		for id, raw := range col {
			if !ix.isDeletedOrExpiredLocked(id) && match(normalize(raw, opt.Lowercase)) {
				out.Add(id)
			}
		}
		return out
	}
	// If source documents are retained, scan them as the compatibility path.
	if !ix.cfg.DisableSource {
		for id := DocID(1); id < ix.nextDocID; id++ {
			if ix.isDeletedOrExpiredLocked(id) || int(id) >= len(ix.docs) || ix.docs[id] == nil {
				continue
			}
			raw, exists := ix.docs[id][q.Field]
			if !exists || raw == nil {
				continue
			}
			got := normalize(fmt.Sprint(raw), opt.Lowercase)
			if match(got) {
				out.Add(id)
			}
		}
		return out
	}
	// Lookup/keyword fields retain their full values as posting terms, so pattern
	// matching can still be correct without either source documents or a derived
	// pattern index. This is slower than an n-gram index but avoids extra indexing
	// cost and is only used when a pattern query is requested.
	fi := ix.fields[q.Field]
	if fi == nil {
		return out
	}
	addTerm := func(term string) {
		if !match(term) {
			return
		}
		if id := fi.termOne[term]; id != 0 && !ix.isDeletedOrExpiredLocked(id) {
			out.Add(id)
		}
		if bm := fi.terms[term]; bm != nil {
			out = out.OrInPlace(bm)
		}
		if id, exists := fi.unique[term]; exists && !ix.isDeletedOrExpiredLocked(id) {
			out.Add(id)
		}
	}
	for term := range fi.termOne {
		addTerm(term)
	}
	for term := range fi.terms {
		if fi.termOne[term] == 0 {
			addTerm(term)
		}
	}
	for term := range fi.unique {
		if fi.termOne[term] == 0 && fi.terms[term] == nil {
			addTerm(term)
		}
	}
	return out
}

type Fuzzy struct {
	Field, Value string
	Distance     int
	LimitTerms   int
}

func (q Fuzzy) eval(ix *Index) *Bitmap {
	return ix.fuzzyBitmap(q.Field, q.Value, q.Distance, q.LimitTerms)
}

type Exists struct{ Field string }

func (q Exists) eval(ix *Index) *Bitmap { return ix.existsBitmap(q.Field) }

type Missing struct{ Field string }

func (q Missing) eval(ix *Index) *Bitmap { return Not{Q: Exists{Field: q.Field}}.eval(ix) }

type Range struct {
	Field            string
	GTE, GT, LTE, LT any
}

func (q Range) eval(ix *Index) *Bitmap { return ix.rangeBitmap(q.Field, q.GTE, q.GT, q.LTE, q.LT) }

type And []Query

func (q And) eval(ix *Index) *Bitmap {
	if len(q) == 0 {
		return MatchAll{}.eval(ix)
	}
	items := append([]Query(nil), q...)
	if len(items) > 1 {
		sort.Slice(items, func(i, j int) bool { return ix.estimateCardinality(items[i]) < ix.estimateCardinality(items[j]) })
	}
	r := items[0].eval(ix).Clone()
	for i := 1; i < len(items); i++ {
		r = r.AndInPlace(items[i].eval(ix))
		if r.Empty() {
			break
		}
	}
	return r
}

type Or []Query

func (q Or) eval(ix *Index) *Bitmap {
	r := NewBitmap()
	for _, qq := range q {
		r = r.OrInPlace(qq.eval(ix))
	}
	return r
}

type Not struct{ Q Query }

func (q Not) eval(ix *Index) *Bitmap {
	ix.mu.RLock()
	max := ix.nextDocID
	ix.mu.RUnlock()
	return q.Q.eval(ix).Not(max).AndInPlace(MatchAll{}.eval(ix))
}

type Bool struct {
	Must, Should, Filter, MustNot []Query
	MinShouldMatch                int
}

func (q Bool) eval(ix *Index) *Bitmap {
	var r *Bitmap
	for _, m := range q.Must {
		if r == nil {
			r = m.eval(ix).Clone()
		} else {
			r = r.AndInPlace(m.eval(ix))
		}
		if r.Empty() {
			return r
		}
	}
	for _, f := range q.Filter {
		if r == nil {
			r = f.eval(ix).Clone()
		} else {
			r = r.AndInPlace(f.eval(ix))
		}
		if r.Empty() {
			return r
		}
	}
	if len(q.Should) > 0 {
		if q.MinShouldMatch <= 1 {
			s := Or(q.Should).eval(ix)
			if r == nil {
				r = s
			} else {
				r = r.AndInPlace(s)
			}
		} else {
			counts := map[DocID]int{}
			for _, sq := range q.Should {
				sq.eval(ix).Each(func(id DocID) bool { counts[id]++; return true })
			}
			s := NewBitmap()
			for id, c := range counts {
				if c >= q.MinShouldMatch {
					s.Add(id)
				}
			}
			if r == nil {
				r = s
			} else {
				r = r.AndInPlace(s)
			}
		}
	}
	if r == nil {
		r = MatchAll{}.eval(ix)
	}
	for _, n := range q.MustNot {
		r = r.AndInPlace(Not{Q: n}.eval(ix))
		if r.Empty() {
			break
		}
	}
	return r
}

type SearchRequest struct {
	Query    Query
	Limit    int
	Offset   int
	WithDocs bool
	Sort     []SortField
	Facets   []string
}

func Simple(field, expr string) Query {
	parts := strings.Fields(expr)
	if len(parts) == 0 {
		return MatchAll{}
	}
	must, should, not := make([]Query, 0, len(parts)), make([]Query, 0, len(parts)), make([]Query, 0, len(parts))
	for _, p := range parts {
		req, neg := false, false
		if strings.HasPrefix(p, "+") {
			req = true
			p = p[1:]
		}
		if strings.HasPrefix(p, "-") {
			neg = true
			p = p[1:]
		}
		var q Query
		switch {
		case strings.HasSuffix(p, "*") && len(p) > 1:
			q = Prefix{field, strings.TrimSuffix(p, "*")}
		case strings.HasPrefix(p, "*") && len(p) > 1:
			q = Suffix{field, strings.TrimPrefix(p, "*")}
		case strings.Contains(p, "~"):
			parts := strings.SplitN(p, "~", 2)
			dist := 1
			if parts[1] != "" {
				if d, err := strconv.Atoi(parts[1]); err == nil {
					dist = d
				}
			}
			q = Fuzzy{Field: field, Value: parts[0], Distance: dist}
		default:
			q = Term{Field: field, Value: p}
		}
		if neg {
			not = append(not, q)
		} else if req {
			must = append(must, q)
		} else {
			should = append(should, q)
		}
	}
	if len(must) == 0 && len(not) == 0 && len(should) > 0 {
		return And(should)
	}
	return Bool{Must: must, Should: should, MustNot: not, MinShouldMatch: 1}
}
