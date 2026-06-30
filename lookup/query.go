package lookup

import (
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
