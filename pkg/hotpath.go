package pkg

import (
	"sort"
	"sync"
)

type Groupspace struct {
	Bitmap *Bitmap
	Hits   []Hit
	IDs    []DocID
	Tokens []string
}

var workspacePool = sync.Pool{New: func() any {
	return &Groupspace{Bitmap: NewBitmap(), Hits: make([]Hit, 0, 64), IDs: make([]DocID, 0, 1024), Tokens: make([]string, 0, 32)}
}}

func AcquireGroupspace() *Groupspace { return workspacePool.Get().(*Groupspace) }
func ReleaseGroupspace(w *Groupspace) {
	if w == nil {
		return
	}
	w.Hits = w.Hits[:0]
	w.IDs = w.IDs[:0]
	w.Tokens = w.Tokens[:0]
	if w.Bitmap != nil {
		w.Bitmap.Reset()
	}
	workspacePool.Put(w)
}

func (ix *Index) Each(q Query, fn func(id string, docID DocID) bool) {
	if q == nil {
		q = MatchAll{}
	}
	bm := q.eval(ix)
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	bm.Each(func(id DocID) bool {
		if ix.isDeletedOrExpiredLocked(id) {
			return true
		}
		return fn(ix.docToExt[id], id)
	})
}
func (ix *Index) Collect(q Query, dst []string) []string {
	dst = dst[:0]
	ix.Each(q, func(id string, _ DocID) bool { dst = append(dst, id); return true })
	return dst
}
func (ix *Index) SearchInto(req SearchRequest, dst []Hit) (Result, []Hit) {
	return ix.searchIntoFast(req, dst)
}

func (ix *Index) Profile(q Query) []ProfileEvent {
	start := ix.clock.UnixNano()
	bm := q.eval(ix)
	return []ProfileEvent{{Clause: queryName(q), TookNS: ix.clock.UnixNano() - start, Hits: bm.Count()}}
}
func queryName(q Query) string {
	switch q.(type) {
	case Term:
		return "term"
	case Terms:
		return "terms"
	case Range:
		return "range"
	case Bool:
		return "bool"
	case And:
		return "and"
	case Or:
		return "or"
	case Phrase:
		return "phrase"
	case VectorQuery:
		return "vector"
	default:
		return "query"
	}
}

func (q And) optimized(ix *Index) *Bitmap {
	qs := append([]Query(nil), q...)
	sort.Slice(qs, func(i, j int) bool { return estimate(ix, qs[i]) < estimate(ix, qs[j]) })
	return And(qs).eval(ix)
}
func estimate(ix *Index, q Query) int { bm := q.eval(ix); return bm.Count() }

// SearchInto executes the common no-sort/no-facet search path without allocating result slices.
// It reuses dst for hits and only allocates when WithDocs is true because documents are cloned.
func (ix *Index) searchIntoFast(req SearchRequest, dst []Hit) (Result, []Hit) {
	start := int64(0)
	if ix.cfg.CollectTook {
		start = ix.clock.UnixNano()
	}
	if req.Limit <= 0 {
		req.Limit = 20
	}
	if req.Query == nil {
		req.Query = MatchAll{}
	}
	if len(req.Sort) != 0 || len(req.Facets) != 0 || req.WithDocs {
		res := ix.Search(req)
		dst = append(dst[:0], res.Hits...)
		res.Hits = dst
		return res, dst
	}
	switch q := req.Query.(type) {
	case Term:
		return ix.searchTermIntoFast(q, req, dst, start)
	case Terms:
		return ix.searchTermsIntoFast(q, req, dst, start)
	case Range:
		return ix.searchRangeIntoFast(q, req, dst, start)
	case Contains:
		return ix.searchContainsIntoFast(q, req, dst, start)
	case Fuzzy:
		return ix.searchFuzzyIntoFast(q, req, dst, start)
	case CIDR:
		return ix.searchCIDRIntoFast(q, req, dst, start)
	case VectorQuery:
		return ix.searchVectorIntoFast(q, req, dst, start)
	case Bool:
		return ix.searchBoolIntoFast(q, req, dst, start)
	case TupleCompositeQuery:
		ex := extras(ix)
		ex.mu.RLock()
		c := ex.composite
		ex.mu.RUnlock()
		if c != nil {
			dst = c.Search(ix, q.Term, q.GroupID, q.DateKey, q.Source, req.Limit, dst)
			return Result{Total: len(dst), Took: ix.elapsed(start)}, dst
		}
	}
	bm := req.Query.eval(ix)
	return ix.searchBitmapIntoFast(bm, req, dst, start)
}

func (ix *Index) searchBitmapIntoFast(bm *Bitmap, req SearchRequest, dst []Hit, start int64) (Result, []Hit) {
	dst = dst[:0]
	total, skipped := 0, 0
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	bm.Each(func(id DocID) bool {
		if ix.isDeletedOrExpiredLocked(id) {
			return true
		}
		total++
		if skipped < req.Offset {
			skipped++
			return true
		}
		if len(dst) >= req.Limit {
			return false
		}
		dst = append(dst, Hit{ID: ix.docToExt[id], DocID: id, Score: 1})
		return true
	})
	return Result{Total: total, Hits: dst, Took: ix.elapsed(start)}, dst
}

func (ix *Index) searchTermIntoFast(q Term, req SearchRequest, dst []Hit, start int64) (Result, []Hit) {
	dst = dst[:0]
	total, skipped := 0, 0
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	opt := ix.cfg.Schema.Fields[q.Field]
	fi := ix.fields[q.Field]
	if fi == nil {
		return Result{Hits: dst, Took: ix.elapsed(start)}, dst
	}
	val := normalizeSpecialString(q.Field, q.Value, opt.Lowercase)
	if did, ok := fi.unique[val]; ok && (!ix.hasTTL && !ix.hasDeletes || !ix.isDeletedOrExpiredLocked(did)) {
		total = 1
		if req.Offset == 0 && req.Limit > 0 {
			dst = append(dst, Hit{ID: ix.docToExt[did], DocID: did, Score: 1})
		}
		return Result{Total: total, Hits: dst, Took: ix.elapsed(start)}, dst
	}
	if did := fi.termOne[val]; did != 0 && (!ix.hasTTL && !ix.hasDeletes || !ix.isDeletedOrExpiredLocked(did)) {
		total = 1
		if skipped >= req.Offset && len(dst) < req.Limit {
			dst = append(dst, Hit{ID: ix.docToExt[did], DocID: did, Score: 1})
		}
		return Result{Total: total, Hits: dst, Took: ix.elapsed(start)}, dst
	}
	if bm := fi.terms[val]; bm != nil {
		bm.Each(func(id DocID) bool {
			if ix.isDeletedOrExpiredLocked(id) {
				return true
			}
			total++
			if skipped < req.Offset {
				skipped++
				return true
			}
			if len(dst) >= req.Limit {
				return false
			}
			dst = append(dst, Hit{ID: ix.docToExt[id], DocID: id, Score: 1})
			return true
		})
	}
	return Result{Total: total, Hits: dst, Took: ix.elapsed(start)}, dst
}

// CountTerm counts live documents matching a single exact term without allocating.
func (ix *Index) CountTerm(field, value string) int {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	sf, ok := ix.fieldByNameLocked(field)
	if !ok {
		return 0
	}
	val := normalizeStringByKind(sf.special, value, sf.opt.Lowercase)
	fi := sf.fi
	if did, ok := fi.unique[val]; ok {
		if !ix.hasTTL && !ix.hasDeletes {
			return 1
		}
		if !ix.isDeletedOrExpiredLocked(did) {
			return 1
		}
		return 0
	}
	if did := fi.termOne[val]; did != 0 {
		if !ix.hasTTL && !ix.hasDeletes {
			return 1
		}
		if !ix.isDeletedOrExpiredLocked(did) {
			return 1
		}
		return 0
	}
	bm := fi.terms[val]
	if bm == nil {
		return 0
	}
	if !ix.hasTTL && !ix.hasDeletes {
		return bm.Count()
	}
	return ix.countLiveLocked(bm)
}

func (ix *Index) searchTermsIntoFast(q Terms, req SearchRequest, dst []Hit, start int64) (Result, []Hit) {
	dst = dst[:0]
	total, skipped := 0, 0
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	sf, ok := ix.fieldByNameLocked(q.Field)
	if !ok {
		return Result{Hits: dst, Took: ix.elapsed(start)}, dst
	}
	for _, raw := range q.Values {
		val := normalizeStringByKind(sf.special, raw, sf.opt.Lowercase)
		if did, ok := sf.fi.unique[val]; ok && !ix.isDeletedOrExpiredLocked(did) {
			total++
			if skipped < req.Offset {
				skipped++
				continue
			}
			if len(dst) < req.Limit {
				dst = append(dst, Hit{ID: ix.docToExt[did], DocID: did, Score: 1})
			}
			continue
		}
		if did := sf.fi.termOne[val]; did != 0 && !ix.isDeletedOrExpiredLocked(did) {
			total++
			if skipped < req.Offset {
				skipped++
				continue
			}
			if len(dst) < req.Limit {
				dst = append(dst, Hit{ID: ix.docToExt[did], DocID: did, Score: 1})
			}
			continue
		}
		if bm := sf.fi.terms[val]; bm != nil {
			bm.Each(func(id DocID) bool {
				if ix.isDeletedOrExpiredLocked(id) {
					return true
				}
				total++
				if skipped < req.Offset {
					skipped++
					return true
				}
				if len(dst) >= req.Limit {
					return false
				}
				dst = append(dst, Hit{ID: ix.docToExt[id], DocID: id, Score: 1})
				return true
			})
		}
		if len(dst) >= req.Limit && req.Limit > 0 {
			break
		}
	}
	return Result{Total: total, Hits: dst, Took: ix.elapsed(start)}, dst
}

func (ix *Index) searchRangeIntoFast(q Range, req SearchRequest, dst []Hit, start int64) (Result, []Hit) {
	dst = dst[:0]
	total, skipped := 0, 0
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if ix.numericExists[q.Field] == nil {
		return Result{Hits: dst, Took: ix.elapsed(start)}, dst
	}
	min, hasMin, minInc := boundFloat(q.GTE, q.GT, true)
	max, hasMax, maxInc := boundFloat(q.LTE, q.LT, false)
	pairs := ix.sortedNumericLocked(q.Field)
	lo, hi := 0, len(pairs)
	if hasMin {
		lo = sort.Search(len(pairs), func(i int) bool {
			if minInc {
				return pairs[i].v >= min
			}
			return pairs[i].v > min
		})
	}
	if hasMax {
		hi = sort.Search(len(pairs), func(i int) bool {
			if maxInc {
				return pairs[i].v > max
			}
			return pairs[i].v >= max
		})
	}
	if hi < lo {
		hi = lo
	}
	for _, p := range pairs[lo:hi] {
		if ix.isDeletedOrExpiredLocked(p.id) {
			continue
		}
		total++
		if skipped < req.Offset {
			skipped++
			continue
		}
		if len(dst) >= req.Limit {
			break
		}
		dst = append(dst, Hit{ID: ix.docToExt[p.id], DocID: p.id, Score: 1})
	}
	return Result{Total: total, Hits: dst, Took: ix.elapsed(start)}, dst
}

func (ix *Index) fieldByNameLocked(name string) (*schemaField, bool) {
	id, ok := ix.fieldIDs[name]
	if !ok || int(id) >= len(ix.fieldList) {
		return nil, false
	}
	return &ix.fieldList[id], true
}

func (ix *Index) searchBoolIntoFast(q Bool, req SearchRequest, dst []Hit, start int64) (Result, []Hit) {
	if len(q.Should) != 0 || len(q.Must) != 1 || len(q.Filter) > 1 || len(q.MustNot) > 1 {
		bm := q.eval(ix)
		return ix.searchBitmapIntoFast(bm, req, dst, start)
	}
	mt, ok := q.Must[0].(Term)
	if !ok {
		bm := q.eval(ix)
		return ix.searchBitmapIntoFast(bm, req, dst, start)
	}
	var rg Range
	hasRange := false
	if len(q.Filter) == 1 {
		var ok bool
		rg, ok = q.Filter[0].(Range)
		if !ok {
			bm := q.eval(ix)
			return ix.searchBitmapIntoFast(bm, req, dst, start)
		}
		hasRange = true
	}
	var nt Term
	hasNot := false
	if len(q.MustNot) == 1 {
		var ok bool
		nt, ok = q.MustNot[0].(Term)
		if !ok {
			bm := q.eval(ix)
			return ix.searchBitmapIntoFast(bm, req, dst, start)
		}
		hasNot = true
	}
	dst = dst[:0]
	total, skipped := 0, 0
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	msf, ok := ix.fieldByNameLocked(mt.Field)
	if !ok {
		return Result{Hits: dst, Took: ix.elapsed(start)}, dst
	}
	mval := normalizeStringByKind(msf.special, mt.Value, msf.opt.Lowercase)
	var min, max float64
	var hasMin, hasMax, minInc, maxInc bool
	if hasRange {
		min, hasMin, minInc = boundFloat(rg.GTE, rg.GT, true)
		max, hasMax, maxInc = boundFloat(rg.LTE, rg.LT, false)
	}
	var nsf *schemaField
	var nval string
	if hasNot {
		if s, ok := ix.fieldByNameLocked(nt.Field); ok {
			nsf = s
			nval = normalizeStringByKind(s.special, nt.Value, s.opt.Lowercase)
		}
	}
	visit := func(id DocID) bool {
		if ix.isDeletedOrExpiredLocked(id) {
			return true
		}
		if hasRange {
			v, ok := ix.numericValueLocked(rg.Field, id)
			if !ok {
				return true
			}
			if hasMin {
				if minInc {
					if v < min {
						return true
					}
				} else if v <= min {
					return true
				}
			}
			if hasMax {
				if maxInc {
					if v > max {
						return true
					}
				} else if v >= max {
					return true
				}
			}
		}
		if nsf != nil && ix.termHasLocked(nsf.fi, nval, id) {
			return true
		}
		total++
		if skipped < req.Offset {
			skipped++
			return true
		}
		if len(dst) >= req.Limit {
			return false
		}
		dst = append(dst, Hit{ID: ix.docToExt[id], DocID: id, Score: 1})
		return true
	}
	if did, ok := msf.fi.unique[mval]; ok {
		visit(did)
	} else if did := msf.fi.termOne[mval]; did != 0 {
		visit(did)
	} else if bm := msf.fi.terms[mval]; bm != nil {
		bm.Each(visit)
	}
	return Result{Total: total, Hits: dst, Took: ix.elapsed(start)}, dst
}

func (ix *Index) termHasLocked(fi *fieldIndex, val string, id DocID) bool {
	if fi == nil {
		return false
	}
	if did, ok := fi.unique[val]; ok {
		return did == id
	}
	if did := fi.termOne[val]; did != 0 {
		return did == id
	}
	if b := fi.terms[val]; b != nil {
		return b.Has(id)
	}
	return false
}

func (ix *Index) searchContainsIntoFast(q Contains, req SearchRequest, dst []Hit, start int64) (Result, []Hit) {
	dst = dst[:0]
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	sf, ok := ix.fieldByNameLocked(q.Field)
	if !ok {
		return Result{Hits: dst, Took: ix.elapsed(start)}, dst
	}
	val := normalize(q.Value, sf.opt.Lowercase)
	gs := grams(val, sf.opt.MinGram, sf.opt.MaxGram, ix.scratch)
	if len(gs) != 1 {
		return ix.searchBitmapIntoFastNoLock(Contains{q.Field, q.Value}.eval(ix), req, dst, start)
	}
	return ix.collectPostingLocked(sf.fi.ngram, sf.fi.ngramOne, gs[0], req, dst, start)
}

func (ix *Index) searchFuzzyIntoFast(q Fuzzy, req SearchRequest, dst []Hit, start int64) (Result, []Hit) {
	dst = dst[:0]
	total, skipped := 0, 0
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	sf, ok := ix.fieldByNameLocked(q.Field)
	if !ok {
		return Result{Hits: dst, Took: ix.elapsed(start)}, dst
	}
	value := normalize(q.Value, sf.opt.Lowercase)
	if value == "" {
		return Result{Hits: dst, Took: ix.elapsed(start)}, dst
	}
	dist := q.Distance
	if dist <= 0 {
		dist = 1
	}
	limitTerms := q.LimitTerms
	if limitTerms <= 0 {
		limitTerms = 256
	}
	seenTerms := 0
	for _, term := range sf.fi.fuzzyTerms[value[0]] {
		if abs(len(term)-len(value)) > dist || levenshtein(term, value, dist) > dist {
			continue
		}
		visit := func(id DocID) bool {
			if ix.isDeletedOrExpiredLocked(id) {
				return true
			}
			total++
			if skipped < req.Offset {
				skipped++
				return true
			}
			if len(dst) >= req.Limit {
				return false
			}
			dst = append(dst, Hit{ID: ix.docToExt[id], DocID: id, Score: 1})
			return true
		}
		if did := sf.fi.termOne[term]; did != 0 {
			visit(did)
		} else if bm := sf.fi.terms[term]; bm != nil {
			bm.Each(visit)
		}
		seenTerms++
		if seenTerms >= limitTerms || len(dst) >= req.Limit {
			break
		}
	}
	return Result{Total: total, Hits: dst, Took: ix.elapsed(start)}, dst
}

func (ix *Index) searchCIDRIntoFast(q CIDR, req SearchRequest, dst []Hit, start int64) (Result, []Hit) {
	dst = dst[:0]
	base, ones, ok := parseCIDR4(q.Value)
	if !ok {
		return Result{Hits: dst, Took: ix.elapsed(start)}, dst
	}
	mask := uint32(0)
	if ones != 0 {
		mask = ^uint32(0) << uint(32-ones)
	}
	lo := base & mask
	hi := lo | ^mask
	total, skipped := 0, 0
	ix.mu.Lock()
	defer ix.mu.Unlock()
	pairs := ix.sortedIPv4Locked(q.Field)
	s := sort.Search(len(pairs), func(i int) bool { return pairs[i].v >= lo })
	e := sort.Search(len(pairs), func(i int) bool { return pairs[i].v > hi })
	for _, p := range pairs[s:e] {
		if ix.isDeletedOrExpiredLocked(p.id) {
			continue
		}
		total++
		if skipped < req.Offset {
			skipped++
			continue
		}
		if len(dst) >= req.Limit {
			break
		}
		dst = append(dst, Hit{ID: ix.docToExt[p.id], DocID: p.id, Score: 1})
	}
	return Result{Total: total, Hits: dst, Took: ix.elapsed(start)}, dst
}

func (ix *Index) collectPostingLocked(m map[string]*Bitmap, one map[string]DocID, term string, req SearchRequest, dst []Hit, start int64) (Result, []Hit) {
	total, skipped := 0, 0
	if did := one[term]; did != 0 && !ix.isDeletedOrExpiredLocked(did) {
		total = 1
		if req.Offset == 0 && req.Limit > 0 {
			dst = append(dst, Hit{ID: ix.docToExt[did], DocID: did, Score: 1})
		}
		return Result{Total: total, Hits: dst, Took: ix.elapsed(start)}, dst
	}
	if bm := m[term]; bm != nil {
		bm.Each(func(id DocID) bool {
			if ix.isDeletedOrExpiredLocked(id) {
				return true
			}
			total++
			if skipped < req.Offset {
				skipped++
				return true
			}
			if len(dst) >= req.Limit {
				return false
			}
			dst = append(dst, Hit{ID: ix.docToExt[id], DocID: id, Score: 1})
			return true
		})
	}
	return Result{Total: total, Hits: dst, Took: ix.elapsed(start)}, dst
}

func (ix *Index) searchBitmapIntoFastNoLock(bm *Bitmap, req SearchRequest, dst []Hit, start int64) (Result, []Hit) {
	return ix.searchBitmapIntoFast(bm, req, dst, start)
}

func parseCIDR4(s string) (uint32, int, bool) {
	slash := -1
	for i := 0; i < len(s); i++ {
		if s[i] == '/' {
			slash = i
			break
		}
	}
	if slash <= 0 || slash == len(s)-1 {
		return 0, 0, false
	}
	ip, ok := parseIPv4(s[:slash])
	if !ok {
		return 0, 0, false
	}
	ones := 0
	for i := slash + 1; i < len(s); i++ {
		c := s[i]
		if c < '0' || c > '9' {
			return 0, 0, false
		}
		ones = ones*10 + int(c-'0')
	}
	if ones < 0 || ones > 32 {
		return 0, 0, false
	}
	return ip, ones, true
}

func parseIPv4(s string) (uint32, bool) {
	var parts [4]uint32
	part, n := 0, 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == '.' {
			if n >= 4 {
				return 0, false
			}
			parts[n] = uint32(part)
			n++
			part = 0
			continue
		}
		c := s[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		part = part*10 + int(c-'0')
		if part > 255 {
			return 0, false
		}
	}
	if n != 4 {
		return 0, false
	}
	return parts[0]<<24 | parts[1]<<16 | parts[2]<<8 | parts[3], true
}

func (ix *Index) searchVectorIntoFast(q VectorQuery, req SearchRequest, dst []Hit, start int64) (Result, []Hit) {
	dst = dst[:0]
	k := q.K
	if k <= 0 {
		k = req.Limit
	}
	if k <= 0 {
		k = 10
	}
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	ann := ix.anns[q.Field]
	if ann != nil {
		var allowed *Bitmap
		if q.Filter != nil {
			// Common production path: tenant/status filters are term bitmaps and are
			// shared immutable posting views, so this does not allocate.
			allowed = q.Filter.eval(ix)
		}
		before := len(dst)
		dst = ann.Search(q.Vector, k, allowed, ix, dst, req.Limit)
		return Result{Total: len(dst) - before, Hits: dst, Took: ix.elapsed(start)}, dst
	}
	if k > 64 {
		ix.mu.RUnlock()
		bm := q.eval(ix)
		ix.mu.RLock()
		return ix.searchBitmapIntoFast(bm, req, dst, start)
	}
	type vpair struct {
		id    DocID
		score float64
	}
	var buf [64]vpair
	top := buf[:0]
	for id, v := range ix.vectors[q.Field] {
		if ix.isDeletedOrExpiredLocked(id) {
			continue
		}
		var sc float64
		switch q.Metric {
		case "l2":
			sc = l2(q.Vector, v)
		case "dot":
			sc = dot(q.Vector, v)
		default:
			sc = cosine(q.Vector, v)
		}
		if len(top) < k {
			top = append(top, vpair{id, sc})
			if len(top) == k {
				for i := 1; i < len(top); i++ {
					if top[i].score < top[0].score {
						top[0], top[i] = top[i], top[0]
					}
				}
			}
			continue
		}
		if sc <= top[0].score {
			continue
		}
		top[0] = vpair{id, sc}
		for i := 1; i < len(top); i++ {
			if top[i].score < top[0].score {
				top[0], top[i] = top[i], top[0]
			}
		}
	}
	sort.Slice(top, func(i, j int) bool { return top[i].score > top[j].score })
	from := req.Offset
	if from > len(top) {
		from = len(top)
	}
	to := from + req.Limit
	if req.Limit <= 0 || to > len(top) {
		to = len(top)
	}
	for _, p := range top[from:to] {
		dst = append(dst, Hit{ID: ix.docToExt[p.id], DocID: p.id, Score: p.score})
	}
	return Result{Total: len(top), Hits: dst, Took: ix.elapsed(start)}, dst
}
