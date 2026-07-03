package pkg

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type fieldIndex struct {
	// Most lookup values are high-cardinality singletons. Keeping the first
	// posting as a DocID avoids allocating a bitmap per unique term during
	// indexing. A bitmap is promoted only when the second distinct doc appears.
	terms, prefix, suffix, ngram            map[string]*Bitmap
	termOne, prefixOne, suffixOne, ngramOne map[string]DocID
	positions                               map[string]map[DocID][]uint32
	phrases                                 map[string]*Bitmap
	phraseOne                               map[string]DocID
	unique                                  map[string]DocID
	fuzzyTerms                              map[byte][]string
	exists                                  *Bitmap
	capHint                                 DocID
}

func newFieldIndex(capHint int, opt FieldOptions) *fieldIndex {
	// Allocate large maps only for structures that are actually used. This keeps
	// memory low while avoiding repeated map growth on the ingestion hot path.
	termCap, uniqueCap, prefixCap, suffixCap, ngramCap := 0, 0, 0, 0, 0
	if opt.Unique {
		uniqueCap = capHint
	} else if opt.Lookup || opt.Kind == FieldKeyword || opt.Kind == FieldBool || opt.Kind == FieldText || opt.Indexed {
		// Text fields usually have a small vocabulary in lookup workloads; keyword
		// fields may be high-cardinality but singleton postings avoid bitmap allocs.
		if opt.Kind == FieldKeyword {
			termCap = capHint / 4
		} else {
			termCap = 64
		}
	}
	if opt.Prefix {
		prefixCap = 256
	}
	if opt.Suffix {
		suffixCap = 256
	}
	if opt.Ngram {
		ngramCap = 512
	}
	return &fieldIndex{
		terms: make(map[string]*Bitmap, termCap), prefix: make(map[string]*Bitmap, prefixCap), suffix: make(map[string]*Bitmap, suffixCap), ngram: make(map[string]*Bitmap, ngramCap),
		termOne: make(map[string]DocID, termCap), prefixOne: make(map[string]DocID, prefixCap), suffixOne: make(map[string]DocID, suffixCap), ngramOne: make(map[string]DocID, ngramCap),
		positions: map[string]map[DocID][]uint32{}, phrases: map[string]*Bitmap{}, phraseOne: map[string]DocID{},
		unique: make(map[string]DocID, uniqueCap), fuzzyTerms: map[byte][]string{}, exists: NewBitmapCap(DocID(capHint + 1)), capHint: DocID(capHint + 1),
	}
}

type specialNormalizer uint8

const (
	specialNone specialNormalizer = iota
	specialEmail
	specialPhone
	specialDomain
	specialURL
	specialIP
)

type schemaField struct {
	id      FieldID
	name    string
	opt     FieldOptions
	fi      *fieldIndex
	special specialNormalizer
}

func detectSpecial(field string) specialNormalizer {
	lf := strings.ToLower(field)
	switch {
	case strings.Contains(lf, "email"):
		return specialEmail
	case strings.Contains(lf, "phone") || strings.Contains(lf, "mobile"):
		return specialPhone
	case strings.Contains(lf, "domain") || strings.Contains(lf, "host"):
		return specialDomain
	case strings.Contains(lf, "url") || strings.Contains(lf, "uri"):
		return specialURL
	case strings.Contains(lf, "ip"):
		return specialIP
	default:
		return specialNone
	}
}

type numPair struct {
	id DocID
	v  float64
}

type ipPair struct {
	id DocID
	v  uint32
}

type Index struct {
	mu            sync.RWMutex
	cfg           Config
	nextDocID     DocID
	extToDoc      map[string]DocID
	docToExt      []string
	docs          []Document
	deleted       *Bitmap
	live          *Bitmap
	expires       map[DocID]int64
	hasTTL        bool
	hasDeletes    bool
	fields        map[string]*fieldIndex
	fieldList     []schemaField
	fieldIDs      map[string]FieldID
	clock         Clock
	numeric       map[string]map[DocID]float64
	numericDense  map[string][]float64
	numericExists map[string]*Bitmap
	numSorted     map[string][]numPair
	numDirty      map[string]bool
	strings       map[string]map[DocID]string
	vectors       map[string]map[DocID][]float64
	anns          map[string]*VectorANN
	ip4           map[string]map[DocID]uint32
	ip4Sorted     map[string][]ipPair
	ip4Dirty      map[string]bool
	wal           *os.File
	tokens        []string
	scratch       []string
	valScratch    []string
}

func New(cfg Config) (*Index, error) {
	capHint := cfg.InitialCapacity
	if capHint < 0 {
		capHint = 0
	}
	ix := &Index{cfg: cfg, nextDocID: 1, extToDoc: make(map[string]DocID, capHint), deleted: NewBitmapCap(DocID(capHint + 1)), live: NewBitmapCap(DocID(capHint + 1)), expires: make(map[DocID]int64), fields: map[string]*fieldIndex{}, fieldIDs: map[string]FieldID{}, numeric: map[string]map[DocID]float64{}, numericDense: map[string][]float64{}, numericExists: map[string]*Bitmap{}, numSorted: map[string][]numPair{}, numDirty: map[string]bool{}, strings: map[string]map[DocID]string{}, vectors: map[string]map[DocID][]float64{}, anns: map[string]*VectorANN{}, ip4: map[string]map[DocID]uint32{}, ip4Sorted: map[string][]ipPair{}, ip4Dirty: map[string]bool{}, docToExt: make([]string, 1, capHint+1), docs: make([]Document, 1, capHint+1)}
	if cfg.Clock != nil {
		ix.clock = cfg.Clock
	} else {
		ix.clock = SystemClock{}
	}
	fid := FieldID(0)
	fieldNames := make([]string, 0, len(cfg.Schema.Fields))
	for name := range cfg.Schema.Fields {
		fieldNames = append(fieldNames, name)
	}
	sort.Strings(fieldNames)
	for _, name := range fieldNames {
		opt := cfg.Schema.Fields[name]
		fi := newFieldIndex(capHint, opt)
		sp := detectSpecial(name)
		ix.fields[name] = fi
		ix.fieldIDs[name] = fid
		ix.fieldList = append(ix.fieldList, schemaField{id: fid, name: name, opt: opt, fi: fi, special: sp})
		fid++
		if opt.TTLField {
			ix.hasTTL = true
		}
		if opt.Sortable || opt.Facetable || opt.Kind == FieldInt || opt.Kind == FieldFloat || opt.Kind == FieldTime {
			// Dense column is the primary numeric storage. The map is kept only for
			// backwards-compatible numeric facets on explicit facetable fields.
			if opt.Facetable {
				ix.numeric[name] = make(map[DocID]float64, capHint)
			}
			ix.numericDense[name] = make([]float64, capHint+1)
			ix.numericExists[name] = NewBitmapCap(DocID(capHint + 1))
		}
		if opt.Facetable || opt.Stored || sp == specialDomain || sp == specialURL {
			ix.strings[name] = make(map[DocID]string, capHint)
		}
		if opt.Kind == FieldVector || opt.Dim > 0 {
			ix.vectors[name] = make(map[DocID][]float64, capHint)
			ix.anns[name] = newVectorANN(opt.Dim, "dot", capHint)
		}
	}
	if cfg.EnableWAL && cfg.WALPath != "" {
		if err := os.MkdirAll(dir(cfg.WALPath), 0755); err != nil {
			return nil, err
		}
		f, err := os.OpenFile(cfg.WALPath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
		if err != nil {
			return nil, err
		}
		ix.wal = f
	}
	return ix, nil
}

func Open(cfg Config) (*Index, error) {
	ix, err := New(cfg)
	if err != nil {
		return nil, err
	}
	if cfg.SnapshotPath != "" {
		_ = ix.LoadSnapshot(cfg.SnapshotPath)
	}
	if cfg.EnableWAL && cfg.WALPath != "" {
		_ = ix.ReplayWAL(cfg.WALPath)
	}
	return ix, nil
}
func (ix *Index) Close() error {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if ix.wal != nil {
		return ix.wal.Close()
	}
	return nil
}

func (ix *Index) Upsert(id string, doc Document) error {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	return ix.upsertLocked(id, doc, true)
}

func (ix *Index) BatchUpsert(docs map[string]Document) error {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	for id, d := range docs {
		if err := ix.upsertLocked(id, d, true); err != nil {
			return err
		}
	}
	return nil
}

// BatchUpsertSlice is the lowest-overhead generic batch API. It avoids map iteration
// overhead and acquires the write lock once for the entire batch.
func (ix *Index) BatchUpsertSlice(ids []string, docs []Document) error {
	if len(ids) != len(docs) {
		return errors.New("ids/docs length mismatch")
	}
	ix.mu.Lock()
	defer ix.mu.Unlock()
	for i := range ids {
		if err := ix.upsertLocked(ids[i], docs[i], true); err != nil {
			return err
		}
	}
	return nil
}

func (ix *Index) upsert(id string, doc Document, log bool) error {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	return ix.upsertLocked(id, doc, log)
}

func (ix *Index) upsertLocked(id string, doc Document, log bool) error {
	if id == "" {
		return errors.New("id required")
	}
	for _, sf := range ix.fieldList {
		if !sf.opt.Unique {
			continue
		}
		if v, ok := doc[sf.name]; ok {
			ix.valScratch = valuesInto(v, ix.valScratch)
			for _, raw := range ix.valScratch {
				val := normalizeStringByKind(sf.special, raw, sf.opt.Lowercase)
				if did, exists := sf.fi.unique[val]; exists && !ix.deleted.Has(did) && ix.docToExt[did] != id {
					return fmt.Errorf("unique constraint violation on %s=%s", sf.name, val)
				}
			}
		}
	}
	if old, ok := ix.extToDoc[id]; ok {
		ix.deleted.Add(old)
		ix.live.Remove(old)
		ix.hasDeletes = true
	}
	did := ix.nextDocID
	ix.nextDocID++
	ix.extToDoc[id] = did
	ix.live.Add(did)
	ix.docToExt = append(ix.docToExt, id)
	if ix.cfg.DisableSource {
		ix.docs = append(ix.docs, nil)
	} else {
		ix.docs = append(ix.docs, cloneDoc(doc))
	}
	ix.indexDocLocked(did, doc)
	if log {
		return ix.appendWALLocked(walRecord{Op: "upsert", ID: id, Doc: doc})
	}
	return nil
}
func (ix *Index) Delete(id string) error { return ix.delete(id, true) }
func (ix *Index) BatchDelete(ids []string) error {
	for _, id := range ids {
		if err := ix.Delete(id); err != nil {
			return err
		}
	}
	return nil
}
func (ix *Index) delete(id string, log bool) error {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	did, ok := ix.extToDoc[id]
	if !ok {
		return nil
	}
	ix.deleted.Add(did)
	ix.live.Remove(did)
	ix.hasDeletes = true
	delete(ix.extToDoc, id)
	if log {
		return ix.appendWALLocked(walRecord{Op: "delete", ID: id})
	}
	return nil
}

func (ix *Index) Get(id string) (Document, bool) {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	did, ok := ix.extToDoc[id]
	if !ok || ix.isDeletedOrExpiredLocked(did) {
		return nil, false
	}
	if ix.docs[did] == nil {
		return nil, true
	}
	return cloneDoc(ix.docs[did]), true
}
func (ix *Index) Count(q Query) int {
	if q == nil {
		q = MatchAll{}
	}
	// Avoid materializing complements for common count-only queries.
	switch x := q.(type) {
	case Term:
		ix.mu.RLock()
		defer ix.mu.RUnlock()
		opt := ix.cfg.Schema.Fields[x.Field]
		if fi := ix.fields[x.Field]; fi != nil {
			val := normalizeSpecialString(x.Field, x.Value, opt.Lowercase)
			if did, ok := fi.unique[val]; ok && !ix.isDeletedOrExpiredLocked(did) {
				return 1
			}
			if did := fi.termOne[val]; did != 0 && !ix.isDeletedOrExpiredLocked(did) {
				return 1
			}
			if b := fi.terms[val]; b != nil {
				return ix.countLiveLocked(b)
			}
		}
		return 0
	case Exists:
		ix.mu.RLock()
		defer ix.mu.RUnlock()
		if fi := ix.fields[x.Field]; fi != nil {
			return ix.countLiveLocked(fi.exists)
		}
		return 0
	case Missing:
		ix.mu.RLock()
		defer ix.mu.RUnlock()
		if fi := ix.fields[x.Field]; fi != nil {
			return ix.live.Count() - ix.countLiveLocked(fi.exists)
		}
		return ix.live.Count()
	case Not:
		ix.mu.RLock()
		live := ix.live.Count()
		ix.mu.RUnlock()
		return live - ix.Count(x.Q)
	}
	bm := q.eval(ix)
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	return ix.countLiveLocked(bm)
}
func (ix *Index) countLiveLocked(bm *Bitmap) int {
	if bm == nil {
		return 0
	}
	n := 0
	bm.Each(func(id DocID) bool {
		if ix.live.Has(id) {
			n++
		}
		return true
	})
	return n
}

func (ix *Index) Search(req SearchRequest) Result {
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
	bm := req.Query.eval(ix)
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	if len(req.Sort) == 0 && len(req.Facets) == 0 {
		hits := make([]Hit, 0, req.Limit)
		skipped, total := 0, 0
		bm.Each(func(id DocID) bool {
			if ix.isDeletedOrExpiredLocked(id) {
				return true
			}
			total++
			if skipped < req.Offset {
				skipped++
				return true
			}
			if len(hits) >= req.Limit {
				return false
			}
			h := Hit{ID: ix.docToExt[id], DocID: id, Score: 1}
			if req.WithDocs && ix.docs[id] != nil {
				h.Doc = cloneDoc(ix.docs[id])
			}
			hits = append(hits, h)
			return true
		})
		return Result{Total: total, Hits: hits, Took: ix.elapsed(start)}
	}
	ids := make([]DocID, 0, req.Limit+req.Offset)
	total := 0
	bm.Each(func(id DocID) bool {
		if ix.isDeletedOrExpiredLocked(id) {
			return true
		}
		total++
		ids = append(ids, id)
		return true
	})
	if len(req.Sort) > 0 {
		ix.sortIDsLocked(ids, req.Sort)
	}
	facets := ix.facetsLocked(bm, req.Facets)
	from := req.Offset
	if from > len(ids) {
		from = len(ids)
	}
	to := from + req.Limit
	if to > len(ids) {
		to = len(ids)
	}
	hits := make([]Hit, 0, to-from)
	for _, id := range ids[from:to] {
		h := Hit{ID: ix.docToExt[id], DocID: id, Score: 1}
		if req.WithDocs && ix.docs[id] != nil {
			h.Doc = cloneDoc(ix.docs[id])
		}
		hits = append(hits, h)
	}
	return Result{Total: total, Hits: hits, Facets: facets, Took: ix.elapsed(start)}
}

func (ix *Index) numericValueLocked(field string, id DocID) (float64, bool) {
	ex := ix.numericExists[field]
	vals := ix.numericDense[field]
	if ex == nil || !ex.Has(id) || int(id) >= len(vals) {
		return 0, false
	}
	return vals[id], true
}

func (ix *Index) sortIDsLocked(ids []DocID, fields []SortField) {
	sort.SliceStable(ids, func(i, j int) bool {
		a, b := ids[i], ids[j]
		for _, sf := range fields {
			opt := ix.cfg.Schema.Fields[sf.Field]
			if opt.Kind == FieldInt || opt.Kind == FieldFloat || opt.Kind == FieldTime {
				av, aok := ix.numericValueLocked(sf.Field, a)
				bv, bok := ix.numericValueLocked(sf.Field, b)
				if !aok || !bok {
					if aok == bok {
						continue
					}
					less := missingLess(aok, sf.Missing)
					if sf.Desc {
						return !less
					}
					return less
				}
				if av == bv {
					continue
				}
				if sf.Desc {
					return av > bv
				}
				return av < bv
			}
			av, aok := ix.strings[sf.Field][a]
			bv, bok := ix.strings[sf.Field][b]
			if !aok || !bok {
				if aok == bok {
					continue
				}
				less := missingLess(aok, sf.Missing)
				if sf.Desc {
					return !less
				}
				return less
			}
			if av == bv {
				continue
			}
			if sf.Desc {
				return av > bv
			}
			return av < bv
		}
		return a < b
	})
}
func missingLess(present bool, policy string) bool {
	if policy == "first" {
		return !present
	}
	return present
}

func (ix *Index) facetsLocked(bm *Bitmap, names []string) map[string][]FacetBucket {
	if len(names) == 0 {
		return nil
	}
	out := map[string][]FacetBucket{}
	for _, field := range names {
		counts := map[string]int{}
		bm.Each(func(id DocID) bool {
			if ix.isDeletedOrExpiredLocked(id) {
				return true
			}
			if v, ok := ix.strings[field][id]; ok {
				counts[v]++
			} else if n, ok := ix.numeric[field][id]; ok {
				counts[strconv.FormatFloat(n, 'f', -1, 64)]++
			}
			return true
		})
		buckets := make([]FacetBucket, 0, len(counts))
		for v, c := range counts {
			buckets = append(buckets, FacetBucket{Value: v, Count: c})
		}
		sort.Slice(buckets, func(i, j int) bool {
			if buckets[i].Count == buckets[j].Count {
				return buckets[i].Value < buckets[j].Value
			}
			return buckets[i].Count > buckets[j].Count
		})
		out[field] = buckets
	}
	return out
}

func (ix *Index) Stats() Stats {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	s := Stats{Docs: int(ix.nextDocID - 1), DeletedDocs: ix.deleted.Count(), Fields: len(ix.fields), NumericCols: len(ix.numeric), StringCols: len(ix.strings)}
	live := 0
	for id := DocID(1); id < ix.nextDocID; id++ {
		if !ix.isDeletedOrExpiredLocked(id) {
			live++
		}
	}
	s.LiveDocs = live
	s.Segments = 1
	s.Shards = 1
	for _, v := range ix.vectors {
		s.Vectors += len(v)
	}
	for _, fi := range ix.fields {
		s.Terms += len(fi.terms) + len(fi.termOne)
		s.Prefixes += len(fi.prefix) + len(fi.prefixOne)
		s.Suffixes += len(fi.suffix) + len(fi.suffixOne)
		s.Ngrams += len(fi.ngram) + len(fi.ngramOne)
	}
	if ix.cfg.WALPath != "" {
		if st, err := os.Stat(ix.cfg.WALPath); err == nil {
			s.WALBytes = st.Size()
		}
	}
	return s
}

func (ix *Index) Analyze(field, text string) []AnalyzeToken {
	ix.mu.RLock()
	opt := ix.cfg.Schema.Fields[field]
	ix.mu.RUnlock()
	toks := analyzeWith(opt, text, nil)
	out := make([]AnalyzeToken, 0, len(toks))
	for i, t := range toks {
		out = append(out, AnalyzeToken{Term: t, Position: i})
	}
	return out
}

func (ix *Index) indexDocLocked(id DocID, doc Document) {
	for _, sf := range ix.fieldList {
		field, opt, fi := sf.name, sf.opt, sf.fi
		v, ok := doc[field]
		if !ok || v == nil {
			continue
		}
		fi.exists.Add(id)

		// Numeric/time fields are stored in columnar maps directly. Avoid converting
		// them to strings and indexing a unique term bitmap for every value; range
		// and exact numeric matching use the numeric column.
		if opt.Kind == FieldInt || opt.Kind == FieldFloat || opt.Kind == FieldTime {
			if n, ok := toFloat(v); ok {
				if ix.numeric[field] == nil {
					ix.numeric[field] = make(map[DocID]float64, ix.cfg.InitialCapacity)
				}
				ix.setNumericLocked(field, id, n)
			}
			if opt.TTLField {
				if ts := toUnix(v); ts > 0 {
					ix.expires[id] = ts
				}
			}
			continue
		}

		// Vector fields are stored as vectors only. Do not stringify the slice.
		if opt.Kind == FieldVector || opt.Dim > 0 {
			if vec, ok := toVector(v); ok {
				ix.addVectorLocked(field, id, vec)
			}
			continue
		}

		ix.valScratch = valuesInto(v, ix.valScratch)
		vals := ix.valScratch
		first := true
		for _, raw := range vals {
			val := normalizeStringByKind(sf.special, raw, opt.Lowercase)
			if val == "" {
				continue
			}
			if first {
				if col := ix.strings[field]; col != nil {
					col[id] = val
				}
				if sf.special == specialIP {
					if ip := net.ParseIP(val); ip != nil {
						if v4 := ip.To4(); v4 != nil {
							if ix.ip4[field] == nil {
								ix.ip4[field] = make(map[DocID]uint32, ix.cfg.InitialCapacity)
							}
							ix.ip4[field][id] = uint32(v4[0])<<24 | uint32(v4[1])<<16 | uint32(v4[2])<<8 | uint32(v4[3])
							ix.ip4Dirty[field] = true
						}
					}
				}
				if sf.special == specialDomain {
					ix.scratch = suffixes(val, ix.scratch)
					for _, s := range ix.scratch {
						addPosting(fi.suffix, fi.suffixOne, s, id, fi.capHint)
					}
				}
				first = false
			}
			if opt.Lookup || opt.Kind == FieldKeyword || opt.Kind == FieldBool || opt.Kind == FieldInt || opt.Kind == FieldFloat || opt.Kind == FieldTime {
				if opt.Unique {
					fi.unique[val] = id
				} else if opt.Kind != FieldInt && opt.Kind != FieldFloat && opt.Kind != FieldTime {
					addTerm(fi, val, id, opt.Fuzzy)
				}
			}
			if opt.Kind == FieldText || opt.Indexed {
				ix.tokens = analyzeWith(opt, val, ix.tokens)
				for pos, t := range ix.tokens {
					addTerm(fi, t, id, opt.Fuzzy)
					addPos(fi, t, id, uint32(pos))
					ix.indexDerivedLocked(fi, opt, t, id)
				}
				if opt.Phrase {
					addPhrases(fi, ix.tokens, id)
				}
			} else {
				ix.indexDerivedLocked(fi, opt, val, id)
			}
		}
		if opt.Kind == FieldVector || opt.Dim > 0 {
			if vec, ok := toVector(v); ok {
				ix.addVectorLocked(field, id, vec)
			}
		}
		if opt.TTLField {
			if ts := toUnix(doc[field]); ts > 0 {
				ix.expires[id] = ts
			}
		}
	}
}
func (ix *Index) indexDerivedLocked(fi *fieldIndex, opt FieldOptions, val string, id DocID) {
	if opt.Prefix {
		ix.scratch = prefixes(val, ix.scratch)
		for _, p := range ix.scratch {
			addPosting(fi.prefix, fi.prefixOne, p, id, fi.capHint)
		}
	}
	if opt.Suffix {
		ix.scratch = suffixes(val, ix.scratch)
		for _, s := range ix.scratch {
			addPosting(fi.suffix, fi.suffixOne, s, id, fi.capHint)
		}
	}
	if opt.Ngram {
		ix.scratch = grams(val, opt.MinGram, opt.MaxGram, ix.scratch)
		for _, g := range ix.scratch {
			addPosting(fi.ngram, fi.ngramOne, g, id, fi.capHint)
		}
	}
}

func postingCount(m map[string]*Bitmap, one map[string]DocID, term string) int {
	if did := one[term]; did != 0 {
		return 1
	}
	if b := m[term]; b != nil {
		return b.Count()
	}
	return 0
}

func postingBitmap(m map[string]*Bitmap, one map[string]DocID, term string, max DocID) *Bitmap {
	if did := one[term]; did != 0 {
		b := NewBitmapCap(max)
		b.Add(did)
		return b
	}
	if b := m[term]; b != nil {
		return b
	}
	return nil
}

func postingClone(m map[string]*Bitmap, one map[string]DocID, term string, max DocID) *Bitmap {
	if did := one[term]; did != 0 {
		b := NewBitmapCap(max)
		b.Add(did)
		return b
	}
	if b := m[term]; b != nil {
		return b.Clone()
	}
	return NewBitmap()
}

func addTerm(fi *fieldIndex, term string, id DocID, fuzzy bool) {
	if term == "" {
		return
	}
	if fuzzy && fi.termOne[term] == 0 && fi.terms[term] == nil {
		fi.fuzzyTerms[term[0]] = append(fi.fuzzyTerms[term[0]], term)
	}
	addPosting(fi.terms, fi.termOne, term, id, fi.capHint)
}

func add(m map[string]*Bitmap, term string, id DocID) {
	// Compatibility helper for call sites that do not have singleton maps.
	b := m[term]
	if b == nil {
		b = NewBitmap()
		m[term] = b
	}
	b.Add(id)
}

func addPosting(m map[string]*Bitmap, one map[string]DocID, term string, id DocID, capHint DocID) {
	if term == "" {
		return
	}
	if b := m[term]; b != nil {
		b.Add(id)
		return
	}
	if prev := one[term]; prev != 0 {
		if prev == id {
			return
		}
		// Do not pre-size every promoted posting to the whole index capacity.
		// High-cardinality sparse terms dominate charge-master style indexes; a
		// dense cap-sized bitmap per repeated term can consume many GB. Let the
		// bitmap grow only as far as the actual document ids it receives.
		b := NewBitmap()
		b.Add(prev)
		b.Add(id)
		m[term] = b
		delete(one, term)
		return
	}
	one[term] = id
}

func addPos(fi *fieldIndex, term string, id DocID, pos uint32) {
	pm := fi.positions[term]
	if pm == nil {
		pm = map[DocID][]uint32{}
		fi.positions[term] = pm
	}
	pm[id] = append(pm[id], pos)
}
func addPhrases(fi *fieldIndex, toks []string, id DocID) {
	// Index short exact phrases because they dominate search-as-you-type and common phrase queries.
	// Longer/sloppy phrases still use positional validation.
	for n := 2; n <= 4; n++ {
		if len(toks) < n {
			break
		}
		for i := 0; i+n <= len(toks); i++ {
			addPosting(fi.phrases, fi.phraseOne, strings.Join(toks[i:i+n], "\x1f"), id, fi.capHint)
		}
	}
}
func (ix *Index) liveBitmapLocked() *Bitmap {
	return ix.live.Clone()
}
func (ix *Index) isDeletedOrExpiredLocked(id DocID) bool {
	if ix.deleted.Has(id) {
		return true
	}
	if ix.hasTTL {
		if exp, ok := ix.expires[id]; ok && exp > 0 && ix.clock.Unix() > exp {
			return true
		}
	}
	return false
}
func (ix *Index) termBitmap(field, value string) *Bitmap {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	opt := ix.cfg.Schema.Fields[field]
	fi := ix.fields[field]
	if fi == nil {
		return NewBitmap()
	}
	val := normalizeSpecialString(field, value, opt.Lowercase)
	if did, ok := fi.unique[val]; ok && !ix.deleted.Has(did) {
		r := NewBitmapCap(ix.nextDocID)
		r.Add(did)
		return r
	}
	if did := fi.termOne[val]; did != 0 && !ix.deleted.Has(did) {
		r := NewBitmapCap(ix.nextDocID)
		r.Add(did)
		return r
	}
	if b := fi.terms[val]; b != nil {
		return b
	}
	return NewBitmap()
}
func (ix *Index) prefixBitmap(field, value string) *Bitmap {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	opt := ix.cfg.Schema.Fields[field]
	fi := ix.fields[field]
	if fi == nil {
		return NewBitmap()
	}
	val := normalize(value, opt.Lowercase)
	if did := fi.prefixOne[val]; did != 0 && !ix.deleted.Has(did) {
		r := NewBitmapCap(ix.nextDocID)
		r.Add(did)
		return r
	}
	if b := fi.prefix[val]; b != nil {
		return b
	}
	return NewBitmap()
}
func (ix *Index) suffixBitmap(field, value string) *Bitmap {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	opt := ix.cfg.Schema.Fields[field]
	fi := ix.fields[field]
	if fi == nil {
		return NewBitmap()
	}
	val := normalize(value, opt.Lowercase)
	if did := fi.suffixOne[val]; did != 0 && !ix.deleted.Has(did) {
		r := NewBitmapCap(ix.nextDocID)
		r.Add(did)
		return r
	}
	if b := fi.suffix[val]; b != nil {
		return b
	}
	return NewBitmap()
}
func (ix *Index) ngramBitmap(field, value string) *Bitmap {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	opt := ix.cfg.Schema.Fields[field]
	fi := ix.fields[field]
	if fi == nil {
		return NewBitmap()
	}
	value = normalize(value, opt.Lowercase)
	gs := grams(value, opt.MinGram, opt.MaxGram, nil)
	var r *Bitmap
	for _, g := range gs {
		var b *Bitmap
		if did := fi.ngramOne[g]; did != 0 && !ix.deleted.Has(did) {
			b = NewBitmapCap(ix.nextDocID)
			b.Add(did)
		} else if bb := fi.ngram[g]; bb != nil {
			b = bb
		} else {
			return NewBitmap()
		}
		if r == nil {
			r = b.Clone()
		} else {
			r = r.AndInPlace(b)
		}
	}
	if r == nil {
		return NewBitmap()
	}
	return r
}
func (ix *Index) existsBitmap(field string) *Bitmap {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	fi := ix.fields[field]
	if fi == nil {
		return NewBitmap()
	}
	return fi.exists
}
func (ix *Index) rangeBitmap(field string, gte, gt, lte, lt any) *Bitmap {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if ix.numericExists[field] == nil {
		return NewBitmap()
	}
	min, hasMin, minInc := boundFloat(gte, gt, true)
	max, hasMax, maxInc := boundFloat(lte, lt, false)
	pairs := ix.sortedNumericLocked(field)
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
	r := NewBitmapCap(ix.nextDocID)
	for _, p := range pairs[lo:hi] {
		if !ix.deleted.Has(p.id) {
			r.Add(p.id)
		}
	}
	return r
}
func (ix *Index) sortedIPv4Locked(field string) []ipPair {
	if !ix.ip4Dirty[field] && ix.ip4Sorted[field] != nil {
		return ix.ip4Sorted[field]
	}
	vals := ix.ip4[field]
	pairs := ix.ip4Sorted[field]
	if cap(pairs) < len(vals) {
		pairs = make([]ipPair, 0, len(vals))
	} else {
		pairs = pairs[:0]
	}
	for id, v := range vals {
		pairs = append(pairs, ipPair{id: id, v: v})
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].v == pairs[j].v {
			return pairs[i].id < pairs[j].id
		}
		return pairs[i].v < pairs[j].v
	})
	ix.ip4Sorted[field] = pairs
	ix.ip4Dirty[field] = false
	return pairs
}

func (ix *Index) sortedNumericLocked(field string) []numPair {
	if !ix.numDirty[field] && ix.numSorted[field] != nil {
		return ix.numSorted[field]
	}
	ex := ix.numericExists[field]
	vals := ix.numericDense[field]
	count := 0
	if ex != nil {
		count = ex.Count()
	}
	pairs := ix.numSorted[field]
	if cap(pairs) < count {
		pairs = make([]numPair, 0, count)
	} else {
		pairs = pairs[:0]
	}
	if ex != nil {
		ex.Each(func(id DocID) bool {
			if int(id) < len(vals) {
				pairs = append(pairs, numPair{id: id, v: vals[id]})
			}
			return true
		})
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].v == pairs[j].v {
			return pairs[i].id < pairs[j].id
		}
		return pairs[i].v < pairs[j].v
	})
	ix.numSorted[field] = pairs
	ix.numDirty[field] = false
	return pairs
}
func boundFloat(inclusive, exclusive any, lower bool) (float64, bool, bool) {
	if exclusive != nil {
		if f, ok := toFloatScalar(exclusive); ok {
			return f, true, false
		}
	}
	if inclusive != nil {
		if f, ok := toFloatScalar(inclusive); ok {
			return f, true, true
		}
	}
	return 0, false, true
}
func (ix *Index) fuzzyBitmap(field, value string, dist, limit int) *Bitmap {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	opt := ix.cfg.Schema.Fields[field]
	fi := ix.fields[field]
	if fi == nil {
		return NewBitmap()
	}
	value = normalize(value, opt.Lowercase)
	if value == "" {
		return NewBitmap()
	}
	if dist <= 0 {
		dist = 1
	}
	if limit <= 0 {
		limit = 256
	}
	r := NewBitmapCap(ix.nextDocID)
	seen := 0
	candidates := fi.fuzzyTerms[value[0]]
	// For distance >1, include neighboring buckets only if the exact first-byte bucket is empty;
	// this keeps the default typo path fast while still returning useful results.
	if len(candidates) == 0 && dist > 1 {
		for _, bucket := range fi.fuzzyTerms {
			candidates = append(candidates, bucket...)
		}
	}
	for _, term := range candidates {
		if abs(len(term)-len(value)) > dist {
			continue
		}
		if levenshtein(term, value, dist) <= dist {
			if did := fi.termOne[term]; did != 0 && !ix.deleted.Has(did) {
				r.Add(did)
			} else if b := fi.terms[term]; b != nil {
				r = r.OrInPlace(b)
			}
			seen++
			if seen >= limit {
				break
			}
		}
	}
	return r
}

func normalizeStringByKind(kind specialNormalizer, raw string, lower bool) string {
	s := normalize(raw, lower)
	switch kind {
	case specialEmail:
		return normalizeEmail(s)
	case specialPhone:
		return normalizePhone(s)
	case specialDomain:
		return normalizeDomain(s)
	case specialURL:
		return normalizeURL(s)
	default:
		return s
	}
}

func normalizeSpecialKind(kind specialNormalizer, v any, lower bool) string {
	var raw string
	switch x := v.(type) {
	case string:
		raw = x
	case []byte:
		raw = string(x)
	case int:
		raw = strconv.Itoa(x)
	case int64:
		raw = strconv.FormatInt(x, 10)
	case float64:
		raw = strconv.FormatFloat(x, 'f', -1, 64)
	case bool:
		raw = strconv.FormatBool(x)
	case time.Time:
		raw = x.Format(time.RFC3339)
	default:
		raw = fmt.Sprint(v)
	}
	s := normalize(raw, lower)
	switch kind {
	case specialEmail:
		return normalizeEmail(s)
	case specialPhone:
		return normalizePhone(s)
	case specialDomain:
		return normalizeDomain(s)
	case specialURL:
		return normalizeURL(s)
	default:
		return s
	}
}

func normalizeSpecial(field string, v any, lower bool) string {
	var raw string
	switch x := v.(type) {
	case string:
		raw = x
	case []byte:
		raw = string(x)
	case int:
		raw = strconv.Itoa(x)
	case int64:
		raw = strconv.FormatInt(x, 10)
	case float64:
		raw = strconv.FormatFloat(x, 'f', -1, 64)
	case bool:
		raw = strconv.FormatBool(x)
	case time.Time:
		raw = x.Format(time.RFC3339)
	default:
		raw = fmt.Sprint(v)
	}
	s := normalize(raw, lower)
	lf := strings.ToLower(field)
	switch {
	case strings.Contains(lf, "email"):
		return normalizeEmail(s)
	case strings.Contains(lf, "phone") || strings.Contains(lf, "mobile"):
		return normalizePhone(s)
	case strings.Contains(lf, "domain") || strings.Contains(lf, "host"):
		return normalizeDomain(s)
	case strings.Contains(lf, "url") || strings.Contains(lf, "uri"):
		return normalizeURL(s)
	default:
		return s
	}
}
func normalizeSpecialString(field, s string, lower bool) string {
	s = normalize(s, lower)
	lf := strings.ToLower(field)
	switch {
	case strings.Contains(lf, "email"):
		return normalizeEmail(s)
	case strings.Contains(lf, "phone") || strings.Contains(lf, "mobile"):
		return normalizePhone(s)
	case strings.Contains(lf, "domain") || strings.Contains(lf, "host"):
		return normalizeDomain(s)
	case strings.Contains(lf, "url") || strings.Contains(lf, "uri"):
		return normalizeURL(s)
	default:
		return s
	}
}

func toVector(v any) ([]float64, bool) {
	switch x := v.(type) {
	case []float64:
		out := append([]float64(nil), x...)
		return out, true
	case []any:
		out := make([]float64, 0, len(x))
		for _, e := range x {
			f, ok := toFloatScalar(e)
			if !ok {
				return nil, false
			}
			out = append(out, f)
		}
		return out, true
	case []int:
		out := make([]float64, len(x))
		for i, n := range x {
			out[i] = float64(n)
		}
		return out, true
	}
	return nil, false
}

func valuesInto(v any, dst []string) []string {
	dst = dst[:0]
	switch x := v.(type) {
	case string:
		return append(dst, x)
	case []string:
		return append(dst, x...)
	case []any:
		for _, e := range x {
			dst = append(dst, fmt.Sprint(e))
		}
		return dst
	case int:
		return append(dst, strconv.Itoa(x))
	case int64:
		return append(dst, strconv.FormatInt(x, 10))
	case float64:
		return append(dst, strconv.FormatFloat(x, 'f', -1, 64))
	case bool:
		return append(dst, strconv.FormatBool(x))
	case time.Time:
		return append(dst, x.Format(time.RFC3339))
	default:
		return append(dst, fmt.Sprint(v))
	}
}

func values(v any) []string {
	switch x := v.(type) {
	case string:
		return []string{x}
	case []string:
		return x
	case []any:
		out := make([]string, 0, len(x))
		for _, e := range x {
			out = append(out, fmt.Sprint(e))
		}
		return out
	case int:
		return []string{strconv.Itoa(x)}
	case int64:
		return []string{strconv.FormatInt(x, 10)}
	case float64:
		return []string{strconv.FormatFloat(x, 'f', -1, 64)}
	case bool:
		return []string{strconv.FormatBool(x)}
	case time.Time:
		return []string{x.Format(time.RFC3339)}
	default:
		return []string{fmt.Sprint(v)}
	}
}
func toUnix(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case float64:
		return int64(x)
	case json.Number:
		i, _ := x.Int64()
		return i
	case string:
		if i, err := strconv.ParseInt(x, 10, 64); err == nil {
			return i
		}
		if t, err := time.Parse(time.RFC3339, x); err == nil {
			return t.Unix()
		}
	}
	return 0
}
func toFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case []any:
		if len(x) == 0 {
			return 0, false
		}
		return toFloatScalar(x[0])
	case []string:
		if len(x) == 0 {
			return 0, false
		}
		return toFloatScalar(x[0])
	default:
		return toFloatScalar(v)
	}
}
func toFloatScalar(v any) (float64, bool) {
	switch x := v.(type) {
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case float64:
		return x, true
	case json.Number:
		f, err := x.Float64()
		return f, err == nil
	case time.Time:
		return float64(x.Unix()), true
	case string:
		if f, err := strconv.ParseFloat(x, 64); err == nil {
			return f, true
		}
		if t, err := time.Parse(time.RFC3339, x); err == nil {
			return float64(t.Unix()), true
		}
	}
	return 0, false
}
func cloneDoc(d Document) Document {
	if d == nil {
		return nil
	}
	c := make(Document, len(d))
	for k, v := range d {
		c[k] = v
	}
	return c
}
func dir(p string) string {
	i := strings.LastIndexByte(p, '/')
	if i < 0 {
		return "."
	}
	return p[:i]
}

type walRecord struct {
	Op  string   `json:"op"`
	ID  string   `json:"id"`
	Doc Document `json:"doc,omitempty"`
}

func (ix *Index) appendWALLocked(r walRecord) error {
	if ix.wal == nil {
		return nil
	}
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	_, err = ix.wal.Write(b)
	return err
}
func (ix *Index) Flush() error {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if ix.wal != nil {
		return ix.wal.Sync()
	}
	return nil
}
func (ix *Index) ReplayWAL(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()
	rd := bufio.NewReader(f)
	for {
		line, err := rd.ReadBytes('\n')
		if len(line) > 0 {
			var r walRecord
			if json.Unmarshal(line, &r) == nil {
				switch r.Op {
				case "upsert":
					_ = ix.upsert(r.ID, r.Doc, false)
				case "delete":
					_ = ix.delete(r.ID, false)
				}
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	return nil
}

type snapshot struct {
	Config Config              `json:"config"`
	Docs   map[string]Document `json:"docs"`
}

func (ix *Index) SaveSnapshot(path string) error {
	ix.mu.RLock()
	snap := snapshot{Config: ix.cfg, Docs: map[string]Document{}}
	for id, did := range ix.extToDoc {
		if !ix.isDeletedOrExpiredLocked(did) {
			if ix.docs[did] != nil {
				snap.Docs[id] = cloneDoc(ix.docs[did])
			}
		}
	}
	ix.mu.RUnlock()
	if err := os.MkdirAll(dir(path), 0755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	err = json.NewEncoder(f).Encode(snap)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
func (ix *Index) LoadSnapshot(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()
	var snap snapshot
	dec := json.NewDecoder(f)
	dec.UseNumber()
	if err := dec.Decode(&snap); err != nil {
		return err
	}
	for id, doc := range snap.Docs {
		_ = ix.upsert(id, doc, false)
	}
	return nil
}

func (ix *Index) estimateCardinality(q Query) int {
	switch v := q.(type) {
	case Term:
		ix.mu.RLock()
		defer ix.mu.RUnlock()
		fi := ix.fields[v.Field]
		if fi == nil {
			return 0
		}
		if b := fi.terms[normalize(v.Value, ix.cfg.Schema.Fields[v.Field].Lowercase)]; b != nil {
			return b.Count()
		}
		return 0
	case Prefix:
		ix.mu.RLock()
		defer ix.mu.RUnlock()
		fi := ix.fields[v.Field]
		if fi == nil {
			return 0
		}
		if b := fi.prefix[normalize(v.Value, ix.cfg.Schema.Fields[v.Field].Lowercase)]; b != nil {
			return b.Count()
		}
		return 0
	case Suffix:
		ix.mu.RLock()
		defer ix.mu.RUnlock()
		fi := ix.fields[v.Field]
		if fi == nil {
			return 0
		}
		if b := fi.suffix[normalize(v.Value, ix.cfg.Schema.Fields[v.Field].Lowercase)]; b != nil {
			return b.Count()
		}
		return 0
	case Exists:
		ix.mu.RLock()
		defer ix.mu.RUnlock()
		if fi := ix.fields[v.Field]; fi != nil {
			return fi.exists.Count()
		}
		return 0
	default:
		return 1 << 30
	}
}

func (ix *Index) EachTerm(field, value string, fn func(id string, docID DocID) bool) {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	opt := ix.cfg.Schema.Fields[field]
	fi := ix.fields[field]
	if fi == nil {
		return
	}
	val := normalize(value, opt.Lowercase)
	if did, ok := fi.unique[val]; ok {
		if !ix.isDeletedOrExpiredLocked(did) {
			fn(ix.docToExt[did], did)
		}
		return
	}
	if did := fi.termOne[val]; did != 0 {
		if !ix.isDeletedOrExpiredLocked(did) {
			fn(ix.docToExt[did], did)
		}
		return
	}
	b := fi.terms[val]
	if b == nil {
		return
	}
	b.Each(func(id DocID) bool {
		if !ix.isDeletedOrExpiredLocked(id) {
			return fn(ix.docToExt[id], id)
		}
		return true
	})
}

func (ix *Index) CollectTerm(field, value string, dst []string) []string {
	dst = dst[:0]
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	opt := ix.cfg.Schema.Fields[field]
	fi := ix.fields[field]
	if fi == nil {
		return dst
	}
	val := normalize(value, opt.Lowercase)
	if did, ok := fi.unique[val]; ok {
		if !ix.isDeletedOrExpiredLocked(did) {
			dst = append(dst, ix.docToExt[did])
		}
		return dst
	}
	if did := fi.termOne[val]; did != 0 {
		if !ix.isDeletedOrExpiredLocked(did) {
			dst = append(dst, ix.docToExt[did])
		}
		return dst
	}
	b := fi.terms[val]
	if b == nil {
		return dst
	}
	b.Each(func(id DocID) bool {
		if !ix.isDeletedOrExpiredLocked(id) {
			dst = append(dst, ix.docToExt[id])
		}
		return true
	})
	return dst
}
