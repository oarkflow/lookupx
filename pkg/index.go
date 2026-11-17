package pkg

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
	termKeys                                []string
	termKeysDirty                           bool
	patternCache                            map[string]*Bitmap
	patternCacheOrder                       []string
	patternCacheBytes                       int
	patternCacheMu                          sync.Mutex
	patternBuildMu                          sync.Mutex
	exists                                  *Bitmap
	capHint                                 DocID
	docLengths                              []uint32
	totalTokens                             uint64
	extraTF                                 map[string]map[DocID]uint16
}

type globalScoreWorkspace struct {
	scores  []float64
	touched []DocID
}

var globalScorePool = sync.Pool{New: func() any { return &globalScoreWorkspace{} }}

func newFieldIndex(capHint int, opt FieldOptions) *fieldIndex {
	// Allocate large maps only for structures that are actually used. This keeps
	// memory low while avoiding repeated map growth on the ingestion hot path.
	termCap, uniqueCap, prefixCap, suffixCap, ngramCap := 0, 0, 0, 0, 0
	if opt.Unique {
		uniqueCap = capHint
	} else if opt.Lookup || opt.Kind == FieldKeyword || opt.Kind == FieldBool || opt.Kind == FieldText || opt.Indexed {
		// Do not preallocate keyword/text term maps from InitialCapacity. A 1.5M-row
		// charge-master build with 6 keyword fields used to allocate hundreds of MB
		// before the first page was indexed. Most lookup fields are low/medium
		// cardinality and Go maps grow acceptably; unique fields still preallocate.
		termCap = 64
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
		unique: make(map[string]DocID, uniqueCap), fuzzyTerms: map[byte][]string{}, exists: NewBitmapCap(DocID(capHint + 1)), capHint: DocID(capHint + 1), extraTF: map[string]map[DocID]uint16{},
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
	id           FieldID
	name         string
	opt          FieldOptions
	fi           *fieldIndex
	special      specialNormalizer
	analyzeFn    func(text string, dst []string) []string
	stringsCol   []string
	numericDense []float64
	numericEx    *Bitmap
	noDerived    bool
	noLower      bool
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
	mu                 sync.RWMutex
	cfg                Config
	nextDocID          DocID
	extToDoc           map[string]DocID
	docToExt           []string
	docs               []Document
	deleted            *Bitmap
	live               *Bitmap
	expires            map[DocID]int64
	hasTTL             bool
	hasDeletes         bool
	fields             map[string]*fieldIndex
	fieldList          []schemaField
	termSearchFields   []string
	globalSearchFields []string
	fieldIDs           map[string]FieldID
	clock              Clock
	numeric            map[string]map[DocID]float64
	numericDense       map[string][]float64
	numericExists      map[string]*Bitmap
	numSorted          map[string][]numPair
	numDirty           map[string]bool
	strings            map[string][]string
	vectors            map[string]map[DocID][]float64
	anns               map[string]*VectorANN
	ip4                map[string]map[DocID]uint32
	ip4Sorted          map[string][]ipPair
	ip4Dirty           map[string]bool
	wal                *os.File
	walSeq             uint64
	tokens             []string
	scratch            []string
	valScratch         []string
}

func New(cfg Config) (*Index, error) {
	capHint := cfg.InitialCapacity
	if capHint < 0 {
		capHint = 0
	}
	ix := &Index{cfg: cfg, nextDocID: 1, deleted: NewBitmapCap(DocID(capHint + 1)), live: NewBitmapCap(DocID(capHint + 1)), expires: make(map[DocID]int64), fields: map[string]*fieldIndex{}, fieldIDs: map[string]FieldID{}, numeric: map[string]map[DocID]float64{}, numericDense: map[string][]float64{}, numericExists: map[string]*Bitmap{}, numSorted: map[string][]numPair{}, numDirty: map[string]bool{}, strings: map[string][]string{}, vectors: map[string]map[DocID][]float64{}, anns: map[string]*VectorANN{}, ip4: map[string]map[DocID]uint32{}, ip4Sorted: map[string][]ipPair{}, ip4Dirty: map[string]bool{}, docToExt: make([]string, 1, capHint+1)}
	if !cfg.DisableSource {
		ix.docs = make([]Document, 1, capHint+1)
	}
	if !cfg.AppendOnly {
		ix.extToDoc = make(map[string]DocID, capHint)
	}
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
		if opt.Kind == FieldText || opt.Kind == FieldKeyword || opt.Kind == FieldBool {
			aliasTarget, hasAliasTarget := cfg.Schema.Fields[opt.IndexAlias]
			deduplicateAlias := opt.IndexAlias != "" && opt.IndexAlias != name && hasAliasTarget &&
				(aliasTarget.Kind == FieldText || aliasTarget.Kind == FieldKeyword || aliasTarget.Kind == FieldBool)
			if !deduplicateAlias {
				ix.globalSearchFields = append(ix.globalSearchFields, name)
			}
			if opt.TermSearch && !(deduplicateAlias && aliasTarget.TermSearch) {
				ix.termSearchFields = append(ix.termSearchFields, name)
			}
		}
		fi := newFieldIndex(capHint, opt)
		sp := detectSpecial(name)
		ix.fields[name] = fi
		ix.fieldIDs[name] = fid
		var analyzeFn func(text string, dst []string) []string
		if opt.Kind == FieldText || opt.Indexed {
			aname := opt.Analyzer
			if aname == "" {
				aname = "standard"
			}
			if a := analyzers[aname]; a != nil {
				analyzeFn = a.Analyze
			}
		}
		sf := schemaField{
			id: fid, name: name, opt: opt, fi: fi, special: sp,
			analyzeFn: analyzeFn,
			noDerived: !opt.Prefix && !opt.Suffix && !opt.Ngram,
		}
		ix.fieldList = append(ix.fieldList, sf)
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
			if opt.IndexAlias == "" && opt.StoredAlias == "" {
				ix.strings[name] = make([]string, 1, capHint+1)
			}
		}
		if opt.Kind == FieldVector || opt.Dim > 0 {
			ix.vectors[name] = make(map[DocID][]float64, capHint)
			ix.anns[name] = newVectorANNWithOptions(opt, capHint)
		}
	}
	// Explicit text aliases share postings. Field values and result columns stay
	// independent, while identical text fields avoid building duplicate term maps.
	for i := range ix.fieldList {
		sf := &ix.fieldList[i]
		if sf.opt.IndexAlias == "" {
			continue
		}
		target, ok := ix.fields[sf.opt.IndexAlias]
		if !ok || sf.opt.IndexAlias == sf.name {
			return nil, fmt.Errorf("field %q has invalid index_alias %q", sf.name, sf.opt.IndexAlias)
		}
		targetOpts := cfg.Schema.Fields[sf.opt.IndexAlias]
		if sf.opt.Kind != FieldText || targetOpts.Kind != FieldText || targetOpts.IndexAlias != "" || sf.opt.Analyzer != targetOpts.Analyzer || sf.opt.Lowercase != targetOpts.Lowercase || sf.opt.Fuzzy != targetOpts.Fuzzy || sf.opt.Phrase != targetOpts.Phrase || sf.opt.Prefix != targetOpts.Prefix || sf.opt.Suffix != targetOpts.Suffix || sf.opt.Ngram != targetOpts.Ngram {
			return nil, fmt.Errorf("field %q index_alias %q requires matching text-index options", sf.name, sf.opt.IndexAlias)
		}
		sf.fi = target
		ix.fields[sf.name] = target
		if targetColumn, ok := ix.strings[sf.opt.IndexAlias]; ok && (sf.opt.Stored || sf.opt.Facetable || sf.special == specialDomain || sf.special == specialURL) {
			ix.strings[sf.name] = targetColumn
		}
	}
	// Stored aliases share the dense DocID column for identical projected values.
	for i := range ix.fieldList {
		sf := &ix.fieldList[i]
		if sf.opt.StoredAlias == "" {
			continue
		}
		target := sf.opt.StoredAlias
		column, ok := ix.strings[target]
		if !ok || target == sf.name || !(sf.opt.Stored || sf.opt.Facetable || sf.special == specialDomain || sf.special == specialURL) {
			return nil, fmt.Errorf("field %q has invalid stored_alias %q", sf.name, target)
		}
		ix.strings[sf.name] = column
	}
	for i, sf := range ix.fieldList {
		if col, ok := ix.strings[sf.name]; ok {
			ix.fieldList[i].stringsCol = col
		}
		if dense, ok := ix.numericDense[sf.name]; ok {
			ix.fieldList[i].numericDense = dense
			ix.fieldList[i].numericEx = ix.numericExists[sf.name]
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
		if err := ix.LoadSnapshot(cfg.SnapshotPath); err != nil {
			_ = ix.Close()
			return nil, err
		}
	}
	if cfg.EnableWAL && cfg.WALPath != "" {
		if err := ix.ReplayWAL(cfg.WALPath); err != nil {
			_ = ix.Close()
			return nil, err
		}
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
	if err := ix.validateDocumentLocked(doc); err != nil {
		return err
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
	if !ix.cfg.AppendOnly {
		if old, ok := ix.extToDoc[id]; ok {
			ix.deleted.Add(old)
			ix.live.Remove(old)
			ix.hasDeletes = true
		}
	}
	did := ix.nextDocID
	ix.nextDocID++
	if !ix.cfg.AppendOnly {
		ix.extToDoc[id] = did
	}
	ix.live.Add(did)
	ix.docToExt = append(ix.docToExt, id)
	if !ix.cfg.DisableSource {
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
	if ix.cfg.AppendOnly {
		return errors.New("delete by external id is disabled in append-only indexes")
	}
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
	if ix.cfg.AppendOnly {
		return nil, false
	}
	did, ok := ix.extToDoc[id]
	if !ok || ix.isDeletedOrExpiredLocked(did) {
		return nil, false
	}
	return ix.documentLocked(did), true
}

// documentLocked returns the source document when retained, otherwise it
// reconstructs a result document from configured stored columns. Callers hold
// at least the index read lock.
func (ix *Index) documentLocked(id DocID) Document {
	if int(id) < len(ix.docs) && ix.docs[id] != nil {
		return cloneDoc(ix.docs[id])
	}
	doc := make(Document, len(ix.fieldList))
	for _, field := range ix.fieldList {
		doc[field.name] = nil
	}
	for field, column := range ix.strings {
		if value, ok := stringColumnValue(column, id); ok {
			doc[field] = value
		}
	}
	return doc
}

func stringColumnValue(col []string, id DocID) (string, bool) {
	if id == 0 || uint64(id) >= uint64(len(col)) || col[id] == "" {
		return "", false
	}
	return col[id], true
}

// setStringColumnLocked stores a value in a dense DocID-aligned column. Dense
// columns avoid the hash-bucket and key overhead of one map entry per stored
// field per document. Callers hold ix.mu for writing.
func (ix *Index) setStringColumnLocked(field string, id DocID, value string) {
	col, ok := ix.strings[field]
	if !ok || id == 0 {
		return
	}
	need := int(id) + 1
	if need > cap(col) {
		newCap := cap(col) * 2
		if newCap < need {
			newCap = need
		}
		next := make([]string, len(col), newCap)
		copy(next, col)
		col = next
	}
	if need > len(col) {
		col = col[:need]
	}
	col[id] = value
	root := field
	opt := ix.cfg.Schema.Fields[field]
	if opt.StoredAlias != "" {
		root = opt.StoredAlias
	} else if opt.IndexAlias != "" {
		root = opt.IndexAlias
	}
	for i := range ix.fieldList {
		sf := &ix.fieldList[i]
		if sf.name == root || sf.opt.IndexAlias == root || sf.opt.StoredAlias == root {
			sf.stringsCol = col
			ix.strings[sf.name] = col
		}
	}
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
	if !ix.hasTTL {
		return ix.live.Clone().AndInPlace(bm).Count()
	}
	n := 0
	bm.Each(func(id DocID) bool {
		if !ix.isDeletedOrExpiredLocked(id) {
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
	if req.AfterDocID > 0 && len(req.Sort) == 0 && len(req.Facets) == 0 {
		total := -1
		if !req.SkipTotal {
			total = ix.Count(req.Query)
		}
		hits := make([]Hit, 0, req.Limit)
		ix.mu.RLock()
		bm.EachAfter(req.AfterDocID, func(id DocID) bool {
			if ix.isDeletedOrExpiredLocked(id) {
				return true
			}
			h := Hit{ID: ix.docToExt[id], DocID: id, Score: 1}
			if req.WithDocs {
				h.Doc = ix.documentLocked(id)
			}
			hits = append(hits, h)
			return len(hits) < req.Limit
		})
		ix.mu.RUnlock()
		return Result{Total: total, Hits: hits, Took: ix.elapsed(start)}
	}
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	if global, ok := globalTermFromQuery(req.Query); ok && len(req.Sort) == 0 && len(req.Facets) == 0 {
		type scoredDoc struct {
			id    DocID
			score float64
		}
		ws := globalScorePool.Get().(*globalScoreWorkspace)
		if cap(ws.scores) < int(ix.nextDocID) {
			ws.scores = make([]float64, ix.nextDocID)
		} else {
			ws.scores = ws.scores[:ix.nextDocID]
		}
		ws.touched = ws.touched[:0]
		ix.globalBM25ScoresLocked(global, ws)
		ranked := make([]scoredDoc, 0, bm.Count())
		bm.Each(func(id DocID) bool {
			if !ix.isDeletedOrExpiredLocked(id) {
				score := ws.scores[id]
				if score == 0 && global.Fuzzy {
					score = 0.1
				}
				ranked = append(ranked, scoredDoc{id: id, score: score})
			}
			return true
		})
		sort.Slice(ranked, func(i, j int) bool {
			if ranked[i].score == ranked[j].score {
				return ranked[i].id < ranked[j].id
			}
			return ranked[i].score > ranked[j].score
		})
		from := req.Offset
		if from > len(ranked) {
			from = len(ranked)
		}
		to := from + req.Limit
		if to > len(ranked) {
			to = len(ranked)
		}
		hits := make([]Hit, 0, to-from)
		for _, rankedDoc := range ranked[from:to] {
			id := rankedDoc.id
			h := Hit{ID: ix.docToExt[id], DocID: id, Score: rankedDoc.score}
			if req.WithDocs {
				h.Doc = ix.documentLocked(id)
			}
			hits = append(hits, h)
		}
		for _, id := range ws.touched {
			ws.scores[id] = 0
		}
		globalScorePool.Put(ws)
		return Result{Total: len(ranked), Hits: hits, Took: ix.elapsed(start)}
	}
	if len(req.Sort) == 0 && len(req.Facets) == 0 {
		hits := make([]Hit, 0, req.Limit)
		skipped := 0
		total := -1
		if !req.SkipTotal || req.AfterDocID == 0 {
			total = ix.countLiveLocked(bm)
		}
		bm.Each(func(id DocID) bool {
			if ix.isDeletedOrExpiredLocked(id) {
				return true
			}
			if skipped < req.Offset {
				skipped++
				return true
			}
			if len(hits) >= req.Limit {
				return false
			}
			h := Hit{ID: ix.docToExt[id], DocID: id, Score: 1}
			if req.WithDocs {
				h.Doc = ix.documentLocked(id)
			}
			hits = append(hits, h)
			return len(hits) < req.Limit
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
		if req.WithDocs {
			h.Doc = ix.documentLocked(id)
		}
		hits = append(hits, h)
	}
	return Result{Total: total, Hits: hits, Facets: facets, Took: ix.elapsed(start)}
}

func globalTermFromQuery(q Query) (GlobalTerm, bool) {
	switch x := q.(type) {
	case GlobalTerm:
		return x, true
	case Bool:
		for _, list := range [][]Query{x.Must, x.Filter, x.Should} {
			for _, child := range list {
				if g, ok := globalTermFromQuery(child); ok {
					return g, true
				}
			}
		}
	case And:
		for _, child := range x {
			if g, ok := globalTermFromQuery(child); ok {
				return g, true
			}
		}
	}
	return GlobalTerm{}, false
}

func (ix *Index) globalBM25ScoresLocked(q GlobalTerm, ws *globalScoreWorkspace) {
	add := func(id DocID, score float64) {
		if ws.scores[id] == 0 {
			ws.touched = append(ws.touched, id)
		}
		ws.scores[id] += score
	}
	n := float64(ix.live.Count())
	fields := ix.globalTermFields(q)
	for _, word := range q.Words {
		for _, field := range fields {
			opt, ok := ix.cfg.Schema.Fields[field]
			if !ok {
				continue
			}
			if opt.Kind != FieldText && opt.Kind != FieldKeyword && opt.Kind != FieldBool {
				continue
			}
			fi := ix.fields[field]
			if fi == nil {
				continue
			}
			term := normalize(word, opt.Lowercase)
			avgdl := 1.0
			if docs := fi.exists.Count(); docs > 0 && fi.totalTokens > 0 {
				avgdl = float64(fi.totalTokens) / float64(docs)
			}
			if did := fi.termOne[term]; did != 0 {
				idf := math.Log(1 + (n-1+0.5)/(1+0.5))
				add(did, idf*ix.bm25TFNormLocked(fi, term, did, avgdl))
			}
			if b := fi.terms[term]; b != nil {
				df := float64(b.Count())
				idf := math.Log(1 + (n-df+0.5)/(df+0.5))
				b.Each(func(id DocID) bool { add(id, idf*ix.bm25TFNormLocked(fi, term, id, avgdl)); return true })
			}
			if did, exists := fi.unique[term]; exists {
				add(did, math.Log(1+(n-1+0.5)/(1+0.5))*ix.bm25TFNormLocked(fi, term, did, avgdl))
			}
			if q.Fuzzy && opt.Fuzzy && term != "" {
				expanded := 0
				for _, candidate := range fi.fuzzyTerms[term[0]] {
					if candidate == term || abs(len(candidate)-len(term)) > 1 || levenshtein(candidate, term, 1) > 1 {
						continue
					}
					if did := fi.termOne[candidate]; did != 0 {
						idf := math.Log(1 + (n-1+0.5)/(1+0.5))
						add(did, .8*idf*ix.bm25TFNormLocked(fi, candidate, did, avgdl))
					}
					if posting := fi.terms[candidate]; posting != nil {
						df := float64(posting.Count())
						idf := math.Log(1 + (n-df+0.5)/(df+0.5))
						posting.Each(func(id DocID) bool { add(id, .8*idf*ix.bm25TFNormLocked(fi, candidate, id, avgdl)); return true })
					}
					expanded++
					if expanded >= 128 {
						break
					}
				}
			}
		}
	}
}

func (ix *Index) bm25TFNormLocked(fi *fieldIndex, term string, id DocID, avgdl float64) float64 {
	tf, dl := 1.0, 1.0
	if byDoc := fi.extraTF[term]; byDoc != nil && byDoc[id] > 1 {
		tf = float64(byDoc[id])
	} else if byDoc := fi.positions[term]; byDoc != nil {
		if positions := byDoc[id]; len(positions) > 0 {
			tf = float64(len(positions))
		}
	}
	if int(id) < len(fi.docLengths) && fi.docLengths[id] > 0 {
		dl = float64(fi.docLengths[id])
	}
	const k1, b = 1.2, 0.75
	return tf * (k1 + 1) / (tf + k1*(1-b+b*dl/avgdl))
}

func (ix *Index) recordTextStatsLocked(fi *fieldIndex, id DocID, tokens []string) {
	if int(id) >= len(fi.docLengths) {
		if int(id) == len(fi.docLengths) {
			fi.docLengths = append(fi.docLengths, 0)
		} else {
			fi.docLengths = append(fi.docLengths, make([]uint32, int(id)-len(fi.docLengths)+1)...)
		}
	}
	fi.docLengths[id] += uint32(len(tokens))
	fi.totalTokens += uint64(len(tokens))
	for i, term := range tokens {
		count := uint16(1)
		for j := 0; j < i; j++ {
			if tokens[j] == term {
				count++
			}
		}
		if count <= 1 {
			continue
		}
		byDoc := fi.extraTF[term]
		if byDoc == nil {
			byDoc = map[DocID]uint16{}
			fi.extraTF[term] = byDoc
		}
		byDoc[id] = count
	}
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
			av, aok := stringColumnValue(ix.strings[sf.Field], a)
			bv, bok := stringColumnValue(ix.strings[sf.Field], b)
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
			if v, ok := stringColumnValue(ix.strings[field], id); ok {
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
	for name, v := range ix.vectors {
		liveVectors := 0
		for did := range v {
			if !ix.isDeletedOrExpiredLocked(did) {
				liveVectors++
			}
		}
		s.Vectors += liveVectors
		if ann := ix.anns[name]; ann != nil {
			n := ann.NodeCount()
			s.VectorNodes += n
			if n > liveVectors {
				s.VectorTombstones += n - liveVectors
			}
		}
	}
	seen := make(map[*fieldIndex]struct{}, len(ix.fields))
	for _, fi := range ix.fields {
		if _, ok := seen[fi]; ok {
			continue
		}
		seen[fi] = struct{}{}
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
		// The target field owns indexing for an explicit identical-text alias.
		// Indexing both names would double term frequency and document length.
		if sf.opt.IndexAlias != "" {
			continue
		}
		field, opt, fi := sf.name, sf.opt, sf.fi
		v, ok := doc[field]
		if !ok || v == nil {
			continue
		}
		fi.exists.AddUnsafe(id)

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
				if sf.stringsCol != nil {
					ix.setStringColumnLocked(sf.name, id, val)
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
			if opt.StoredOnly {
				continue
			}
			if opt.Lookup || opt.Kind == FieldKeyword || opt.Kind == FieldBool || opt.Kind == FieldInt || opt.Kind == FieldFloat || opt.Kind == FieldTime {
				if opt.Unique {
					fi.unique[val] = id
				} else if opt.Kind != FieldInt && opt.Kind != FieldFloat && opt.Kind != FieldTime {
					addTerm(fi, val, id, opt.Fuzzy)
				}
			}
			if opt.Kind == FieldText || opt.Indexed {
				if sf.analyzeFn != nil && atomic.LoadInt32(&noSynonyms) != 0 {
					ix.tokens = sf.analyzeFn(val, ix.tokens)
				} else {
					ix.tokens = analyzeWith(opt, val, ix.tokens)
				}
				for pos, t := range ix.tokens {
					addTerm(fi, t, id, opt.Fuzzy)
					addPos(fi, t, id, uint32(pos))
					if !sf.noDerived {
						ix.indexDerivedLocked(fi, opt, t, id)
					}
				}
				ix.recordTextStatsLocked(fi, id, ix.tokens)
				if opt.Phrase {
					addPhrases(fi, ix.tokens, id)
				}
			} else if !sf.noDerived {
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
		ix.scratch = prefixesBounded(val, opt.MinPrefix, opt.MaxPrefix, ix.scratch)
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
	if len(fi.patternCache) > 0 {
		fi.patternCacheMu.Lock()
		fi.patternCache = nil
		fi.patternCacheOrder = nil
		fi.patternCacheBytes = 0
		fi.patternCacheMu.Unlock()
	}
	if fi.terms[term] == nil && fi.termOne[term] == 0 {
		fi.termKeysDirty = true
	}
	if fuzzy && fi.termOne[term] == 0 && fi.terms[term] == nil {
		fi.fuzzyTerms[term[0]] = append(fi.fuzzyTerms[term[0]], term)
	}
	addPosting(fi.terms, fi.termOne, term, id, fi.capHint)
}

func addPosting(m map[string]*Bitmap, one map[string]DocID, term string, id DocID, _ DocID) {
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
	return ix.prefixBitmapWithin(field, value, nil)
}

// prefixBitmapWithin intersects a prefix result with an optional selective
// bitmap, using cached full-prefix postings for text fields when available.
func (ix *Index) prefixBitmapWithin(field, value string, candidates *Bitmap) *Bitmap {
	ix.mu.RLock()
	opt := ix.cfg.Schema.Fields[field]
	fi := ix.fields[field]
	if fi == nil {
		ix.mu.RUnlock()
		return NewBitmap()
	}
	val := normalize(value, opt.Lowercase)
	if did := fi.prefixOne[val]; did != 0 && !ix.deleted.Has(did) {
		r := NewBitmapCap(ix.nextDocID)
		if candidates == nil || candidates.Has(did) {
			r.Add(did)
		}
		ix.mu.RUnlock()
		return r
	}
	if b := fi.prefix[val]; b != nil {
		if candidates != nil {
			b = candidates.Clone().AndInPlace(b)
		}
		ix.mu.RUnlock()
		return b
	}
	if opt.Kind == FieldText && val != "" {
		ix.mu.RUnlock()
		matched := ix.textTermPatternBitmap(field, val, strings.HasPrefix, true)
		if candidates != nil {
			matched = candidates.Clone().AndInPlace(matched)
		}
		return matched
	}
	ix.mu.RUnlock()
	return NewBitmap()
}
func (ix *Index) suffixBitmap(field, value string) *Bitmap {
	ix.mu.RLock()
	opt := ix.cfg.Schema.Fields[field]
	fi := ix.fields[field]
	if fi == nil {
		ix.mu.RUnlock()
		return NewBitmap()
	}
	val := normalize(value, opt.Lowercase)
	if did := fi.suffixOne[val]; did != 0 && !ix.deleted.Has(did) {
		r := NewBitmapCap(ix.nextDocID)
		r.Add(did)
		ix.mu.RUnlock()
		return r
	}
	if b := fi.suffix[val]; b != nil {
		ix.mu.RUnlock()
		return b
	}
	if opt.Kind == FieldText && val != "" {
		ix.mu.RUnlock()
		return ix.textTermPatternBitmap(field, val, strings.HasSuffix, false)
	}
	ix.mu.RUnlock()
	return NewBitmap()
}
func (ix *Index) ngramBitmap(field, value string) *Bitmap {
	ix.mu.RLock()
	opt := ix.cfg.Schema.Fields[field]
	fi := ix.fields[field]
	if fi == nil {
		ix.mu.RUnlock()
		return NewBitmap()
	}
	value = normalize(value, opt.Lowercase)
	if opt.Kind == FieldText && !opt.Ngram && value != "" {
		ix.mu.RUnlock()
		return ix.textTermPatternBitmap(field, value, strings.Contains, false)
	}
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
			ix.mu.RUnlock()
			return NewBitmap()
		}
		if r == nil {
			r = b.Clone()
		} else {
			r = r.AndInPlace(b)
		}
	}
	if r == nil {
		ix.mu.RUnlock()
		return NewBitmap()
	}
	ix.mu.RUnlock()
	return r
}

// textTermPatternBitmap implements prefix, infix, and suffix patterns for text
// fields that do not materialize derived postings. The sorted token dictionary
// is built lazily once per generation; prefix patterns use a binary range, and
// infix/suffix patterns scan only the dictionary strings, never all documents.
func (ix *Index) textTermPatternBitmap(field, value string, match func(string, string) bool, prefixRange bool) *Bitmap {
	ix.mu.RLock()
	fi := ix.fields[field]
	if fi == nil {
		ix.mu.RUnlock()
		return NewBitmap()
	}
	cacheKey := ""
	if prefixRange {
		cacheKey = "prefix:" + value
		if cached := fi.cachedPattern(cacheKey); cached != nil {
			ix.mu.RUnlock()
			return cached
		}
		ix.mu.RUnlock()
		fi.patternBuildMu.Lock()
		defer fi.patternBuildMu.Unlock()
		ix.mu.RLock()
		fi = ix.fields[field]
		if fi == nil {
			ix.mu.RUnlock()
			return NewBitmap()
		}
		if cached := fi.cachedPattern(cacheKey); cached != nil {
			ix.mu.RUnlock()
			return cached
		}
	}
	if fi.termKeys == nil || fi.termKeysDirty {
		ix.mu.RUnlock()
		ix.mu.Lock()
		fi = ix.fields[field]
		if fi != nil && (fi.termKeys == nil || fi.termKeysDirty) {
			keys := make([]string, 0, len(fi.terms)+len(fi.termOne))
			for term := range fi.termOne {
				keys = append(keys, term)
			}
			for term := range fi.terms {
				keys = append(keys, term)
			}
			sort.Strings(keys)
			fi.termKeys = keys
			fi.termKeysDirty = false
		}
		ix.mu.Unlock()
		ix.mu.RLock()
		fi = ix.fields[field]
	}
	matched := NewBitmap()
	if fi != nil {
		start := 0
		if prefixRange {
			start = sort.SearchStrings(fi.termKeys, value)
		}
		for i := start; i < len(fi.termKeys); i++ {
			term := fi.termKeys[i]
			if prefixRange && !strings.HasPrefix(term, value) {
				break
			}
			if !match(term, value) {
				continue
			}
			if id := fi.termOne[term]; id != 0 {
				matched.Add(id)
			} else if postings := fi.terms[term]; postings != nil {
				matched = matched.OrInPlace(postings)
			}
		}
	}
	if cacheKey != "" {
		fi.storePattern(cacheKey, matched)
	}
	ix.mu.RUnlock()
	return matched
}

func (fi *fieldIndex) cachedPattern(key string) *Bitmap {
	fi.patternCacheMu.Lock()
	b := fi.patternCache[key]
	if b != nil {
		b = b.Clone()
	}
	fi.patternCacheMu.Unlock()
	return b
}

func (fi *fieldIndex) storePattern(key string, bitmap *Bitmap) {
	const maxCachedPatterns = 8
	const maxPatternCacheBytes = 1 << 20
	cacheSize := len(bitmap.words)*8 + len(bitmap.sparse)*4
	fi.patternCacheMu.Lock()
	if old := fi.patternCache[key]; old != nil {
		fi.patternCacheBytes -= len(old.words)*8 + len(old.sparse)*4
		delete(fi.patternCache, key)
		for i, existing := range fi.patternCacheOrder {
			if existing == key {
				fi.patternCacheOrder = append(fi.patternCacheOrder[:i], fi.patternCacheOrder[i+1:]...)
				break
			}
		}
	}
	if cacheSize > maxPatternCacheBytes {
		fi.patternCacheMu.Unlock()
		return
	}
	if fi.patternCache == nil {
		fi.patternCache = make(map[string]*Bitmap, maxCachedPatterns)
	}
	for len(fi.patternCacheOrder) >= maxCachedPatterns || fi.patternCacheBytes+cacheSize > maxPatternCacheBytes {
		oldest := fi.patternCacheOrder[0]
		oldBitmap := fi.patternCache[oldest]
		delete(fi.patternCache, oldest)
		if oldBitmap != nil {
			fi.patternCacheBytes -= len(oldBitmap.words)*8 + len(oldBitmap.sparse)*4
		}
		fi.patternCacheOrder = fi.patternCacheOrder[1:]
	}
	clone := bitmap.Clone()
	fi.patternCache[key] = clone
	fi.patternCacheOrder = append(fi.patternCacheOrder, key)
	fi.patternCacheBytes += cacheSize
	fi.patternCacheMu.Unlock()
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
func boundFloat(inclusive, exclusive any, _ bool) (float64, bool, bool) {
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
	case []float32:
		out := make([]float64, len(x))
		for i, n := range x {
			out[i] = float64(n)
		}
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
		for _, layout := range autoTimeLayouts {
			if t, err := time.Parse(layout, x); err == nil {
				return float64(t.Unix()), true
			}
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
	Seq uint64   `json:"seq,omitempty"`
	Op  string   `json:"op"`
	ID  string   `json:"id"`
	Doc Document `json:"doc,omitempty"`
	CRC uint32   `json:"crc,omitempty"`
}

func (ix *Index) appendWALLocked(r walRecord) error {
	if ix.wal == nil {
		return nil
	}
	ix.walSeq++
	r.Seq = ix.walSeq
	r.CRC = walCRC(r)
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	if _, err = ix.wal.Write(b); err != nil {
		return err
	}
	if ix.cfg.WALSyncEveryWrite {
		return ix.wal.Sync()
	}
	return nil
}

func walCRC(r walRecord) uint32 {
	r.CRC = 0
	b, _ := json.Marshal(r)
	return crc32.ChecksumIEEE(b)
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
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()
	rd := bufio.NewReader(f)
	lineNo := 0
	for {
		line, err := rd.ReadBytes('\n')
		if len(line) > 0 {
			lineNo++
			var r walRecord
			if e := json.Unmarshal(line, &r); e != nil {
				if ix.cfg.StrictRecovery {
					return fmt.Errorf("wal replay %s:%d: malformed record: %w", path, lineNo, e)
				}
			} else if r.CRC != 0 && walCRC(r) != r.CRC {
				if ix.cfg.StrictRecovery {
					return fmt.Errorf("wal replay %s:%d: checksum mismatch", path, lineNo)
				}
			} else {
				switch r.Op {
				case "upsert":
					if e := ix.upsert(r.ID, r.Doc, false); e != nil && ix.cfg.StrictRecovery {
						return fmt.Errorf("wal replay %s:%d: upsert %q: %w", path, lineNo, r.ID, e)
					}
				case "delete":
					if e := ix.delete(r.ID, false); e != nil && ix.cfg.StrictRecovery {
						return fmt.Errorf("wal replay %s:%d: delete %q: %w", path, lineNo, r.ID, e)
					}
				default:
					if ix.cfg.StrictRecovery {
						return fmt.Errorf("wal replay %s:%d: unknown op %q", path, lineNo, r.Op)
					}
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
	Version int                 `json:"version,omitempty"`
	Config  Config              `json:"config"`
	Docs    map[string]Document `json:"docs,omitempty"`
	Dump    *persistentDump     `json:"dump,omitempty"`
}

func (ix *Index) SaveSnapshot(path string) error {
	dump, err := ix.persistentDump()
	if err != nil {
		return err
	}
	ix.mu.RLock()
	snap := snapshot{Version: 2, Config: ix.cfg, Docs: map[string]Document{}, Dump: dump}
	for id, did := range ix.extToDoc {
		if !ix.isDeletedOrExpiredLocked(did) && int(did) < len(ix.docs) && ix.docs[did] != nil {
			snap.Docs[id] = cloneDoc(ix.docs[did])
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
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return syncDir(dir(path))
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
	if snap.Dump != nil {
		if err := ix.restorePersistentDump(snap.Dump); err != nil {
			return err
		}
		if len(snap.Docs) > 0 {
			ix.mu.Lock()
			if len(ix.docs) < len(ix.docToExt) {
				ix.docs = make([]Document, len(ix.docToExt))
			}
			for id, doc := range snap.Docs {
				if did, ok := ix.extToDoc[id]; ok && int(did) < len(ix.docs) {
					ix.docs[did] = cloneDoc(doc)
				}
			}
			ix.mu.Unlock()
		}
		return nil
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
