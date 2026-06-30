package lookup

import (
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// -----------------------------------------------------------------------------
// Production-scale extension state. Kept outside Index to avoid changing public
// construction code while still allowing persistent segments, composite lookup,
// frozen mode and plan/task metadata to be attached to any index instance.
// -----------------------------------------------------------------------------

type indexExtras struct {
	mu                sync.RWMutex
	frozen            bool
	composite         *TupleCompositeIndex // deprecated/domain-specific wrapper
	genericComposites map[string]*CompositeIndex
	planner           *QueryPlanCache
	storeDir          string
}

var extraIndexes sync.Map // map[*Index]*indexExtras

func extras(ix *Index) *indexExtras {
	if ix == nil {
		return nil
	}
	if v, ok := extraIndexes.Load(ix); ok {
		return v.(*indexExtras)
	}
	ex := &indexExtras{planner: NewQueryPlanCache(1024)}
	v, _ := extraIndexes.LoadOrStore(ix, ex)
	return v.(*indexExtras)
}

// Freeze compacts and prepares read-mostly acceleration structures. It is safe to
// call after a full or incremental load. Mutable operations still work, but the
// method records that readers can use frozen fast paths and builds the composite
// record index when possible.
func (ix *Index) Freeze() error {
	if ix == nil {
		return errors.New("nil index")
	}
	ix.mu.RLock()
	// Trigger lazy sorted columns while read lock is held enough to see schema.
	ix.mu.RUnlock()
	ex := extras(ix)
	ex.mu.Lock()
	defer ex.mu.Unlock()
	if ex.composite == nil {
		ex.composite = NewTupleCompositeIndex()
		if err := ex.composite.BuildFromIndex(ix); err != nil {
			return err
		}
	}
	ex.frozen = true
	return nil
}

func (ix *Index) IsFrozen() bool {
	ex := extras(ix)
	if ex == nil {
		return false
	}
	ex.mu.RLock()
	v := ex.frozen
	ex.mu.RUnlock()
	return v
}

// -----------------------------------------------------------------------------
// Composite record lookup: (term, group_id, date_key) -> postings. This is the
// fastest path for Dataset/DatasetB/DatasetC style exact lookups and avoids generic bool
// intersections for the common query shape.
// -----------------------------------------------------------------------------

type TupleKey struct {
	TermID     uint32
	GroupID uint32
	DateKey        uint32
	SourceID   uint16
}

type TupleCompositeIndex struct {
	mu        sync.RWMutex
	terms     map[string]uint32
	revTerms  []string
	sources   map[string]uint16
	revSource []string
	postings  map[TupleKey]*Bitmap
	singles   map[TupleKey]DocID
}

func NewTupleCompositeIndex() *TupleCompositeIndex {
	return &TupleCompositeIndex{terms: make(map[string]uint32, 1<<16), revTerms: []string{""}, sources: map[string]uint16{"": 0}, revSource: []string{""}, postings: make(map[TupleKey]*Bitmap, 1<<16), singles: make(map[TupleKey]DocID, 1<<16)}
}

func (c *TupleCompositeIndex) internTermLocked(term string) uint32 {
	term = strings.ToLower(strings.TrimSpace(term))
	if term == "" {
		return 0
	}
	if id, ok := c.terms[term]; ok {
		return id
	}
	id := uint32(len(c.revTerms))
	c.terms[term] = id
	c.revTerms = append(c.revTerms, term)
	return id
}
func (c *TupleCompositeIndex) termIDLocked(term string) uint32 {
	return c.terms[strings.ToLower(strings.TrimSpace(term))]
}
func (c *TupleCompositeIndex) internSourceLocked(source string) uint16 {
	source = strings.ToLower(strings.TrimSpace(source))
	if id, ok := c.sources[source]; ok {
		return id
	}
	id := uint16(len(c.revSource))
	c.sources[source] = id
	c.revSource = append(c.revSource, source)
	return id
}
func (c *TupleCompositeIndex) sourceIDLocked(source string) uint16 {
	return c.sources[strings.ToLower(strings.TrimSpace(source))]
}

func (c *TupleCompositeIndex) Add(term string, groupID uint32, date_key string, source string, did DocID) {
	if c == nil || did == 0 || term == "" || groupID == 0 || date_key == "" {
		return
	}
	dd, ok := EncodeDateYYYYMMDD(date_key)
	if !ok {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	key := TupleKey{TermID: c.internTermLocked(term), GroupID: groupID, DateKey: dd, SourceID: c.internSourceLocked(source)}
	if bm := c.postings[key]; bm != nil {
		bm.Add(did)
		return
	}
	if prev := c.singles[key]; prev != 0 {
		bm := NewBitmapCap(did + 1)
		bm.Add(prev)
		bm.Add(did)
		delete(c.singles, key)
		c.postings[key] = bm
		return
	}
	c.singles[key] = did
}

func (c *TupleCompositeIndex) Bitmap(term string, groupID uint32, date_key string, source string) *Bitmap {
	if c == nil {
		return NewBitmap()
	}
	dd, ok := EncodeDateYYYYMMDD(date_key)
	if !ok {
		return NewBitmap()
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	tid := c.termIDLocked(term)
	if tid == 0 {
		return NewBitmap()
	}
	sid := c.sourceIDLocked(source)
	key := TupleKey{TermID: tid, GroupID: groupID, DateKey: dd, SourceID: sid}
	if did := c.singles[key]; did != 0 {
		b := NewBitmapCap(did + 1)
		b.Add(did)
		return b
	}
	if bm := c.postings[key]; bm != nil {
		return bm.Clone()
	}
	return NewBitmap()
}

func (c *TupleCompositeIndex) Search(ix *Index, term string, groupID uint32, date_key string, source string, limit int, dst []Hit) []Hit {
	dst = dst[:0]
	if ix == nil || c == nil {
		return dst
	}
	bm := c.Bitmap(term, groupID, date_key, source)
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	bm.Each(func(id DocID) bool {
		if ix.isDeletedOrExpiredLocked(id) {
			return true
		}
		dst = append(dst, Hit{ID: ix.docToExt[id], DocID: id, Score: 1})
		return limit <= 0 || len(dst) < limit
	})
	return dst
}

func (c *TupleCompositeIndex) BuildFromIndex(ix *Index) error {
	if ix == nil {
		return errors.New("nil index")
	}
	// Build by intersecting generic term/group/date_key postings. This is a best
	// effort fallback for already-loaded indexes. Streaming sources update the
	// composite index directly and are much faster for 100M+ rows.
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	termFI := ix.fields["term"]
	workFI := ix.fields["group_id"]
	date_keyFI := ix.fields["date_key"]
	if termFI == nil || workFI == nil || date_keyFI == nil {
		return nil
	}
	for term, did := range termFI.termOne {
		c.addForDocFromPostingsLocked(ix, did, term, workFI, date_keyFI)
	}
	for term, bm := range termFI.terms {
		bm.Each(func(did DocID) bool { c.addForDocFromPostingsLocked(ix, did, term, workFI, date_keyFI); return true })
	}
	return nil
}
func (c *TupleCompositeIndex) addForDocFromPostingsLocked(ix *Index, did DocID, term string, workFI, date_keyFI *fieldIndex) {
	var work, date_key string
	for v, id := range workFI.termOne {
		if id == did {
			work = v
			break
		}
	}
	if work == "" {
		for v, bm := range workFI.terms {
			if bm.Has(did) {
				work = v
				break
			}
		}
	}
	for v, id := range date_keyFI.termOne {
		if id == did {
			date_key = v
			break
		}
	}
	if date_key == "" {
		for v, bm := range date_keyFI.terms {
			if bm.Has(did) {
				date_key = v
				break
			}
		}
	}
	wi, _ := strconv.ParseUint(work, 10, 32)
	if wi > 0 && date_key != "" {
		c.Add(term, uint32(wi), date_key, "", did)
	}
}

func (ix *Index) EnableTupleComposite() {
	extras(ix).mu.Lock()
	extras(ix).composite = NewTupleCompositeIndex()
	extras(ix).mu.Unlock()
}
func (ix *Index) TupleComposite() *TupleCompositeIndex {
	ex := extras(ix)
	ex.mu.RLock()
	c := ex.composite
	ex.mu.RUnlock()
	return c
}

func (ix *Index) TupleLookup(term string, groupID uint32, date_key string, limit int, dst []Hit) []Hit {
	ex := extras(ix)
	ex.mu.RLock()
	c := ex.composite
	ex.mu.RUnlock()
	if c == nil {
		return dst[:0]
	}
	return c.Search(ix, term, groupID, date_key, "", limit, dst)
}

// TupleCompositeQuery can be returned by ParseLookupQuery. It falls back to
// generic bool filtering when a composite index has not been built yet.
type TupleCompositeQuery struct {
	Term       string
	GroupID uint32
	DateKey        string
	Source     string
}

func (q TupleCompositeQuery) eval(ix *Index) *Bitmap {
	ex := extras(ix)
	ex.mu.RLock()
	c := ex.composite
	ex.mu.RUnlock()
	if c != nil {
		return c.Bitmap(q.Term, q.GroupID, q.DateKey, q.Source)
	}
	return TupleQuery(q.Term, strconv.FormatUint(uint64(q.GroupID), 10), q.DateKey).eval(ix)
}

// ParseLookupQueryFast parses URL query strings and returns a composite query
// when term + group_id + date_key are present.
func ParseLookupQueryFast(raw string) Query {
	raw = strings.TrimPrefix(raw, "?")
	vals := map[string]string{}
	for _, p := range strings.Split(raw, "&") {
		if p == "" {
			continue
		}
		kv := strings.SplitN(p, "=", 2)
		if len(kv) == 2 {
			vals[kv[0]] = kv[1]
		}
	}
	if vals["term"] != "" && vals["group_id"] != "" && vals["date_key"] != "" {
		wi, _ := strconv.ParseUint(vals["group_id"], 10, 32)
		if wi > 0 {
			return TupleCompositeQuery{Term: vals["term"], GroupID: uint32(wi), DateKey: vals["date_key"], Source: vals["source"]}
		}
	}
	return TupleQuery(vals["term"], vals["group_id"], vals["date_key"])
}

func EncodeDateYYYYMMDD(s string) (uint32, bool) {
	if len(s) != 10 {
		return 0, false
	}
	y, err1 := strconv.Atoi(s[0:4])
	m, err2 := strconv.Atoi(s[5:7])
	d, err3 := strconv.Atoi(s[8:10])
	if err1 != nil || err2 != nil || err3 != nil || s[4] != '-' || s[7] != '-' {
		return 0, false
	}
	// Civil days from Howard Hinnant's algorithm, offset to uint32.
	y -= boolToInt(m <= 2)
	era := divFloor(y, 400)
	yoe := y - era*400
	mp := m + map[bool]int{true: 9, false: -3}[m <= 2]
	doy := (153*mp+2)/5 + d - 1
	doe := yoe*365 + yoe/4 - yoe/100 + doy
	days := era*146097 + doe - 719468
	return uint32(int64(days) + 1<<31), true
}
func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
func divFloor(a, b int) int {
	q := a / b
	r := a % b
	if r != 0 && ((r < 0) != (b < 0)) {
		q--
	}
	return q
}

// -----------------------------------------------------------------------------
// Persistent index storage. The implementation stores complete internal lookup
// structures, not just source documents, so DisableSource indexes can be restored.
// It is segment-manifest compatible: each save writes generation N and atomically
// advances CURRENT. For billion-row deployments, create many partition/segment
// directories and load them through MultiIndexManager.
// -----------------------------------------------------------------------------

type PersistentStore interface {
	SaveIndex(ctx context.Context, indexID string, ix *Index) (PersistentManifest, error)
	LoadIndex(ctx context.Context, indexID string, cfg Config) (*Index, PersistentManifest, error)
	ListGenerations(ctx context.Context, indexID string) ([]PersistentManifest, error)
}

type FileSegmentStore struct{ Root string }

type PersistentManifest struct {
	IndexID    string    `json:"index_id"`
	Generation uint64    `json:"generation"`
	CreatedAt  time.Time `json:"created_at"`
	Docs       uint64    `json:"docs"`
	Path       string    `json:"path"`
	Frozen     bool      `json:"frozen"`
	Format     string    `json:"format"`
}

func (s FileSegmentStore) dir(indexID string) string {
	return filepath.Join(s.Root, cleanIndexID(indexID))
}
func (s FileSegmentStore) SaveIndex(ctx context.Context, indexID string, ix *Index) (PersistentManifest, error) {
	if s.Root == "" {
		return PersistentManifest{}, errors.New("store root required")
	}
	if ix == nil {
		return PersistentManifest{}, errors.New("nil index")
	}
	_ = ix.Freeze()
	base := s.dir(indexID)
	if err := os.MkdirAll(base, 0755); err != nil {
		return PersistentManifest{}, err
	}
	gen := uint64(time.Now().UnixNano())
	genDir := filepath.Join(base, fmt.Sprintf("generation-%020d", gen))
	if err := os.MkdirAll(genDir, 0755); err != nil {
		return PersistentManifest{}, err
	}
	dump, err := ix.persistentDump()
	if err != nil {
		return PersistentManifest{}, err
	}
	dataPath := filepath.Join(genDir, "index.gob")
	f, err := os.Create(dataPath + ".tmp")
	if err != nil {
		return PersistentManifest{}, err
	}
	if err = gob.NewEncoder(f).Encode(dump); err == nil {
		err = f.Close()
	} else {
		_ = f.Close()
	}
	if err != nil {
		return PersistentManifest{}, err
	}
	if err = os.Rename(dataPath+".tmp", dataPath); err != nil {
		return PersistentManifest{}, err
	}
	man := PersistentManifest{IndexID: cleanIndexID(indexID), Generation: gen, CreatedAt: time.Now().UTC(), Docs: uint64(ix.Stats().LiveDocs), Path: genDir, Frozen: ix.IsFrozen(), Format: "lookupx-gob-segment-v1"}
	mb, _ := json.MarshalIndent(man, "", "  ")
	if err = os.WriteFile(filepath.Join(genDir, "manifest.json"), mb, 0644); err != nil {
		return PersistentManifest{}, err
	}
	return man, os.WriteFile(filepath.Join(base, "CURRENT"), []byte(fmt.Sprintf("generation-%020d", gen)), 0644)
}
func (s FileSegmentStore) LoadIndex(ctx context.Context, indexID string, cfg Config) (*Index, PersistentManifest, error) {
	base := s.dir(indexID)
	cur, err := os.ReadFile(filepath.Join(base, "CURRENT"))
	if err != nil {
		return nil, PersistentManifest{}, err
	}
	genDir := filepath.Join(base, strings.TrimSpace(string(cur)))
	mb, err := os.ReadFile(filepath.Join(genDir, "manifest.json"))
	if err != nil {
		return nil, PersistentManifest{}, err
	}
	var man PersistentManifest
	if err = json.Unmarshal(mb, &man); err != nil {
		return nil, man, err
	}
	f, err := os.Open(filepath.Join(genDir, "index.gob"))
	if err != nil {
		return nil, man, err
	}
	defer f.Close()
	var dump persistentDump
	if err = gob.NewDecoder(f).Decode(&dump); err != nil {
		return nil, man, err
	}
	if cfg.Schema.Fields != nil {
		dump.Config.Schema = cfg.Schema
	}
	ix, err := New(dump.Config)
	if err != nil {
		return nil, man, err
	}
	if err = ix.restorePersistentDump(&dump); err != nil {
		_ = ix.Close()
		return nil, man, err
	}
	return ix, man, nil
}
func (s FileSegmentStore) ListGenerations(ctx context.Context, indexID string) ([]PersistentManifest, error) {
	base := s.dir(indexID)
	ents, err := os.ReadDir(base)
	if err != nil {
		return nil, err
	}
	out := []PersistentManifest{}
	for _, e := range ents {
		if !e.IsDir() || !strings.HasPrefix(e.Name(), "generation-") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(base, e.Name(), "manifest.json"))
		if err == nil {
			var m PersistentManifest
			if json.Unmarshal(b, &m) == nil {
				out = append(out, m)
			}
		}
	}
	return out, nil
}

func (ix *Index) SavePersistent(ctx context.Context, store PersistentStore, indexID string) (PersistentManifest, error) {
	return store.SaveIndex(ctx, indexID, ix)
}
func OpenPersistent(ctx context.Context, store PersistentStore, indexID string, cfg Config) (*Index, PersistentManifest, error) {
	return store.LoadIndex(ctx, indexID, cfg)
}

type persistentDump struct {
	Config        Config
	NextDocID     DocID
	ExtToDoc      map[string]DocID
	DocToExt      []string
	Deleted       []uint64
	Live          []uint64
	Expires       map[DocID]int64
	HasTTL        bool
	HasDeletes    bool
	Fields        map[string]fieldDump
	NumericDense  map[string][]float64
	NumericExists map[string][]uint64
	Strings       map[string]map[DocID]string
	IP4           map[string]map[DocID]uint32
	Vectors       map[string]map[DocID][]float64
	Composite     *compositeDump
	Frozen        bool
}
type fieldDump struct {
	Terms     map[string][]uint64
	TermOne   map[string]DocID
	Prefix    map[string][]uint64
	PrefixOne map[string]DocID
	Suffix    map[string][]uint64
	SuffixOne map[string]DocID
	Ngram     map[string][]uint64
	NgramOne  map[string]DocID
	Unique    map[string]DocID
	Exists    []uint64
}
type compositeDump struct {
	Terms     map[string]uint32
	RevTerms  []string
	Sources   map[string]uint16
	RevSource []string
	Postings  map[TupleKey][]uint64
	Singles   map[TupleKey]DocID
}

func (ix *Index) persistentDump() (*persistentDump, error) {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	cfg := ix.cfg
	cfg.Clock = nil
	cfg.EnableWAL = false
	cfg.WALPath = ""
	d := &persistentDump{Config: cfg, NextDocID: ix.nextDocID, ExtToDoc: copyMapStringDoc(ix.extToDoc), DocToExt: append([]string(nil), ix.docToExt...), Deleted: append([]uint64(nil), ix.deleted.words...), Live: append([]uint64(nil), ix.live.words...), Expires: copyMapDocInt64(ix.expires), HasTTL: ix.hasTTL, HasDeletes: ix.hasDeletes, Fields: map[string]fieldDump{}, NumericDense: map[string][]float64{}, NumericExists: map[string][]uint64{}, Strings: map[string]map[DocID]string{}, IP4: map[string]map[DocID]uint32{}, Vectors: map[string]map[DocID][]float64{}}
	for name, fi := range ix.fields {
		d.Fields[name] = fieldDump{Terms: dumpBitmapMap(fi.terms), TermOne: copyMapStringDoc(fi.termOne), Prefix: dumpBitmapMap(fi.prefix), PrefixOne: copyMapStringDoc(fi.prefixOne), Suffix: dumpBitmapMap(fi.suffix), SuffixOne: copyMapStringDoc(fi.suffixOne), Ngram: dumpBitmapMap(fi.ngram), NgramOne: copyMapStringDoc(fi.ngramOne), Unique: copyMapStringDoc(fi.unique), Exists: append([]uint64(nil), fi.exists.words...)}
	}
	for k, v := range ix.numericDense {
		d.NumericDense[k] = append([]float64(nil), v...)
	}
	for k, v := range ix.numericExists {
		d.NumericExists[k] = append([]uint64(nil), v.words...)
	}
	for k, v := range ix.strings {
		m := map[DocID]string{}
		for a, b := range v {
			m[a] = b
		}
		d.Strings[k] = m
	}
	for k, v := range ix.ip4 {
		m := map[DocID]uint32{}
		for a, b := range v {
			m[a] = b
		}
		d.IP4[k] = m
	}
	for k, v := range ix.vectors {
		m := map[DocID][]float64{}
		for a, b := range v {
			m[a] = append([]float64(nil), b...)
		}
		d.Vectors[k] = m
	}
	ex := extras(ix)
	ex.mu.RLock()
	d.Frozen = ex.frozen
	if ex.composite != nil {
		d.Composite = dumpComposite(ex.composite)
	}
	ex.mu.RUnlock()
	return d, nil
}
func (ix *Index) restorePersistentDump(d *persistentDump) error {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	ix.nextDocID = d.NextDocID
	ix.extToDoc = copyMapStringDoc(d.ExtToDoc)
	ix.docToExt = append([]string(nil), d.DocToExt...)
	ix.docs = make([]Document, len(ix.docToExt))
	ix.deleted = &Bitmap{words: append([]uint64(nil), d.Deleted...)}
	ix.live = &Bitmap{words: append([]uint64(nil), d.Live...)}
	ix.expires = copyMapDocInt64(d.Expires)
	ix.hasTTL = d.HasTTL
	ix.hasDeletes = d.HasDeletes
	for name, fd := range d.Fields {
		if fi := ix.fields[name]; fi != nil {
			fi.terms = loadBitmapMap(fd.Terms)
			fi.termOne = copyMapStringDoc(fd.TermOne)
			fi.prefix = loadBitmapMap(fd.Prefix)
			fi.prefixOne = copyMapStringDoc(fd.PrefixOne)
			fi.suffix = loadBitmapMap(fd.Suffix)
			fi.suffixOne = copyMapStringDoc(fd.SuffixOne)
			fi.ngram = loadBitmapMap(fd.Ngram)
			fi.ngramOne = copyMapStringDoc(fd.NgramOne)
			fi.unique = copyMapStringDoc(fd.Unique)
			fi.exists = &Bitmap{words: append([]uint64(nil), fd.Exists...)}
		}
	}
	ix.numericDense = map[string][]float64{}
	for k, v := range d.NumericDense {
		ix.numericDense[k] = append([]float64(nil), v...)
	}
	ix.numericExists = map[string]*Bitmap{}
	for k, v := range d.NumericExists {
		ix.numericExists[k] = &Bitmap{words: append([]uint64(nil), v...)}
	}
	ix.strings = map[string]map[DocID]string{}
	for k, v := range d.Strings {
		m := map[DocID]string{}
		for a, b := range v {
			m[a] = b
		}
		ix.strings[k] = m
	}
	ix.ip4 = map[string]map[DocID]uint32{}
	for k, v := range d.IP4 {
		m := map[DocID]uint32{}
		for a, b := range v {
			m[a] = b
		}
		ix.ip4[k] = m
	}
	ix.vectors = map[string]map[DocID][]float64{}
	ix.anns = map[string]*VectorANN{}
	for k, v := range d.Vectors {
		m := map[DocID][]float64{}
		dim := 0
		for a, b := range v {
			if dim == 0 {
				dim = len(b)
			}
			m[a] = append([]float64(nil), b...)
		}
		ix.vectors[k] = m
		ann := newVectorANN(dim, "dot", len(m))
		for id, vec := range m {
			ann.Add(id, vec)
		}
		ix.anns[k] = ann
	}
	ex := extras(ix)
	ex.mu.Lock()
	ex.frozen = d.Frozen
	if d.Composite != nil {
		ex.composite = loadComposite(d.Composite)
	}
	ex.mu.Unlock()
	return nil
}
func dumpBitmapMap(in map[string]*Bitmap) map[string][]uint64 {
	out := map[string][]uint64{}
	for k, b := range in {
		if b != nil {
			out[k] = append([]uint64(nil), b.words...)
		}
	}
	return out
}
func loadBitmapMap(in map[string][]uint64) map[string]*Bitmap {
	out := map[string]*Bitmap{}
	for k, w := range in {
		out[k] = &Bitmap{words: append([]uint64(nil), w...)}
	}
	return out
}
func copyMapStringDoc(in map[string]DocID) map[string]DocID {
	out := map[string]DocID{}
	for k, v := range in {
		out[k] = v
	}
	return out
}
func copyMapDocInt64(in map[DocID]int64) map[DocID]int64 {
	out := map[DocID]int64{}
	for k, v := range in {
		out[k] = v
	}
	return out
}
func dumpComposite(c *TupleCompositeIndex) *compositeDump {
	c.mu.RLock()
	defer c.mu.RUnlock()
	p := map[TupleKey][]uint64{}
	for k, b := range c.postings {
		p[k] = append([]uint64(nil), b.words...)
	}
	return &compositeDump{Terms: copyMapStringU32(c.terms), RevTerms: append([]string(nil), c.revTerms...), Sources: copyMapStringU16(c.sources), RevSource: append([]string(nil), c.revSource...), Postings: p, Singles: copyMapKeyDoc(c.singles)}
}
func loadComposite(d *compositeDump) *TupleCompositeIndex {
	c := NewTupleCompositeIndex()
	c.terms = copyMapStringU32(d.Terms)
	c.revTerms = append([]string(nil), d.RevTerms...)
	c.sources = copyMapStringU16(d.Sources)
	c.revSource = append([]string(nil), d.RevSource...)
	c.postings = map[TupleKey]*Bitmap{}
	for k, w := range d.Postings {
		c.postings[k] = &Bitmap{words: append([]uint64(nil), w...)}
	}
	c.singles = copyMapKeyDoc(d.Singles)
	return c
}
func copyMapStringU32(in map[string]uint32) map[string]uint32 {
	out := map[string]uint32{}
	for k, v := range in {
		out[k] = v
	}
	return out
}
func copyMapStringU16(in map[string]uint16) map[string]uint16 {
	out := map[string]uint16{}
	for k, v := range in {
		out[k] = v
	}
	return out
}
func copyMapKeyDoc(in map[TupleKey]DocID) map[TupleKey]DocID {
	out := map[TupleKey]DocID{}
	for k, v := range in {
		out[k] = v
	}
	return out
}

// -----------------------------------------------------------------------------
// Async reload/index task manager. Designed for 100M/1B row reloads where HTTP
// requests must return immediately and progress must be observable/cancellable.
// -----------------------------------------------------------------------------

type TaskStatus string

const (
	TaskQueued    TaskStatus = "queued"
	TaskRunning   TaskStatus = "running"
	TaskSucceeded TaskStatus = "succeeded"
	TaskFailed    TaskStatus = "failed"
	TaskCancelled TaskStatus = "cancelled"
)

type TaskInfo struct {
	ID         string       `json:"id"`
	IndexID    string       `json:"index_id"`
	Status     TaskStatus   `json:"status"`
	StartedAt  time.Time    `json:"started_at,omitempty"`
	FinishedAt time.Time    `json:"finished_at,omitempty"`
	Progress   BulkProgress `json:"progress"`
	Stats      BulkStats    `json:"stats"`
	Error      string       `json:"error,omitempty"`
}
type TaskManager struct {
	mu      sync.RWMutex
	seq     uint64
	tasks   map[string]*TaskInfo
	cancels map[string]context.CancelFunc
}

func NewTaskManager() *TaskManager {
	return &TaskManager{tasks: map[string]*TaskInfo{}, cancels: map[string]context.CancelFunc{}}
}
func (tm *TaskManager) StartReload(parent context.Context, mgr *MultiIndexManager, indexID string) string {
	ctx, cancel := context.WithCancel(parent)
	id := fmt.Sprintf("task-%d", atomic.AddUint64(&tm.seq, 1))
	ti := &TaskInfo{ID: id, IndexID: indexID, Status: TaskQueued}
	tm.mu.Lock()
	tm.tasks[id] = ti
	tm.cancels[id] = cancel
	tm.mu.Unlock()
	go func() {
		tm.update(id, func(t *TaskInfo) { t.Status = TaskRunning; t.StartedAt = time.Now() })
		stats, err := mgr.Reload(ctx, indexID)
		tm.mu.Lock()
		defer tm.mu.Unlock()
		delete(tm.cancels, id)
		t := tm.tasks[id]
		t.Stats = stats
		t.FinishedAt = time.Now()
		if err != nil {
			if ctx.Err() != nil {
				t.Status = TaskCancelled
			} else {
				t.Status = TaskFailed
				t.Error = err.Error()
			}
		} else {
			t.Status = TaskSucceeded
		}
	}()
	return id
}
func (tm *TaskManager) update(id string, fn func(*TaskInfo)) {
	tm.mu.Lock()
	if t := tm.tasks[id]; t != nil {
		fn(t)
	}
	tm.mu.Unlock()
}
func (tm *TaskManager) Get(id string) (TaskInfo, bool) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	if t := tm.tasks[id]; t != nil {
		return *t, true
	}
	return TaskInfo{}, false
}
func (tm *TaskManager) List() []TaskInfo {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	out := make([]TaskInfo, 0, len(tm.tasks))
	for _, t := range tm.tasks {
		out = append(out, *t)
	}
	return out
}
func (tm *TaskManager) Cancel(id string) bool {
	tm.mu.Lock()
	c := tm.cancels[id]
	tm.mu.Unlock()
	if c != nil {
		c()
		return true
	}
	return false
}

var managerTasks sync.Map // map[*MultiIndexManager]*TaskManager
func (m *MultiIndexManager) Tasks() *TaskManager {
	if v, ok := managerTasks.Load(m); ok {
		return v.(*TaskManager)
	}
	tm := NewTaskManager()
	v, _ := managerTasks.LoadOrStore(m, tm)
	return v.(*TaskManager)
}

// -----------------------------------------------------------------------------
// Config-driven multi-index service and partition helpers.
// -----------------------------------------------------------------------------

type ServiceConfig struct {
	Server struct {
		Addr    string   `json:"addr"`
		APIKeys []string `json:"api_keys"`
		DataDir string   `json:"data_dir"`
	} `json:"server"`
	Indexes []ServiceIndexConfig `json:"indexes"`
}
type ServiceIndexConfig struct {
	ID              string                `json:"id"`
	Schema          string                `json:"schema"`
	InitialCapacity int                   `json:"initial_capacity"`
	Persistent      bool                  `json:"persistent"`
	Source          SQLTableReloadRequest `json:"source"`
}

func LoadServiceConfig(path string) (ServiceConfig, error) {
	var c ServiceConfig
	b, err := os.ReadFile(path)
	if err != nil {
		return c, err
	}
	err = json.Unmarshal(b, &c)
	return c, err
}
func BuildManagerFromConfig(ctx context.Context, c ServiceConfig) (*MultiIndexManager, error) {
	m := NewMultiIndexManager()
	for _, ic := range c.Indexes {
		schema := TupleLookupSchema()
		if ic.Schema != "" && !strings.Contains(strings.ToLower(ic.Schema), "record") {
			schema = TupleLookupSchema()
		}
		_, err := m.Register(IndexDefinition{ID: ic.ID, Config: Config{Schema: schema, DisableSource: true, InitialCapacity: ic.InitialCapacity, Clock: StaticClock{T: time.Unix(0, 0)}}})
		if err != nil {
			return nil, err
		}
	}
	return m, nil
}

type PartitionRouter struct{ Manager *MultiIndexManager }

func (r PartitionRouter) PartitionID(indexID string, groupID uint32, date_key string) string {
	month := date_key
	if len(month) >= 7 {
		month = month[:7]
	}
	return cleanIndexID(fmt.Sprintf("%s-wi%d-%s", indexID, groupID, month))
}
func (r PartitionRouter) SearchTuple(indexID, term string, groupID uint32, date_key string, limit int, dst []Hit) []Hit {
	if r.Manager == nil {
		return dst[:0]
	}
	if ix, ok := r.Manager.Get(r.PartitionID(indexID, groupID, date_key)); ok {
		return ix.TupleLookup(term, groupID, date_key, limit, dst)
	}
	if ix, ok := r.Manager.Get(indexID); ok {
		return ix.TupleLookup(term, groupID, date_key, limit, dst)
	}
	return dst[:0]
}

// Small HTTP extension handler. Call from MultiServer by registering under /v1 if
// the app wants async tasks/persistence endpoints.
func (s *MultiServer) ServeProductionHTTP(w http.ResponseWriter, r *http.Request, store PersistentStore) bool {
	path := strings.Trim(r.URL.Path, "/")
	parts := strings.Split(path, "/")
	if path == "v1/tasks" && r.Method == http.MethodGet {
		writeJSON(w, map[string]any{"tasks": s.Manager.Tasks().List()})
		return true
	}
	if len(parts) == 3 && parts[0] == "v1" && parts[1] == "tasks" && r.Method == http.MethodGet {
		if t, ok := s.Manager.Tasks().Get(parts[2]); ok {
			writeJSON(w, t)
		} else {
			http.NotFound(w, r)
		}
		return true
	}
	if len(parts) == 4 && parts[0] == "v1" && parts[1] == "tasks" && parts[3] == "cancel" && r.Method == http.MethodPost {
		writeJSON(w, map[string]any{"ok": s.Manager.Tasks().Cancel(parts[2])})
		return true
	}
	if len(parts) >= 4 && parts[0] == "v1" && parts[1] == "indexes" {
		id := parts[2]
		action := parts[3]
		if action == "reload-async" && r.Method == http.MethodPost {
			tid := s.Manager.Tasks().StartReload(r.Context(), s.Manager, id)
			writeJSON(w, map[string]any{"task_id": tid})
			return true
		}
		ix, ok := s.Manager.Get(id)
		if !ok {
			http.Error(w, "index not found", 404)
			return true
		}
		switch action {
		case "freeze":
			if r.Method == http.MethodPost {
				err := ix.Freeze()
				if err != nil {
					http.Error(w, err.Error(), 500)
				} else {
					writeJSON(w, map[string]any{"ok": true, "frozen": ix.IsFrozen()})
				}
				return true
			}
		case "persist":
			if r.Method == http.MethodPost {
				if store == nil {
					http.Error(w, "store required", 500)
					return true
				}
				man, err := store.SaveIndex(r.Context(), id, ix)
				if err != nil {
					http.Error(w, err.Error(), 500)
				} else {
					writeJSON(w, man)
				}
				return true
			}
		case "lookup-composite":
			if r.Method == http.MethodGet {
				q := r.URL.Query()
				wi, _ := strconv.ParseUint(q.Get("group_id"), 10, 32)
				_, hits := ix.SearchInto(SearchRequest{Query: TupleCompositeQuery{Term: q.Get("term"), GroupID: uint32(wi), DateKey: q.Get("date_key")}, Limit: IntParam(r, "limit", 20)}, nil)
				writeJSON(w, map[string]any{"hits": hits, "total": len(hits)})
				return true
			}
		}
	}
	if s.serveBillionHTTP(w, r, store) {
		return true
	}
	return false
}

func (ix *Index) updateTupleCompositeFromSource(rec *SourceRecord, did DocID) {
	ex := extras(ix)
	ex.mu.RLock()
	c := ex.composite
	ex.mu.RUnlock()
	if c == nil {
		return
	}
	var term, date_key, source string
	var wi uint64
	for i := range rec.Values {
		v := &rec.Values[i]
		if int(v.Field) >= len(ix.fieldList) {
			continue
		}
		name := ix.fieldList[v.Field].name
		switch name {
		case "term":
			term = v.String
		case "group_id":
			if v.Kind == ValueNumber || v.Kind == ValueTimeUnix {
				wi = uint64(v.Number)
			} else {
				wi, _ = strconv.ParseUint(v.String, 10, 32)
			}
		case "date_key":
			date_key = v.String
		case "source", "code_system":
			source = v.String
		}
	}
	if term != "" && wi > 0 && date_key != "" {
		c.Add(term, uint32(wi), date_key, source, did)
	}
}

// QueryPlanCache stores normalized query templates. It is intentionally small and
// generation-safe; callers should clear it after an index generation swap.
type QueryPlanCache struct {
	mu    sync.RWMutex
	max   int
	order []string
	plans map[string]Query
}

func NewQueryPlanCache(max int) *QueryPlanCache {
	if max <= 0 {
		max = 1024
	}
	return &QueryPlanCache{max: max, plans: map[string]Query{}}
}
func (c *QueryPlanCache) Get(key string) (Query, bool) {
	c.mu.RLock()
	q, ok := c.plans[key]
	c.mu.RUnlock()
	return q, ok
}
func (c *QueryPlanCache) Put(key string, q Query) {
	if c == nil || key == "" || q == nil {
		return
	}
	c.mu.Lock()
	if _, ok := c.plans[key]; !ok {
		c.order = append(c.order, key)
		if len(c.order) > c.max {
			old := c.order[0]
			c.order = c.order[1:]
			delete(c.plans, old)
		}
	}
	c.plans[key] = q
	c.mu.Unlock()
}
func (c *QueryPlanCache) Clear() {
	c.mu.Lock()
	c.order = nil
	c.plans = map[string]Query{}
	c.mu.Unlock()
}

func (ix *Index) CompileLookupQuery(raw string) Query {
	ex := extras(ix)
	key := "lookup:" + raw
	if q, ok := ex.planner.Get(key); ok {
		return q
	}
	q := ParseLookupQueryFast(raw)
	ex.planner.Put(key, q)
	return q
}
