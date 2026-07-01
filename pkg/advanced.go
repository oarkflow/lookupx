package pkg

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
	"net"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

type Phrase struct {
	Field, Value string
	Slop         int
}
type Proximity struct {
	Field   string
	Terms   []string
	Slop    int
	Ordered bool
}
type CIDR struct{ Field, Value string }
type DomainWildcard struct{ Field, Pattern string }
type VectorQuery struct {
	Field  string
	Vector []float64
	K      int
	Metric string
	Filter Query
}
type Compound struct {
	Fields []string
	Values []string
}

type ExplainQuery struct{ Q Query }

func (q Phrase) eval(ix *Index) *Bitmap { return ix.phraseBitmap(q.Field, q.Value, q.Slop) }
func (q Proximity) eval(ix *Index) *Bitmap {
	return ix.proximityBitmap(q.Field, q.Terms, q.Slop, q.Ordered)
}
func (q CIDR) eval(ix *Index) *Bitmap           { return ix.cidrBitmap(q.Field, q.Value) }
func (q DomainWildcard) eval(ix *Index) *Bitmap { return ix.domainWildcardBitmap(q.Field, q.Pattern) }
func (q Compound) eval(ix *Index) *Bitmap {
	return Term{Field: compoundField(q.Fields), Value: compoundValue(q.Values)}.eval(ix)
}
func (q VectorQuery) eval(ix *Index) *Bitmap {
	return ix.vectorBitmap(q.Field, q.Vector, q.K, q.Metric, q.Filter)
}
func (q ExplainQuery) eval(ix *Index) *Bitmap { return q.Q.eval(ix) }

type Analyzer interface {
	Analyze(text string, dst []string) []string
}
type AnalyzerFunc func(text string, dst []string) []string

func (f AnalyzerFunc) Analyze(text string, dst []string) []string { return f(text, dst) }

var analyzerMu sync.RWMutex
var analyzers = map[string]Analyzer{}
var synonyms = map[string][]string{}

func RegisterAnalyzer(name string, a Analyzer) {
	analyzerMu.Lock()
	analyzers[name] = a
	analyzerMu.Unlock()
}
func RegisterSynonym(term string, expansions ...string) {
	analyzerMu.Lock()
	synonyms[strings.ToLower(term)] = expansions
	analyzerMu.Unlock()
}

func init() {
	RegisterAnalyzer("standard", AnalyzerFunc(func(text string, dst []string) []string { return tokenize(text, true, dst) }))
	RegisterAnalyzer("stop", AnalyzerFunc(func(text string, dst []string) []string {
		dst = tokenize(text, true, dst)
		out := dst[:0]
		for _, t := range dst {
			if !stopword(t) {
				out = append(out, t)
			}
		}
		return out
	}))
	RegisterAnalyzer("stem", AnalyzerFunc(func(text string, dst []string) []string {
		dst = tokenize(text, true, dst)
		for i, t := range dst {
			dst[i] = simpleStem(t)
		}
		return dst
	}))
	RegisterAnalyzer("nepali", AnalyzerFunc(func(text string, dst []string) []string { return tokenize(normalizeDevanagari(text), true, dst) }))
	RegisterAnalyzer("cjk", AnalyzerFunc(func(text string, dst []string) []string { return cjkTokens(text, dst) }))
}

func analyzeWith(opt FieldOptions, text string, dst []string) []string {
	name := opt.Analyzer
	if name == "" {
		name = "standard"
	}
	analyzerMu.RLock()
	a := analyzers[name]
	analyzerMu.RUnlock()
	if a == nil {
		return tokenize(text, opt.Lowercase, dst)
	}
	dst = a.Analyze(text, dst)
	analyzerMu.RLock()
	defer analyzerMu.RUnlock()
	out := dst
	for _, t := range dst {
		if ex := synonyms[t]; len(ex) > 0 {
			out = append(out, ex...)
		}
	}
	return out
}

func stopword(s string) bool {
	switch s {
	case "a", "an", "the", "and", "or", "of", "to", "in", "on", "for", "with", "is", "are", "was", "were":
		return true
	}
	return false
}
func simpleStem(s string) string {
	for _, suf := range []string{"ingly", "edly", "ing", "ed", "ies", "s"} {
		if len(s) > len(suf)+2 && strings.HasSuffix(s, suf) {
			if suf == "ies" {
				return strings.TrimSuffix(s, suf) + "y"
			}
			return strings.TrimSuffix(s, suf)
		}
	}
	return s
}
func normalizeDevanagari(s string) string {
	return strings.NewReplacer("०", "0", "१", "1", "२", "2", "३", "3", "४", "4", "५", "5", "६", "6", "७", "7", "८", "8", "९", "9", "।", " ").Replace(s)
}
func cjkTokens(s string, dst []string) []string {
	dst = dst[:0]
	r := []rune(strings.ToLower(s))
	for i := 0; i < len(r); i++ {
		if !isSpace(r[i]) {
			dst = append(dst, string(r[i]))
			if i+1 < len(r) && !isSpace(r[i+1]) {
				dst = append(dst, string(r[i:i+2]))
			}
		}
	}
	return dst
}
func isSpace(r rune) bool { return r == ' ' || r == '\t' || r == '\n' || r == '\r' }

func compoundField(fields []string) string { return "__compound:" + strings.Join(fields, "+") }
func compoundValue(values []string) string { return strings.Join(values, "\x1f") }
func CompoundKey(fields []string, doc Document) string {
	vals := make([]string, 0, len(fields))
	for _, f := range fields {
		vals = append(vals, normalize(fmt.Sprint(doc[f]), true))
	}
	return compoundValue(vals)
}

func normalizeEmail(v string) string { return strings.ToLower(strings.TrimSpace(v)) }
func normalizePhone(v string) string {
	var b strings.Builder
	for _, r := range v {
		if r >= '0' && r <= '9' || r == '+' {
			b.WriteRune(r)
		}
	}
	return b.String()
}
func normalizeDomain(v string) string {
	v = strings.ToLower(strings.TrimSpace(v))
	v = strings.TrimPrefix(v, "*.")
	return strings.TrimSuffix(v, ".")
}
func normalizeURL(v string) string {
	u, err := url.Parse(strings.TrimSpace(v))
	if err != nil {
		return strings.ToLower(v)
	}
	u.Scheme = strings.ToLower(u.Scheme)
	u.Host = strings.ToLower(u.Host)
	return u.String()
}

func ipToUint64(s string) (uint64, bool) {
	ip := net.ParseIP(strings.TrimSpace(s))
	if ip == nil {
		return 0, false
	}
	if v4 := ip.To4(); v4 != nil {
		return uint64(binary.BigEndian.Uint32(v4)), true
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(ip.String()))
	return h.Sum64(), true
}

func cosine(a, b []float64) float64 {
	var dot, na, nb float64
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		dot += a[i] * b[i]
		na += a[i] * a[i]
		nb += b[i] * b[i]
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / (math.Sqrt(na) * math.Sqrt(nb))
}
func l2(a, b []float64) float64 {
	var s float64
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		d := a[i] - b[i]
		s += d * d
	}
	return -math.Sqrt(s)
}
func dot(a, b []float64) float64 {
	var s float64
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		s += a[i] * b[i]
	}
	return s
}

func (ix *Index) BM25(field, term string, id DocID) float64 {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	fi := ix.fields[field]
	if fi == nil {
		return 0
	}
	b := fi.terms[normalize(term, ix.cfg.Schema.Fields[field].Lowercase)]
	if b == nil || !b.Has(id) {
		return 0
	}
	df := float64(b.Count())
	n := float64(ix.nextDocID)
	if df == 0 {
		return 0
	}
	idf := math.Log(1 + (n-df+0.5)/(df+0.5))
	return idf
}

func (ix *Index) Highlight(id, field, term string, size int) []Highlight {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	did, ok := ix.extToDoc[id]
	if !ok || ix.isDeletedOrExpiredLocked(did) {
		return nil
	}
	text := fmt.Sprint(ix.docs[did][field])
	low := strings.ToLower(text)
	t := strings.ToLower(term)
	pos := strings.Index(low, t)
	if pos < 0 {
		return nil
	}
	if size <= 0 {
		size = 80
	}
	start := pos - size/2
	if start < 0 {
		start = 0
	}
	end := pos + len(term) + size/2
	if end > len(text) {
		end = len(text)
	}
	frag := text[start:pos] + "<mark>" + text[pos:pos+len(term)] + "</mark>" + text[pos+len(term):end]
	return []Highlight{{Field: field, Fragments: []string{frag}}}
}

func (ix *Index) phraseBitmap(field, value string, slop int) *Bitmap {
	ix.mu.RLock()
	opt := ix.cfg.Schema.Fields[field]
	terms := analyzeWith(opt, value, nil)
	fi := ix.fields[field]
	if len(terms) == 0 || fi == nil {
		ix.mu.RUnlock()
		return NewBitmap()
	}
	if slop == 0 && len(terms) >= 2 && len(terms) <= 4 && opt.Phrase {
		key := strings.Join(terms, "\x1f")
		if did := fi.phraseOne[key]; did != 0 && !ix.deleted.Has(did) {
			b := NewBitmapCap(ix.nextDocID)
			b.Add(did)
			ix.mu.RUnlock()
			return b
		}
		if b := fi.phrases[key]; b != nil {
			ix.mu.RUnlock()
			return b
		}
		ix.mu.RUnlock()
		return NewBitmap()
	}
	// Build candidates directly from positional postings, shortest posting first.
	best := -1
	bestCount := int(^uint(0) >> 1)
	for i, t := range terms {
		c := postingCount(fi.terms, fi.termOne, t)
		if c == 0 {
			ix.mu.RUnlock()
			return NewBitmap()
		}
		if c < bestCount {
			bestCount, best = c, i
		}
	}
	bm := postingClone(fi.terms, fi.termOne, terms[best], ix.nextDocID)
	for i, t := range terms {
		if i == best {
			continue
		}
		bm.AndInPlace(postingBitmap(fi.terms, fi.termOne, t, ix.nextDocID))
		if bm.Empty() {
			ix.mu.RUnlock()
			return bm
		}
	}
	r := NewBitmapCap(ix.nextDocID)
	bm.Each(func(id DocID) bool {
		if ix.isDeletedOrExpiredLocked(id) {
			return true
		}
		if positionsPhrase(fi, id, terms, slop) {
			r.Add(id)
		}
		return true
	})
	ix.mu.RUnlock()
	return r
}
func positionsPhrase(fi *fieldIndex, id DocID, terms []string, slop int) bool {
	if slop < 0 {
		slop = 0
	}
	first := fi.positions[terms[0]][id]
	if len(first) == 0 {
		return false
	}
	for _, p0 := range first {
		prev := p0
		ok := true
		for i := 1; i < len(terms); i++ {
			plist := fi.positions[terms[i]][id]
			found := false
			maxp := prev + uint32(slop) + 1
			for _, p := range plist {
				if p <= prev {
					continue
				}
				if p > maxp {
					break
				}
				prev = p
				found = true
				break
			}
			if !found {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
	}
	return false
}
func phraseTokens(toks, terms []string, slop int) bool {
	if slop < 0 {
		slop = 0
	}
	for i := range toks {
		if toks[i] != terms[0] {
			continue
		}
		p := i
		ok := true
		for j := 1; j < len(terms); j++ {
			found := false
			max := p + slop + 1
			for k := p + 1; k < len(toks) && k <= max; k++ {
				if toks[k] == terms[j] {
					p = k
					found = true
					break
				}
			}
			if !found {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
	}
	return false
}
func (ix *Index) proximityBitmap(field string, terms []string, slop int, ordered bool) *Bitmap {
	if ordered {
		return ix.phraseBitmap(field, strings.Join(terms, " "), slop)
	}
	qs := make([]Query, len(terms))
	for i, t := range terms {
		qs[i] = Term{field, t}
	}
	return And(qs).eval(ix)
}
func (ix *Index) cidrBitmap(field, value string) *Bitmap {
	ip, netw, err := net.ParseCIDR(value)
	r := NewBitmap()
	if err != nil {
		return r
	}
	base4 := ip.To4()
	if base4 == nil {
		return r
	}
	ones, bits := netw.Mask.Size()
	if bits != 32 {
		return r
	}
	base := binary.BigEndian.Uint32(base4)
	var mask uint32
	if ones == 0 {
		mask = 0
	} else {
		mask = ^uint32(0) << uint(32-ones)
	}
	lo := base & mask
	hi := lo | ^mask
	ix.mu.Lock()
	defer ix.mu.Unlock()
	pairs := ix.sortedIPv4Locked(field)
	if len(pairs) == 0 {
		return r
	}
	start := sort.Search(len(pairs), func(i int) bool { return pairs[i].v >= lo })
	end := sort.Search(len(pairs), func(i int) bool { return pairs[i].v > hi })
	if end < start {
		end = start
	}
	r = NewBitmapCap(ix.nextDocID)
	for _, p := range pairs[start:end] {
		if !ix.isDeletedOrExpiredLocked(p.id) {
			r.Add(p.id)
		}
	}
	return r
}
func (ix *Index) domainWildcardBitmap(field, pattern string) *Bitmap {
	suffix := normalizeDomain(pattern)
	ix.mu.RLock()
	fi := ix.fields[field]
	if fi != nil {
		if b := fi.suffix[suffix]; b != nil {
			ix.mu.RUnlock()
			return b
		}
	}
	ix.mu.RUnlock()
	r := NewBitmap()
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	for id, s := range ix.strings[field] {
		d := normalizeDomain(s)
		if d == suffix || strings.HasSuffix(d, "."+suffix) {
			r.Add(id)
		}
	}
	return r
}
func (ix *Index) vectorBitmap(field string, q []float64, k int, metric string, filter Query) *Bitmap {
	if k <= 0 {
		k = 10
	}
	var allowed *Bitmap
	if filter != nil {
		allowed = filter.eval(ix)
	}
	type pair struct {
		id    DocID
		score float64
	}
	top := make([]pair, 0, k)
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	for id, v := range ix.vectors[field] {
		if allowed != nil && !allowed.Has(id) {
			continue
		}
		if ix.isDeletedOrExpiredLocked(id) {
			continue
		}
		var sc float64
		switch metric {
		case "l2":
			sc = l2(q, v)
		case "dot":
			sc = dot(q, v)
		default:
			sc = cosine(q, v)
		}
		if len(top) < k {
			top = append(top, pair{id, sc})
			if len(top) == k {
				sort.Slice(top, func(i, j int) bool { return top[i].score < top[j].score })
			}
			continue
		}
		if sc <= top[0].score {
			continue
		}
		top[0] = pair{id, sc}
		// Restore min-at-zero for tiny k without allocating a heap.
		for i := 1; i < len(top); i++ {
			if top[i].score < top[0].score {
				top[0], top[i] = top[i], top[0]
			}
		}
	}
	r := NewBitmapCap(ix.nextDocID)
	for _, p := range top {
		r.Add(p.id)
	}
	return r
}

func (ix *Index) TruncateWAL() error {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if ix.cfg.WALPath == "" {
		return nil
	}
	if ix.wal != nil {
		_ = ix.wal.Close()
	}
	f, err := openTrunc(ix.cfg.WALPath)
	if err != nil {
		return err
	}
	ix.wal = f
	return nil
}
func openTrunc(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR|os.O_APPEND, 0644)
}

type ShardSet struct{ shards []*Index }

func NewShardSet(shards ...*Index) *ShardSet { return &ShardSet{shards: shards} }
func (s *ShardSet) Search(req SearchRequest) Result {
	out := Result{}
	for _, ix := range s.shards {
		r := ix.Search(req)
		out.Total += r.Total
		out.Hits = append(out.Hits, r.Hits...)
	}
	if len(out.Hits) > req.Limit && req.Limit > 0 {
		out.Hits = out.Hits[:req.Limit]
	}
	return out
}
func (s *ShardSet) Upsert(id string, doc Document) error {
	if len(s.shards) == 0 {
		return nil
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(id))
	return s.shards[int(h.Sum32())%len(s.shards)].Upsert(id, doc)
}

type ReplicaSet struct {
	primary  *Index
	replicas []*Index
}

func NewReplicaSet(primary *Index, replicas ...*Index) *ReplicaSet {
	return &ReplicaSet{primary: primary, replicas: replicas}
}
func (r *ReplicaSet) Upsert(id string, doc Document) error {
	if err := r.primary.Upsert(id, doc); err != nil {
		return err
	}
	for _, rep := range r.replicas {
		_ = rep.Upsert(id, doc)
	}
	return nil
}
func (r *ReplicaSet) Delete(id string) error {
	if err := r.primary.Delete(id); err != nil {
		return err
	}
	for _, rep := range r.replicas {
		_ = rep.Delete(id)
	}
	return nil
}

func sleepUntil(t time.Time) {
	if d := time.Until(t); d > 0 {
		time.Sleep(d)
	}
}
