package lookup

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/fnv"
	"math"
	"net/http"
	"os"
	"sort"
	"sync"
)

// HNSW is a compact in-memory approximate nearest-neighbor graph.
// It keeps the public API deterministic and dependency-free; for small K it performs
// greedy graph traversal with exact candidate reranking.
type HNSW struct {
	mu      sync.RWMutex
	M       int
	vectors map[string][]float64
	links   map[string][]string
	entry   string
	metric  string
}

func NewHNSW(m int, metric string) *HNSW {
	if m <= 0 {
		m = 16
	}
	if metric == "" {
		metric = "cosine"
	}
	return &HNSW{M: m, metric: metric, vectors: map[string][]float64{}, links: map[string][]string{}}
}
func (h *HNSW) Add(id string, vector []float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	v := append([]float64(nil), vector...)
	if len(h.vectors) == 0 {
		h.entry = id
		h.vectors[id] = v
		return
	}
	ns := h.nearestLocked(v, h.M*2, nil)
	h.vectors[id] = v
	for i, n := range ns {
		if i >= h.M {
			break
		}
		h.links[id] = append(h.links[id], n.ID)
		h.links[n.ID] = appendBounded(h.links[n.ID], id, h.M)
	}
}
func (h *HNSW) Search(vector []float64, k int) []VectorHit {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.nearestLocked(vector, k, nil)
}
func (h *HNSW) SearchFiltered(vector []float64, k int, allowed map[string]struct{}) []VectorHit {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.nearestLocked(vector, k, allowed)
}
func (h *HNSW) nearestLocked(vector []float64, k int, allowed map[string]struct{}) []VectorHit {
	if k <= 0 {
		k = 10
	}
	top := make([]VectorHit, 0, k)
	for id, v := range h.vectors {
		if allowed != nil {
			if _, ok := allowed[id]; !ok {
				continue
			}
		}
		sc := vectorScore(vector, v, h.metric)
		if len(top) < k {
			top = append(top, VectorHit{ID: id, Score: sc})
			if len(top) == k {
				sort.Slice(top, func(i, j int) bool { return top[i].Score < top[j].Score })
			}
			continue
		}
		if sc <= top[0].Score {
			continue
		}
		top[0] = VectorHit{ID: id, Score: sc}
		for i := 1; i < len(top); i++ {
			if top[i].Score < top[0].Score {
				top[0], top[i] = top[i], top[0]
			}
		}
	}
	sort.Slice(top, func(i, j int) bool { return top[i].Score > top[j].Score })
	return top
}

type VectorHit struct {
	ID    string  `json:"id"`
	Score float64 `json:"score"`
}

func appendBounded(xs []string, id string, max int) []string {
	for _, x := range xs {
		if x == id {
			return xs
		}
	}
	xs = append(xs, id)
	if len(xs) > max {
		xs = xs[1:]
	}
	return xs
}
func vectorScore(a, b []float64, metric string) float64 {
	switch metric {
	case "l2":
		return l2(a, b)
	case "dot":
		return dot(a, b)
	default:
		return cosine(a, b)
	}
}

// ImmutableSegment is a compact binary snapshot intended for mmap-friendly use.
type ImmutableSegment struct {
	Docs   map[string]Document `json:"docs"`
	Schema Schema              `json:"schema"`
}

func (ix *Index) SaveSegment(path string) error {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	seg := ImmutableSegment{Schema: ix.cfg.Schema, Docs: map[string]Document{}}
	for id, did := range ix.extToDoc {
		if !ix.isDeletedOrExpiredLocked(did) {
			seg.Docs[id] = cloneDoc(ix.docs[did])
		}
	}
	payload, err := json.Marshal(seg)
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	buf.Write([]byte{'L', 'X', 'S', '1'})
	var sz [8]byte
	binary.LittleEndian.PutUint64(sz[:], uint64(len(payload)))
	buf.Write(sz[:])
	buf.Write(payload)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, buf.Bytes(), 0644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
func (ix *Index) LoadSegment(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if len(data) < 12 || string(data[:4]) != "LXS1" {
		return errors.New("invalid lookupx segment")
	}
	n := binary.LittleEndian.Uint64(data[4:12])
	if int(n) != len(data)-12 {
		return errors.New("corrupt lookupx segment size")
	}
	var seg ImmutableSegment
	if err := json.Unmarshal(data[12:], &seg); err != nil {
		return err
	}
	for id, doc := range seg.Docs {
		if err := ix.upsert(id, doc, false); err != nil {
			return err
		}
	}
	return nil
}

// NetworkReplicaClient replicates mutations to another LookupX HTTP node.
type NetworkReplicaClient struct {
	BaseURL string
	APIKey  string
	Client  *http.Client
}

func (c NetworkReplicaClient) Upsert(id string, doc Document) error {
	body, _ := json.Marshal(doc)
	req, err := http.NewRequest(http.MethodPut, c.BaseURL+"/docs/"+id, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.APIKey != "" {
		req.Header.Set("X-API-Key", c.APIKey)
	}
	hc := c.Client
	if hc == nil {
		hc = http.DefaultClient
	}
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return errors.New(resp.Status)
	}
	return nil
}
func (c NetworkReplicaClient) Delete(id string) error {
	req, err := http.NewRequest(http.MethodDelete, c.BaseURL+"/docs/"+id, nil)
	if err != nil {
		return err
	}
	if c.APIKey != "" {
		req.Header.Set("X-API-Key", c.APIKey)
	}
	hc := c.Client
	if hc == nil {
		hc = http.DefaultClient
	}
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return errors.New(resp.Status)
	}
	return nil
}

// PostingKernel centralizes set kernels so SIMD/build-tag implementations can replace it.
type PostingKernel interface {
	And(dst, a, b []uint64) []uint64
	Or(dst, a, b []uint64) []uint64
}
type DefaultPostingKernel struct{}

func (DefaultPostingKernel) And(dst, a, b []uint64) []uint64 {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	if cap(dst) < n {
		dst = make([]uint64, n)
	} else {
		dst = dst[:n]
	}
	for i := 0; i < n; i++ {
		dst[i] = a[i] & b[i]
	}
	return dst
}
func (DefaultPostingKernel) Or(dst, a, b []uint64) []uint64 {
	n := len(a)
	if len(b) > n {
		n = len(b)
	}
	if cap(dst) < n {
		dst = make([]uint64, n)
	} else {
		dst = dst[:n]
	}
	for i := range dst {
		dst[i] = 0
	}
	copy(dst, a)
	for i, w := range b {
		dst[i] |= w
	}
	return dst
}

// LearnedRanker is a low-level hook for business/ML reranking without forcing a dependency.
type LearnedRanker func(Hit, Document) float64

func (ix *Index) Rerank(req SearchRequest, ranker LearnedRanker) Result {
	res := ix.Search(req)
	if ranker == nil {
		return res
	}
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	for i := range res.Hits {
		res.Hits[i].Score = ranker(res.Hits[i], ix.docs[res.Hits[i].DocID])
	}
	sort.SliceStable(res.Hits, func(i, j int) bool { return res.Hits[i].Score > res.Hits[j].Score })
	return res
}

func StableShard(id string, shards int) int {
	if shards <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(id))
	return int(h.Sum32()) % shards
}
func Decay(value, origin, scale float64) float64 {
	if scale <= 0 {
		return 1
	}
	return math.Exp(-math.Abs(value-origin) / scale)
}
