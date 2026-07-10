package pkg

import (
	"math"
	"strings"
	"sync"
)

type annNode struct {
	doc   DocID
	vec   []float32
	norm  float32
	neigh []uint32
}

type annCandidate struct {
	idx   uint32
	doc   DocID
	score float32
}

type annWorkspace struct {
	visited []uint32
	mark    uint32
	front   []annCandidate
	top     []annCandidate
	result  []annCandidate
	query   []float32
}

// VectorANN is a dependency-free, single-layer proximity graph for low-latency
// vector retrieval. Searches are concurrent; mutations are exclusive. Query
// workspaces are pooled so steady-state search avoids heap allocation.
type VectorANN struct {
	mu             sync.RWMutex
	M              int
	EFConstruction int
	EFSearch       int
	Dim            int
	Metric         string
	nodes          []annNode
	docIndex       map[DocID]uint32
	entry          uint32
	pool           sync.Pool
	insertVisited  []uint32
	insertMark     uint32
	insertFront    []annCandidate
	insertTop      []annCandidate
	insertResult   []annCandidate
}

func newVectorANNWithOptions(opt FieldOptions, capHint int) *VectorANN {
	if capHint < 1 {
		capHint = 1
	}
	metric := normalizeVectorMetric(opt.VectorMetric)
	m := opt.VectorM
	if m <= 0 {
		m = 16
	}
	if m < 4 {
		m = 4
	}
	if m > 64 {
		m = 64
	}
	efc := opt.VectorEFConstruction
	if efc <= 0 {
		efc = maxInt(64, m*4)
	}
	if efc < m {
		efc = m
	}
	efs := opt.VectorEFSearch
	if efs <= 0 {
		efs = 64
	}
	a := &VectorANN{M: m, EFConstruction: efc, EFSearch: efs, Dim: opt.Dim, Metric: metric,
		nodes: make([]annNode, 0, capHint), docIndex: make(map[DocID]uint32, capHint), entry: ^uint32(0),
		insertVisited: make([]uint32, capHint+1), insertFront: make([]annCandidate, 0, efc*2),
		insertTop: make([]annCandidate, 0, efc*2), insertResult: make([]annCandidate, 0, m)}
	a.pool.New = func() any {
		return &annWorkspace{
			visited: make([]uint32, capHint+1), front: make([]annCandidate, 0, efs*2),
			top: make([]annCandidate, 0, efs*2), result: make([]annCandidate, 0, 64),
			query: make([]float32, 0, maxInt(opt.Dim, 128)),
		}
	}
	return a
}

func normalizeVectorMetric(metric string) string {
	switch strings.ToLower(metric) {
	case "l2", "euclidean":
		return "l2"
	case "cos", "cosine", "":
		return "cosine"
	case "dot", "inner_product", "ip":
		return "dot"
	default:
		return "cosine"
	}
}

// Add inserts or replaces a vector. Vectors with a dimension different from the
// configured field dimension are rejected. Replacement preserves graph links.
func (a *VectorANN) Add(doc DocID, v []float64) {
	if a == nil || len(v) == 0 || (a.Dim > 0 && len(v) != a.Dim) {
		return
	}
	fv, norm := vector32(v)
	a.mu.Lock()
	defer a.mu.Unlock()
	if old, ok := a.docIndex[doc]; ok {
		a.nodes[old].vec, a.nodes[old].norm = fv, norm
		return
	}
	idx := uint32(len(a.nodes))
	node := annNode{doc: doc, vec: fv, norm: norm}
	if len(a.nodes) == 0 {
		a.entry = idx
		a.docIndex[doc] = idx
		a.nodes = append(a.nodes, node)
		return
	}
	candidates := a.insertNeighborsLocked(fv, norm)
	node.neigh = make([]uint32, len(candidates))
	for i, c := range candidates {
		node.neigh[i] = c.idx
	}
	a.docIndex[doc] = idx
	a.nodes = append(a.nodes, node)
	for _, c := range candidates {
		ns := append(a.nodes[c.idx].neigh, idx)
		a.nodes[c.idx].neigh = a.pruneNeighborsLocked(c.idx, ns)
	}
}

// insertNeighborsLocked selects neighbors for a new vector using the existing
// proximity graph instead of a full scan. Small indexes still use an exact scan
// to bootstrap graph quality; larger indexes use EFConstruction to bound insert
// cost to roughly O(EFConstruction*M) instead of O(N).
func (a *VectorANN) insertNeighborsLocked(fv []float32, norm float32) []annCandidate {
	a.insertResult = a.insertResult[:0]
	if len(a.nodes) <= a.M*2 || a.entry == ^uint32(0) {
		for i := range a.nodes {
			sc := scoreVector(a.Metric, fv, norm, a.nodes[i].vec, a.nodes[i].norm)
			a.insertResult = insertTop(a.insertResult, annCandidate{idx: uint32(i), doc: a.nodes[i].doc, score: sc}, a.M)
		}
		return a.insertResult
	}
	ef := a.EFConstruction
	if ef <= 0 {
		ef = maxInt(64, a.M*4)
	}
	if ef > len(a.nodes) {
		ef = len(a.nodes)
	}
	if len(a.insertVisited) < len(a.nodes) {
		a.insertVisited = make([]uint32, len(a.nodes))
	}
	a.insertMark++
	if a.insertMark == 0 {
		clear(a.insertVisited)
		a.insertMark = 1
	}
	a.insertFront = a.insertFront[:0]
	a.insertTop = a.insertTop[:0]
	entry := a.entry
	es := scoreVector(a.Metric, fv, norm, a.nodes[entry].vec, a.nodes[entry].norm)
	seed := annCandidate{idx: entry, doc: a.nodes[entry].doc, score: es}
	a.insertFront = append(a.insertFront, seed)
	a.insertTop = append(a.insertTop, seed)
	a.insertVisited[entry] = a.insertMark
	maxSteps := maxInt(ef*8, 64)
	for steps := 0; len(a.insertFront) > 0 && steps < maxSteps; steps++ {
		best := bestCandidate(a.insertFront)
		cur := a.insertFront[best]
		a.insertFront[best] = a.insertFront[len(a.insertFront)-1]
		a.insertFront = a.insertFront[:len(a.insertFront)-1]
		worst := worstScore(a.insertTop)
		if len(a.insertTop) >= ef && cur.score < worst {
			break
		}
		for _, nb := range a.nodes[cur.idx].neigh {
			if int(nb) >= len(a.insertVisited) || a.insertVisited[nb] == a.insertMark {
				continue
			}
			a.insertVisited[nb] = a.insertMark
			n := &a.nodes[nb]
			sc := scoreVector(a.Metric, fv, norm, n.vec, n.norm)
			if len(a.insertTop) < ef || sc > worst {
				c := annCandidate{idx: nb, doc: n.doc, score: sc}
				a.insertTop = insertTop(a.insertTop, c, ef)
				a.insertFront = append(a.insertFront, c)
				worst = worstScore(a.insertTop)
			}
		}
	}
	for _, c := range a.insertTop {
		a.insertResult = insertTop(a.insertResult, c, a.M)
	}
	return a.insertResult
}

// Search performs ANN search unless exact is true. metric may override the
// field metric. EFSearch and oversample are per-query controls.
func (a *VectorANN) Search(q []float64, k int, metric string, efSearch int, oversample int, exact bool, allowed *Bitmap, ix *Index, dst []Hit, limit int) []Hit {
	if a == nil || len(q) == 0 || k == 0 || (a.Dim > 0 && len(q) != a.Dim) {
		return dst[:0]
	}
	if k < 0 {
		k = 10
	}
	if limit <= 0 || limit > k {
		limit = k
	}
	m := normalizeVectorMetric(metric)
	if metric == "" {
		m = a.Metric
	}
	qv, qn := vector32(q)
	a.mu.RLock()
	defer a.mu.RUnlock()
	dst = dst[:0]
	if len(a.nodes) == 0 {
		return dst
	}
	ws := a.pool.Get().(*annWorkspace)
	defer a.pool.Put(ws)
	ws.query = append(ws.query[:0], qv...)
	if exact {
		ws.result = ws.result[:0]
		for i := range a.nodes {
			n := &a.nodes[i]
			if allowed != nil && !allowed.Has(n.doc) {
				continue
			}
			if ix != nil && ix.isDeletedOrExpiredLocked(n.doc) {
				continue
			}
			sc := scoreVector(m, ws.query, qn, n.vec, n.norm)
			ws.result = insertTop(ws.result, annCandidate{idx: uint32(i), doc: n.doc, score: sc}, k)
		}
		return appendHits(ix, ws.result, dst, limit)
	}
	if efSearch <= 0 {
		efSearch = a.EFSearch
	}
	if oversample <= 0 {
		oversample = 4
	}
	ef := maxInt(efSearch, k*oversample)
	if allowed != nil {
		ef = maxInt(ef, k*8)
	}
	if ef > len(a.nodes) {
		ef = len(a.nodes)
	}
	if len(ws.visited) < len(a.nodes) {
		ws.visited = make([]uint32, len(a.nodes))
	}
	ws.mark++
	if ws.mark == 0 {
		clear(ws.visited)
		ws.mark = 1
	}
	ws.front = ws.front[:0]
	ws.top = ws.top[:0]
	ws.result = ws.result[:0]
	entry := a.entry
	es := scoreVector(m, ws.query, qn, a.nodes[entry].vec, a.nodes[entry].norm)
	seed := annCandidate{idx: entry, doc: a.nodes[entry].doc, score: es}
	ws.front = append(ws.front, seed)
	ws.top = append(ws.top, seed)
	ws.visited[entry] = ws.mark
	maxSteps := maxInt(ef*8, 64)
	for steps := 0; len(ws.front) > 0 && steps < maxSteps; steps++ {
		best := bestCandidate(ws.front)
		cur := ws.front[best]
		ws.front[best] = ws.front[len(ws.front)-1]
		ws.front = ws.front[:len(ws.front)-1]
		worst := worstScore(ws.top)
		if len(ws.top) >= ef && cur.score < worst {
			break
		}
		for _, nb := range a.nodes[cur.idx].neigh {
			if int(nb) >= len(ws.visited) || ws.visited[nb] == ws.mark {
				continue
			}
			ws.visited[nb] = ws.mark
			n := &a.nodes[nb]
			sc := scoreVector(m, ws.query, qn, n.vec, n.norm)
			if len(ws.top) < ef || sc > worst {
				c := annCandidate{idx: nb, doc: n.doc, score: sc}
				ws.top = insertTop(ws.top, c, ef)
				ws.front = append(ws.front, c)
				worst = worstScore(ws.top)
			}
		}
	}
	for _, c := range ws.top {
		n := &a.nodes[c.idx]
		if allowed != nil && !allowed.Has(n.doc) {
			continue
		}
		if ix != nil && ix.isDeletedOrExpiredLocked(n.doc) {
			continue
		}
		ws.result = insertTop(ws.result, annCandidate{idx: c.idx, doc: n.doc, score: scoreVector(m, ws.query, qn, n.vec, n.norm)}, k)
	}
	if allowed != nil && len(ws.result) < limit {
		// Highly selective tenant/ACL/category filters can exclude every ANN
		// candidate. Reliability is more important than returning an empty false
		// negative, so fall back to an exact scan over the allowed bitmap only.
		ws.result = ws.result[:0]
		allowed.Each(func(doc DocID) bool {
			idx, ok := a.docIndex[doc]
			if !ok || int(idx) >= len(a.nodes) {
				return true
			}
			n := &a.nodes[idx]
			if ix != nil && ix.isDeletedOrExpiredLocked(n.doc) {
				return true
			}
			ws.result = insertTop(ws.result, annCandidate{idx: idx, doc: n.doc, score: scoreVector(m, ws.query, qn, n.vec, n.norm)}, k)
			return true
		})
	}
	return appendHits(ix, ws.result, dst, limit)
}

func appendHits(ix *Index, xs []annCandidate, dst []Hit, limit int) []Hit {
	sortCandidatesDesc(xs)
	if limit > len(xs) {
		limit = len(xs)
	}
	for i := 0; i < limit; i++ {
		c := xs[i]
		id := ""
		if ix != nil && int(c.doc) < len(ix.docToExt) {
			id = ix.docToExt[c.doc]
		}
		dst = append(dst, Hit{ID: id, DocID: c.doc, Score: float64(c.score)})
	}
	return dst
}

func vector32(v []float64) ([]float32, float32) {
	out := make([]float32, len(v))
	var ss float32
	for i, x := range v {
		f := float32(x)
		out[i] = f
		ss += f * f
	}
	return out, float32(math.Sqrt(float64(ss)))
}
func scoreVector(metric string, aq []float32, an float32, bv []float32, bn float32) float32 {
	if len(aq) != len(bv) {
		return -float32(math.MaxFloat32)
	}
	var s float32
	switch metric {
	case "l2":
		for i := range aq {
			d := aq[i] - bv[i]
			s -= d * d
		}
		return s
	case "dot":
		for i := range aq {
			s += aq[i] * bv[i]
		}
		return s
	default:
		for i := range aq {
			s += aq[i] * bv[i]
		}
		if an == 0 || bn == 0 {
			return 0
		}
		return s / (an * bn)
	}
}
func (a *VectorANN) pruneNeighborsLocked(node uint32, ns []uint32) []uint32 {
	if len(ns) <= a.M {
		return ns
	}
	base := a.nodes[node]
	top := make([]annCandidate, 0, a.M)
	for _, nb := range ns {
		if int(nb) >= len(a.nodes) || nb == node {
			continue
		}
		o := a.nodes[nb]
		top = insertTop(top, annCandidate{idx: nb, doc: o.doc, score: scoreVector(a.Metric, base.vec, base.norm, o.vec, o.norm)}, a.M)
	}
	out := make([]uint32, len(top))
	for i, c := range top {
		out[i] = c.idx
	}
	return out
}
func insertTop(xs []annCandidate, c annCandidate, k int) []annCandidate {
	if k <= 0 {
		return xs
	}
	if len(xs) < k {
		return append(xs, c)
	}
	min := 0
	for i := 1; i < len(xs); i++ {
		if xs[i].score < xs[min].score {
			min = i
		}
	}
	if c.score > xs[min].score {
		xs[min] = c
	}
	return xs
}
func worstScore(xs []annCandidate) float32 {
	if len(xs) == 0 {
		return -float32(math.MaxFloat32)
	}
	w := xs[0].score
	for i := 1; i < len(xs); i++ {
		if xs[i].score < w {
			w = xs[i].score
		}
	}
	return w
}
func bestCandidate(xs []annCandidate) int {
	b := 0
	for i := 1; i < len(xs); i++ {
		if xs[i].score > xs[b].score {
			b = i
		}
	}
	return b
}
func sortCandidatesDesc(xs []annCandidate) {
	for i := 1; i < len(xs); i++ {
		x := xs[i]
		j := i - 1
		for j >= 0 && xs[j].score < x.score {
			xs[j+1] = xs[j]
			j--
		}
		xs[j+1] = x
	}
}
func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
