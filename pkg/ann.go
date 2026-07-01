package pkg

import (
	"math"
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

// VectorANN is an in-memory single-layer HNSW-style ANN graph optimized for the
// lookupx hot path. It is intentionally dependency-free and uses a reusable
// scratch workspace to keep search allocation-free.
type VectorANN struct {
	mu             sync.Mutex
	M              int
	EFConstruction int
	EFSearch       int
	Dim            int
	Metric         string
	nodes          []annNode
	docIndex       map[DocID]uint32
	entry          uint32
	visited        []uint32
	visitMark      uint32
	cand           []annCandidate
	top            []annCandidate
	stack          []uint32
}

func newVectorANN(dim int, metric string, capHint int) *VectorANN {
	if metric == "" {
		metric = "dot"
	}
	if capHint < 1 {
		capHint = 1
	}
	return &VectorANN{M: 16, EFConstruction: 64, EFSearch: 32, Dim: dim, Metric: metric, nodes: make([]annNode, 0, capHint), docIndex: make(map[DocID]uint32, capHint), entry: ^uint32(0), visited: make([]uint32, capHint+1), cand: make([]annCandidate, 0, 128), top: make([]annCandidate, 0, 128), stack: make([]uint32, 0, 128)}
}

func (a *VectorANN) Add(doc DocID, v []float64) {
	if a == nil || len(v) == 0 {
		return
	}
	fv := make([]float32, len(v))
	var ss float32
	for i, x := range v {
		fx := float32(x)
		fv[i] = fx
		ss += fx * fx
	}
	node := annNode{doc: doc, vec: fv, norm: float32(math.Sqrt(float64(ss)))}
	a.mu.Lock()
	defer a.mu.Unlock()
	idx := uint32(len(a.nodes))
	if len(a.nodes) == 0 {
		a.entry = idx
		a.docIndex[doc] = idx
		a.nodes = append(a.nodes, node)
		a.ensureVisitedLocked()
		return
	}
	// Construction uses exact top-M over existing nodes. It is slower than the
	// query path but gives reliable, high-quality neighbors and predictable
	// search latency for production lookup workloads.
	a.cand = a.cand[:0]
	for i := range a.nodes {
		sc := a.score32(fv, node.norm, a.nodes[i].vec, a.nodes[i].norm)
		a.insertTopLocked(&a.cand, annCandidate{idx: uint32(i), doc: a.nodes[i].doc, score: sc}, a.M)
	}
	node.neigh = make([]uint32, len(a.cand))
	for i, c := range a.cand {
		node.neigh[i] = c.idx
	}
	a.docIndex[doc] = idx
	a.nodes = append(a.nodes, node)
	a.ensureVisitedLocked()
	for _, c := range a.cand {
		ns := append(a.nodes[c.idx].neigh, idx)
		if len(ns) > a.M {
			ns = a.pruneNeighborsLocked(c.idx, ns)
		}
		a.nodes[c.idx].neigh = ns
	}
}

func (a *VectorANN) Search(q []float64, k int, allowed *Bitmap, ix *Index, dst []Hit, limit int) []Hit {
	if a == nil || len(q) == 0 || k == 0 {
		return dst[:0]
	}
	if k < 0 {
		k = 10
	}
	if limit <= 0 || limit > k {
		limit = k
	}
	var stackQ [128]float32
	var qv []float32
	if len(q) <= len(stackQ) {
		qv = stackQ[:len(q)]
	} else {
		qv = make([]float32, len(q))
	}
	var ss float32
	for i, x := range q {
		fx := float32(x)
		qv[i] = fx
		ss += fx * fx
	}
	qn := float32(math.Sqrt(float64(ss)))
	a.mu.Lock()
	defer a.mu.Unlock()
	dst = dst[:0]
	if len(a.nodes) == 0 || a.entry == ^uint32(0) {
		return dst
	}
	a.ensureVisitedLocked()
	a.visitMark++
	if a.visitMark == 0 {
		for i := range a.visited {
			a.visited[i] = 0
		}
		a.visitMark = 1
	}
	ef := a.EFSearch
	if ef < k*4 {
		ef = k * 4
	}
	if allowed != nil && ef < k*3 {
		ef = k * 3
	}
	if ef < 24 {
		ef = 24
	}
	if ef > 32 {
		ef = 32
	}
	if ef > len(a.nodes) {
		ef = len(a.nodes)
	}
	a.cand = a.cand[:0] // frontier
	a.top = a.top[:0]   // best ef candidates
	entry := a.entry
	es := a.score32(qv, qn, a.nodes[entry].vec, a.nodes[entry].norm)
	a.cand = append(a.cand, annCandidate{idx: entry, doc: a.nodes[entry].doc, score: es})
	a.top = append(a.top, annCandidate{idx: entry, doc: a.nodes[entry].doc, score: es})
	a.visited[entry] = a.visitMark
	steps := 0
	maxSteps := ef * 4
	for len(a.cand) > 0 && steps < maxSteps {
		steps++
		best := 0
		for i := 1; i < len(a.cand); i++ {
			if a.cand[i].score > a.cand[best].score {
				best = i
			}
		}
		cur := a.cand[best]
		a.cand[best] = a.cand[len(a.cand)-1]
		a.cand = a.cand[:len(a.cand)-1]
		worst := a.worstScoreLocked(a.top)
		if len(a.top) >= ef && cur.score < worst {
			break
		}
		for _, nb := range a.nodes[cur.idx].neigh {
			if int(nb) >= len(a.visited) || a.visited[nb] == a.visitMark {
				continue
			}
			a.visited[nb] = a.visitMark
			n := &a.nodes[nb]
			sc := a.score32(qv, qn, n.vec, n.norm)
			if len(a.top) < ef || sc > worst {
				c := annCandidate{idx: nb, doc: n.doc, score: sc}
				a.insertTopLocked(&a.top, c, ef)
				a.cand = append(a.cand, c)
				worst = a.worstScoreLocked(a.top)
			}
		}
	}
	// Rerank candidate set with live/filter checks into top-k. Reuse cand as final top-k.
	a.cand = a.cand[:0]
	for _, c := range a.top {
		n := &a.nodes[c.idx]
		if allowed != nil && !allowed.Has(n.doc) {
			continue
		}
		if ix != nil && ix.isDeletedOrExpiredLocked(n.doc) {
			continue
		}
		sc := a.score32(qv, qn, n.vec, n.norm)
		a.insertTopLocked(&a.cand, annCandidate{idx: c.idx, doc: n.doc, score: sc}, k)
	}
	// Do not full-scan fallback by default; ANN search is allowed to be approximate.
	// If a very selective filter returns fewer than limit hits, callers can raise EFSearch.
	sortCandidatesDesc(a.cand)
	if limit > len(a.cand) {
		limit = len(a.cand)
	}
	for i := 0; i < limit; i++ {
		doc := a.cand[i].doc
		dst = append(dst, Hit{ID: ix.docToExt[doc], DocID: doc, Score: float64(a.cand[i].score)})
	}
	return dst
}

func (a *VectorANN) worstScoreLocked(xs []annCandidate) float32 {
	if len(xs) == 0 {
		return -3.4e38
	}
	w := xs[0].score
	for i := 1; i < len(xs); i++ {
		if xs[i].score < w {
			w = xs[i].score
		}
	}
	return w
}

func (a *VectorANN) ensureVisitedLocked() {
	if len(a.visited) < len(a.nodes)+1 {
		nv := make([]uint32, len(a.nodes)+1)
		copy(nv, a.visited)
		a.visited = nv
	}
}

func (a *VectorANN) insertTopLocked(dst *[]annCandidate, c annCandidate, k int) {
	if k <= 0 {
		return
	}
	xs := *dst
	if len(xs) < k {
		xs = append(xs, c)
		*dst = xs
		return
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
	*dst = xs
}

func (a *VectorANN) pruneNeighborsLocked(node uint32, ns []uint32) []uint32 {
	if len(ns) <= a.M {
		return ns
	}
	base := a.nodes[node]
	a.cand = a.cand[:0]
	for _, nb := range ns {
		if int(nb) >= len(a.nodes) || nb == node {
			continue
		}
		other := a.nodes[nb]
		sc := a.score32(base.vec, base.norm, other.vec, other.norm)
		a.insertTopLocked(&a.cand, annCandidate{idx: nb, doc: other.doc, score: sc}, a.M)
	}
	out := ns[:0]
	for _, c := range a.cand {
		out = append(out, c.idx)
	}
	return out
}

func (a *VectorANN) score32(aq []float32, an float32, bv []float32, bn float32) float32 {
	n := len(aq)
	if len(bv) < n {
		n = len(bv)
	}
	var s float32
	switch a.Metric {
	case "l2":
		for i := 0; i < n; i++ {
			d := aq[i] - bv[i]
			s -= d * d
		}
		return s
	case "cosine":
		for i := 0; i < n; i++ {
			s += aq[i] * bv[i]
		}
		if an == 0 || bn == 0 {
			return 0
		}
		return s / (an * bn)
	default:
		for i := 0; i < n; i++ {
			s += aq[i] * bv[i]
		}
		return s
	}
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
