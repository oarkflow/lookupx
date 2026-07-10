package pkg

import (
	"errors"
	"fmt"
	"math"
	"os"
)

// validateDocumentLocked performs mutation preflight checks while the caller
// holds ix.mu. It prevents partially indexed documents caused by bad vector
// values, dimension mismatches, NaN/Inf coordinates, or unsupported vector
// encodings.
func (ix *Index) validateDocumentLocked(doc Document) error {
	if doc == nil {
		return nil
	}
	for _, sf := range ix.fieldList {
		if sf.opt.Kind != FieldVector && sf.opt.Dim <= 0 {
			continue
		}
		v, ok := doc[sf.name]
		if !ok || v == nil {
			continue
		}
		vec, ok := toVector(v)
		if !ok {
			return fmt.Errorf("vector field %q must be []float64, []int, or JSON numeric array", sf.name)
		}
		if len(vec) == 0 {
			return fmt.Errorf("vector field %q must not be empty", sf.name)
		}
		if sf.opt.Dim > 0 && len(vec) != sf.opt.Dim {
			return fmt.Errorf("vector field %q dimension mismatch: got %d want %d", sf.name, len(vec), sf.opt.Dim)
		}
		for i, x := range vec {
			if math.IsNaN(x) || math.IsInf(x, 0) {
				return fmt.Errorf("vector field %q contains non-finite value at dimension %d", sf.name, i)
			}
		}
	}
	return nil
}

// Compact rebuilds mutable acceleration structures and drops stale vector nodes
// for deleted/expired documents. It is intentionally conservative: postings keep
// tombstones for correctness, while vector graphs are rebuilt to avoid long-run
// update/delete degradation.
func (ix *Index) Compact() error {
	if ix == nil {
		return errors.New("nil index")
	}
	return ix.RebuildVectorIndexes()
}

// RebuildVectorIndexes rebuilds every vector ANN graph from currently live
// vectors. It is safe to run online, but it takes the index write lock while the
// live vector maps are swapped.
func (ix *Index) RebuildVectorIndexes() error {
	if ix == nil {
		return errors.New("nil index")
	}
	ix.mu.Lock()
	defer ix.mu.Unlock()
	newVectors := make(map[string]map[DocID][]float64, len(ix.vectors))
	newANNs := make(map[string]*VectorANN, len(ix.anns))
	for _, sf := range ix.fieldList {
		if sf.opt.Kind != FieldVector && sf.opt.Dim <= 0 {
			continue
		}
		field := sf.name
		old := ix.vectors[field]
		live := make(map[DocID][]float64, len(old))
		ann := newVectorANNWithOptions(sf.opt, len(old))
		for did, vec := range old {
			if ix.isDeletedOrExpiredLocked(did) {
				continue
			}
			if sf.opt.Dim > 0 && len(vec) != sf.opt.Dim {
				return fmt.Errorf("vector field %q stored dimension mismatch for doc %d: got %d want %d", field, did, len(vec), sf.opt.Dim)
			}
			copyVec := append([]float64(nil), vec...)
			live[did] = copyVec
			ann.Add(did, copyVec)
		}
		newVectors[field] = live
		newANNs[field] = ann
	}
	ix.vectors = newVectors
	ix.anns = newANNs
	return nil
}

func (a *VectorANN) NodeCount() int {
	if a == nil {
		return 0
	}
	a.mu.RLock()
	n := len(a.nodes)
	a.mu.RUnlock()
	return n
}

func syncDir(path string) error {
	if path == "" {
		path = "."
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
