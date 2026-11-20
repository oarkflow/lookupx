package pkg

import (
	"bytes"
	"container/list"
	"encoding/gob"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const partitionPreviewRows = 128
const partitionPreviewMaxBytes = 256 << 10

type partitionPreviewRecord struct {
	ID    string
	DocID DocID
	Doc   Document
}
type partitionPreview struct {
	Version    int
	Generation uint64
	Schema     string
	Total      uint64
	Rows       []partitionPreviewRecord
}

// A bounded browse page is independent of the search dictionaries. Saving only
// the first few records keeps a cold default page cheap even for large indexes.
func (ix *Index) writePartitionPreview(path string, generation uint64) error {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	if ix.hasTTL {
		return nil
	}
	p := partitionPreview{Version: 1, Generation: generation, Schema: summarySchema(ix.cfg)}
	used := 0
	ix.live.Each(func(id DocID) bool {
		if id == 0 || id >= ix.nextDocID || ix.isDeletedOrExpiredLocked(id) {
			return true
		}
		if len(p.Rows) >= partitionPreviewRows {
			return false
		}
		row := partitionPreviewRecord{ID: ix.externalID(id), DocID: id, Doc: ix.documentLocked(id)}
		b, err := json.Marshal(row)
		if err != nil || used+len(b) > partitionPreviewMaxBytes {
			return false
		}
		p.Rows = append(p.Rows, row)
		used += len(b)
		return true
	})
	p.Total = uint64(ix.live.Count())
	if len(p.Rows) == 0 {
		return nil
	}
	var b bytes.Buffer
	if err := gob.NewEncoder(&b).Encode(p); err != nil {
		return nil
	} // optional for unsupported source value types
	if b.Len() > partitionPreviewMaxBytes {
		return nil
	}
	return writeSyncedFile(path, b.Bytes(), 0644)
}

type cachedPreview struct {
	path  string
	page  partitionPreview
	bytes int64
}

var servingPreviews = struct {
	sync.Mutex
	entries map[string]*list.Element
	lru     list.List
	bytes   int64
}{entries: make(map[string]*list.Element)}

func loadPartitionPreview(man PersistentManifest) (partitionPreview, bool) {
	path := filepath.Join(man.Path, "browse.gob")
	c := &servingPreviews
	c.Lock()
	defer c.Unlock()
	if e := c.entries[path]; e != nil {
		c.lru.MoveToFront(e)
		return e.Value.(*cachedPreview).page, true
	}
	f, err := os.Open(path)
	if err != nil {
		return partitionPreview{}, false
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, partitionPreviewMaxBytes+1))
	if err != nil || len(data) > partitionPreviewMaxBytes {
		return partitionPreview{}, false
	}
	var page partitionPreview
	if err = gob.NewDecoder(bytes.NewReader(data)).Decode(&page); err != nil || page.Version != 1 || page.Generation != man.Generation || page.Total != man.Docs || len(page.Rows) > partitionPreviewRows {
		return partitionPreview{}, false
	}
	weight := int64(len(data))*8 + 32<<10
	const budget = 8 << 20
	for c.bytes+weight > budget || c.lru.Len() >= 64 {
		e := c.lru.Back()
		if e == nil {
			break
		}
		old := e.Value.(*cachedPreview)
		delete(c.entries, old.path)
		c.bytes -= old.bytes
		c.lru.Remove(e)
	}
	c.entries[path] = c.lru.PushFront(&cachedPreview{path: path, page: page, bytes: weight})
	c.bytes += weight
	return page, true
}
func previewPartitionPage(man PersistentManifest, schema string, offset, limit int, withDocs bool) ([]Hit, bool) {
	p, ok := loadPartitionPreview(man)
	if !ok || p.Schema != schema || offset >= len(p.Rows) {
		return nil, false
	}
	end := offset + limit
	if end > len(p.Rows) {
		if uint64(len(p.Rows)) < p.Total {
			return nil, false
		}
		end = len(p.Rows)
	}
	hits := make([]Hit, 0, end-offset)
	for _, row := range p.Rows[offset:end] {
		hit := Hit{ID: row.ID, DocID: row.DocID, Score: 1}
		if withDocs {
			hit.Doc = cloneDoc(row.Doc)
		}
		hits = append(hits, hit)
	}
	return hits, true
}
