package pkg

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
)

// A lease pins a partition until its search has copied the result records.
// Cached generations are immutable, keyed by generation path AND schema, and
// evicted only while idle. Large partitions can be used without being retained.
type partitionCacheKey struct {
	store      reflect.Type
	path       string
	generation uint64
	config     [32]byte
}

type partitionCacheEntry struct {
	key     partitionCacheKey
	ix      *Index
	bytes   int64
	refs    int
	element *list.Element
}
type partitionLoad struct {
	done chan struct{}
	err  error
}

type partitionCache struct {
	mu              sync.Mutex
	entries         map[partitionCacheKey]*partitionCacheEntry
	loading         map[partitionCacheKey]*partitionLoad
	lru             list.List
	bytes, maxBytes int64
	hits, misses    atomic.Uint64
}

var servingPartitions = partitionCache{entries: make(map[partitionCacheKey]*partitionCacheEntry), maxBytes: 256 << 20, loading: make(map[partitionCacheKey]*partitionLoad)}

// ConfigurePartitionCache sets the process-wide retention budget. It does not
// count active query workspaces or the active ingestion partition.
func ConfigurePartitionCache(bytes int64) error {
	if bytes < 0 {
		return fmt.Errorf("partition cache bytes cannot be negative")
	}
	c := &servingPartitions
	c.mu.Lock()
	defer c.mu.Unlock()
	c.maxBytes = bytes
	c.evictLocked(0)
	return nil
}
func (c *partitionCache) evictLocked(need int64) {
	for e := c.lru.Back(); e != nil && c.bytes+need > c.maxBytes; {
		prev := e.Prev()
		item := e.Value.(*partitionCacheEntry)
		if item.refs == 0 {
			delete(c.entries, item.key)
			c.lru.Remove(e)
			c.bytes -= item.bytes
			_ = item.ix.Close()
		}
		e = prev
	}
}
func (c *partitionCache) acquire(ctx context.Context, store PersistentStore, man PersistentManifest, cfg Config, signature [32]byte) (*Index, func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	key := partitionCacheKey{store: reflect.TypeOf(store), path: man.Path, generation: man.Generation, config: signature}
	// Cold opens for one generation share a notification. Do not hold the
	// cache lock during disk I/O: a cold index must not block cached lookups.
	var pending *partitionLoad
	for {
		c.mu.Lock()
		if item := c.entries[key]; item != nil {
			item.refs++
			c.lru.MoveToFront(item.element)
			c.hits.Add(1)
			c.mu.Unlock()
			return item.ix, func() { c.release(item) }, nil
		}
		if existing := c.loading[key]; existing != nil {
			c.mu.Unlock()
			select {
			case <-existing.done:
				if existing.err != nil {
					return nil, nil, existing.err
				}
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			}
			continue
		}
		pending = &partitionLoad{done: make(chan struct{})}
		if c.loading == nil {
			c.loading = make(map[partitionCacheKey]*partitionLoad)
		}
		c.loading[key] = pending
		c.mu.Unlock()
		break
	}
	c.misses.Add(1)
	var ix *Index
	var weight int64
	var err error
	if _, ok := store.(FileMMapSegmentStore); ok {
		ix, weight, err = openServingPartition(ctx, store, man, cfg)
	} else {
		ix, _, err = store.LoadIndex(ctx, man.IndexID, cfg)
		weight = legacyPartitionWeight(man)
	}
	c.mu.Lock()
	defer func() { pending.err = err; delete(c.loading, key); close(pending.done); c.mu.Unlock() }()
	if err != nil {
		return nil, nil, err
	}
	if err = ctx.Err(); err != nil {
		_ = ix.Close()
		return nil, nil, err
	}
	c.evictLocked(weight)
	item := &partitionCacheEntry{key: key, ix: ix, bytes: weight, refs: 1}
	if weight <= c.maxBytes && c.bytes+weight <= c.maxBytes {
		item.element = c.lru.PushFront(item)
		c.entries[key] = item
		c.bytes += weight
	}
	return ix, func() { c.release(item) }, nil
}
func (c *partitionCache) release(item *partitionCacheEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	item.refs--
	if item.refs == 0 && item.element == nil {
		_ = item.ix.Close()
	}
	c.evictLocked(0)
}
func legacyPartitionWeight(man PersistentManifest) int64 {
	for _, name := range []string{"segment.dat", "index.gob"} {
		if st, err := os.Stat(filepath.Join(man.Path, name)); err == nil {
			return st.Size()*16 + 1<<20
		}
	}
	return 1 << 62 // unknown sizes are never retained
}
func openServingPartition(ctx context.Context, store PersistentStore, man PersistentManifest, cfg Config) (*Index, int64, error) {
	path := filepath.Join(man.Path, "segment.dat")
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	var magic [8]byte
	_, err = f.Read(magic[:])
	_ = f.Close()
	if err != nil {
		return nil, 0, err
	}
	if !isMappedMagic(string(magic[:])) {
		ix, _, err := store.LoadIndex(ctx, man.IndexID, cfg)
		return ix, legacyPartitionWeight(man), err
	}
	data, release, err := mapSegmentFile(path)
	if err != nil {
		return nil, 0, err
	}
	var dump persistentDump
	columns := make(map[string]mappedStringColumn)
	var ids mappedStringColumn
	if err = decodeMappedColumns(data, &dump, true, columns, &ids); err != nil {
		_ = release()
		return nil, 0, err
	}
	// Immutable serving never needs the external-ID update map or initial build
	// capacity. Avoid allocating those alongside already-mapped columns.
	dump.Config.AppendOnly = true
	dump.Config.InitialCapacity = 0
	if cfg.Schema.Fields != nil {
		dump.Config.Schema = cfg.Schema
	}
	ix, err := New(dump.Config)
	if err != nil {
		_ = release()
		return nil, 0, err
	}
	if err = ix.restorePersistentDump(&dump); err != nil {
		_ = ix.Close()
		_ = release()
		return nil, 0, err
	}
	ix.mappedStrings = columns
	ix.mappedIDs = ids
	for _, field := range ix.fieldList {
		if field.opt.IndexAlias != "" {
			if col, ok := columns[field.opt.IndexAlias]; ok {
				columns[field.name] = col
			}
		}
		if field.opt.StoredAlias != "" {
			if col, ok := columns[field.opt.StoredAlias]; ok {
				columns[field.name] = col
			}
		}
	}
	for name := range columns {
		ix.strings[name] = nil
	}
	ix.mappedRelease = release
	// File-backed pages are reclaimable by the OS. Charge retained Go metadata
	// rather than the virtual mapping size: charging the whole file makes a
	// small cache thrash even when all dictionary tables fit comfortably.
	weight := mappedMetadataBytes(&dump) + int64(len(ids.offsets))*4
	for name, col := range columns {
		if cfg.Schema.Fields[name].StoredAlias == "" && cfg.Schema.Fields[name].IndexAlias == "" {
			weight += int64(len(col.offsets)) * 4
		}
	}
	if !segmentMappingUsesOS {
		weight += int64(len(data))
	}
	return ix, weight, nil
}

// Mapped strings must not escape a lease: an idle generation may be unmapped
// immediately when another partition needs space in the cache.
func detachPartitionHits(hits []Hit) {
	for i := range hits {
		h := &hits[i]
		h.ID = strings.Clone(h.ID)
		h.Explanation = strings.Clone(h.Explanation)
		for j := range h.Fields {
			h.Fields[j] = strings.Clone(h.Fields[j])
		}
		if h.Doc != nil {
			h.Doc = detachMappedValue(h.Doc).(Document)
		}
	}
}
func detachMappedValue(v any) any {
	switch x := v.(type) {
	case string:
		return strings.Clone(x)
	case Document:
		out := make(Document, len(x))
		for k, v := range x {
			out[strings.Clone(k)] = detachMappedValue(v)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, v := range x {
			out[strings.Clone(k)] = detachMappedValue(v)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, v := range x {
			out[i] = detachMappedValue(v)
		}
		return out
	case []string:
		out := make([]string, len(x))
		for i, v := range x {
			out[i] = strings.Clone(v)
		}
		return out
	default:
		return v
	}
}

// Conservative accounting for dictionary buckets, bitmap wrappers and slice
// headers. Compact columns charge four bytes per row separately. The bitmap
// charge covers its 48-byte wrapper plus dictionary slots and key headers.
// Numeric/posting arrays and string payloads remain file-backed.
func mappedMetadataBytes(d *persistentDump) int64 {
	n := int64(128<<10) + int64(len(d.DocToExt))*16 + int64(len(d.Docs))*1024
	for _, col := range d.StringColumns {
		n += int64(len(col)) * 16
	}
	for _, col := range d.Strings {
		n += int64(len(col)) * 96
	}
	for _, field := range d.Fields {
		n += int64(len(field.Terms)+len(field.TermsSparse)+len(field.Prefix)+len(field.PrefixSparse)+len(field.Suffix)+len(field.SuffixSparse)+len(field.Ngram)+len(field.NgramSparse)) * 96
		n += int64(len(field.TermOne)+len(field.PrefixOne)+len(field.SuffixOne)+len(field.NgramOne)+len(field.Unique)) * 96
		for _, positions := range field.Positions {
			n += 128 + int64(len(positions))*96
		}
		for _, tf := range field.ExtraTF {
			n += 128 + int64(len(tf))*64
		}
	}
	for _, col := range d.IP4 {
		n += int64(len(col)) * 64
	}
	for _, col := range d.Vectors {
		n += int64(len(col)) * 256
	}
	if c := d.Composite; c != nil {
		n += int64(len(c.Terms)+len(c.Sources))*96 + int64(len(c.Postings)+len(c.Sparse))*96 + int64(len(c.Singles))*96
	}
	return n
}

func partitionConfigSignature(cfg Config) [32]byte {
	raw, _ := json.Marshal(cfg)
	return sha256.Sum256(raw)
}
