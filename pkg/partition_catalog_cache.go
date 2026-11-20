package pkg

import (
	"container/list"
	"encoding/json"
	"os"
	"sync"
)

// Atomic catalog replacements are visible on the next request. Keeping parsed
// summaries avoids rereading and decoding a large JSON catalog for each query.
var servingCatalogs = struct {
	sync.Mutex
	entries map[string]*list.Element
	lru     list.List
	bytes   int64
}{entries: make(map[string]*list.Element)}

type cachedCatalog struct {
	path    string
	info    os.FileInfo
	dataset PartitionDataset
	bytes   int64
}

func loadServingCatalog(path string) (PartitionDataset, error) {
	info, err := os.Stat(path)
	if err != nil {
		return PartitionDataset{}, err
	}
	c := &servingCatalogs
	c.Lock()
	defer c.Unlock()
	if e := c.entries[path]; e != nil {
		item := e.Value.(*cachedCatalog)
		if os.SameFile(info, item.info) && info.Size() == item.info.Size() && info.ModTime() == item.info.ModTime() {
			c.lru.MoveToFront(e)
			return item.dataset, nil
		}
		c.bytes -= item.bytes
		c.lru.Remove(e)
		delete(c.entries, path)
	}
	// Stat the opened file, rather than pairing a pre-open stat with bytes from
	// a replacement generation. The next request compares inode and timestamp.
	f, err := os.Open(path)
	if err != nil {
		return PartitionDataset{}, err
	}
	defer f.Close()
	info, err = f.Stat()
	if err != nil {
		return PartitionDataset{}, err
	}
	var dataset PartitionDataset
	if err = json.NewDecoder(f).Decode(&dataset); err != nil {
		return dataset, err
	}
	weight := info.Size() * 2
	const budget = 16 << 20
	if weight <= budget {
		for c.bytes+weight > budget || c.lru.Len() >= 64 {
			e := c.lru.Back()
			if e == nil {
				break
			}
			item := e.Value.(*cachedCatalog)
			c.bytes -= item.bytes
			delete(c.entries, item.path)
			c.lru.Remove(e)
		}
		c.entries[path] = c.lru.PushFront(&cachedCatalog{path: path, info: info, dataset: dataset, bytes: weight})
		c.bytes += weight
	}
	return dataset, nil
}
