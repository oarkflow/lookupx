package lookup

import (
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// CompositeField describes one field in a generic composite lookup key.
// It is intentionally generic: it can represent Dataset, product catalog, tenant
// lookups, orders, logs, claims, or any other exact lookup shape.
type CompositeField struct {
	Name string  `json:"name"`
	ID   FieldID `json:"-"`
}

// CompositeDefinition defines a generic exact-combination accelerator.
// Example: fields=["term","group_id","date_key"] creates a direct index for
// term=x&group_id=y&date_key=z without hardcoding domain-specific/Dataset semantics.
type CompositeDefinition struct {
	ID     string           `json:"id"`
	Fields []CompositeField `json:"fields"`
}

// CompositeIndex maps an encoded generic tuple to document postings. It uses a
// singleton fast path for high-cardinality tuples and promotes to bitmap only on
// the second matching row.
type CompositeIndex struct {
	mu      sync.RWMutex
	def     CompositeDefinition
	singles map[string]DocID
	bm      map[string]*Bitmap
}

func NewCompositeIndex(def CompositeDefinition) *CompositeIndex {
	if def.ID == "" {
		names := make([]string, 0, len(def.Fields))
		for _, f := range def.Fields {
			names = append(names, f.Name)
		}
		def.ID = strings.Join(names, "+")
	}
	return &CompositeIndex{def: def, singles: make(map[string]DocID, 1<<16), bm: make(map[string]*Bitmap, 1<<16)}
}

func (ix *Index) EnableComposite(def CompositeDefinition) *CompositeIndex {
	if def.ID == "" {
		names := make([]string, 0, len(def.Fields))
		for _, f := range def.Fields {
			names = append(names, f.Name)
		}
		def.ID = strings.Join(names, "+")
	}
	ix.mu.RLock()
	for i := range def.Fields {
		if def.Fields[i].ID == InvalidFieldID || def.Fields[i].ID == 0 && def.Fields[i].Name != "" {
			if id, ok := ix.fieldIDs[def.Fields[i].Name]; ok {
				def.Fields[i].ID = id
			}
		}
	}
	ix.mu.RUnlock()
	c := NewCompositeIndex(def)
	ex := extras(ix)
	ex.mu.Lock()
	if ex.genericComposites == nil {
		ex.genericComposites = map[string]*CompositeIndex{}
	}
	ex.genericComposites[def.ID] = c
	ex.mu.Unlock()
	return c
}

func (ix *Index) Composite(id string) *CompositeIndex {
	ex := extras(ix)
	ex.mu.RLock()
	defer ex.mu.RUnlock()
	if ex.genericComposites == nil {
		return nil
	}
	return ex.genericComposites[id]
}

func (ix *Index) CompositeIDs() []string {
	ex := extras(ix)
	ex.mu.RLock()
	defer ex.mu.RUnlock()
	out := make([]string, 0, len(ex.genericComposites))
	for id := range ex.genericComposites {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func encodeComposite(values []string) string {
	var b strings.Builder
	for i, v := range values {
		if i > 0 {
			b.WriteByte('\x1f')
		}
		b.WriteString(v)
	}
	return b.String()
}

func (c *CompositeIndex) AddValues(values []string, did DocID) {
	if c == nil || did == 0 || len(values) != len(c.def.Fields) {
		return
	}
	for _, v := range values {
		if v == "" {
			return
		}
	}
	key := encodeComposite(values)
	c.mu.Lock()
	defer c.mu.Unlock()
	if b := c.bm[key]; b != nil {
		b.Add(did)
		return
	}
	if prev := c.singles[key]; prev != 0 {
		b := NewBitmapCap(did + 1)
		b.Add(prev)
		b.Add(did)
		delete(c.singles, key)
		c.bm[key] = b
		return
	}
	c.singles[key] = did
}

func (c *CompositeIndex) BitmapValues(values []string) *Bitmap {
	if c == nil || len(values) != len(c.def.Fields) {
		return NewBitmap()
	}
	for _, v := range values {
		if v == "" {
			return NewBitmap()
		}
	}
	key := encodeComposite(values)
	c.mu.RLock()
	defer c.mu.RUnlock()
	if did := c.singles[key]; did != 0 {
		b := NewBitmapCap(did + 1)
		b.Add(did)
		return b
	}
	if b := c.bm[key]; b != nil {
		return b.Clone()
	}
	return NewBitmap()
}

func (c *CompositeIndex) Search(ix *Index, values []string, limit int, dst []Hit) []Hit {
	dst = dst[:0]
	if ix == nil || c == nil {
		return dst
	}
	bm := c.BitmapValues(values)
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

// CompositeLookupQuery is the generic exact tuple query. It works for any index
// where EnableComposite has been called with the same composite ID.
type CompositeLookupQuery struct {
	CompositeID string
	Values      []string
}

func (q CompositeLookupQuery) eval(ix *Index) *Bitmap {
	if ix == nil {
		return NewBitmap()
	}
	c := ix.Composite(q.CompositeID)
	if c == nil {
		return NewBitmap()
	}
	return c.BitmapValues(q.Values)
}

func (ix *Index) CompositeLookup(compositeID string, values []string, limit int, dst []Hit) []Hit {
	c := ix.Composite(compositeID)
	if c == nil {
		return dst[:0]
	}
	return c.Search(ix, values, limit, dst)
}

func (ix *Index) updateGenericCompositesFromSource(rec *SourceRecord, did DocID) {
	ex := extras(ix)
	ex.mu.RLock()
	comps := make([]*CompositeIndex, 0, len(ex.genericComposites))
	for _, c := range ex.genericComposites {
		comps = append(comps, c)
	}
	ex.mu.RUnlock()
	if len(comps) == 0 {
		return
	}
	valsByField := make(map[FieldID]string, len(rec.Values))
	for i := range rec.Values {
		v := rec.Values[i]
		switch v.Kind {
		case ValueKeyword, ValueText:
			valsByField[v.Field] = v.String
		case ValueNumber, ValueTimeUnix:
			valsByField[v.Field] = strconv.FormatFloat(v.Number, 'f', -1, 64)
		}
	}
	for _, c := range comps {
		vals := make([]string, len(c.def.Fields))
		ok := true
		for i, f := range c.def.Fields {
			v := valsByField[f.ID]
			if v == "" {
				ok = false
				break
			}
			vals[i] = v
		}
		if ok {
			c.AddValues(vals, did)
		}
	}
}

// ParseCompositeURLQuery builds a CompositeLookupQuery from URL/query-string
// values using the composite field names. It is generic and is the preferred
// replacement for domain-specific parsers.
func (ix *Index) ParseCompositeURLQuery(compositeID, raw string) Query {
	c := ix.Composite(compositeID)
	if c == nil {
		return MatchAll{}
	}
	vals, _ := url.ParseQuery(raw)
	out := make([]string, len(c.def.Fields))
	for i, f := range c.def.Fields {
		out[i] = vals.Get(f.Name)
	}
	return CompositeLookupQuery{CompositeID: compositeID, Values: out}
}

// GenericLookupSchema creates a schema from arbitrary field specs.
func GenericLookupSchema(fields map[string]FieldKind, prefixFields ...string) Schema {
	s := Schema{Fields: map[string]FieldOptions{}}
	prefix := map[string]bool{}
	for _, p := range prefixFields {
		prefix[p] = true
	}
	for name, kind := range fields {
		opt := FieldOptions{Kind: kind, Indexed: true, Lookup: true, Lowercase: true}
		if kind == FieldText {
			opt.Phrase = true
		}
		if prefix[name] {
			opt.Prefix = true
		}
		s.Fields[name] = opt
	}
	return s
}
