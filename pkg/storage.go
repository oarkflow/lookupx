package pkg

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ValueKind describes how a source column is written into the index.
type ValueKind uint8

const (
	ValueKeyword ValueKind = iota
	ValueText
	ValueNumber
	ValueTimeUnix
	ValueVector
)

// SourceValue is a typed reusable cell emitted by a Source.
type SourceValue struct {
	Field      FieldID
	Kind       ValueKind
	String     string
	Number     float64
	Vector     []float64
	Source     any // original datasource-facing value used in returned documents
	Normalized bool
}

// SourceRecord is reused by cursors to avoid per-row allocations.
type SourceRecord struct {
	ID     string
	Seq    uint64
	Values []SourceValue
}

func (r *SourceRecord) Reset() {
	r.ID = ""
	r.Seq = 0
	r.Values = r.Values[:0]
}
func (r *SourceRecord) AddKeyword(fid FieldID, value string, normalized bool) {
	if value == "" {
		return
	}
	r.Values = append(r.Values, SourceValue{Field: fid, Kind: ValueKeyword, String: value, Normalized: normalized})
}
func (r *SourceRecord) AddText(fid FieldID, value string, normalized bool) {
	if value == "" {
		return
	}
	r.Values = append(r.Values, SourceValue{Field: fid, Kind: ValueText, String: value, Normalized: normalized})
}
func (r *SourceRecord) AddNumber(fid FieldID, value float64) {
	r.Values = append(r.Values, SourceValue{Field: fid, Kind: ValueNumber, Number: value})
}
func (r *SourceRecord) AddUnixTime(fid FieldID, value int64) {
	r.Values = append(r.Values, SourceValue{Field: fid, Kind: ValueTimeUnix, Number: float64(value)})
}
func (r *SourceRecord) AddVector(fid FieldID, value []float64) {
	if len(value) == 0 {
		return
	}
	r.Values = append(r.Values, SourceValue{Field: fid, Kind: ValueVector, Vector: value})
}

// Source streams records from any backing store without loading the full dataset.
type Source interface {
	Open(ctx context.Context) (Cursor, error)
}

type Cursor interface {
	Next(ctx context.Context, dst *SourceRecord) bool
	Err() error
	Close() error
}

// CheckpointStore tracks ingestion progress for resumable database/file imports.
type CheckpointStore interface {
	Load(ctx context.Context, name string) (uint64, error)
	Save(ctx context.Context, name string, seq uint64) error
	Reset(ctx context.Context, name string) error
}

type MemoryCheckpoint struct {
	mu   sync.Mutex
	vals map[string]uint64
}

func NewMemoryCheckpoint() *MemoryCheckpoint { return &MemoryCheckpoint{vals: map[string]uint64{}} }
func (m *MemoryCheckpoint) Load(ctx context.Context, name string) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.vals[name], nil
}
func (m *MemoryCheckpoint) Save(ctx context.Context, name string, seq uint64) error {
	m.mu.Lock()
	m.vals[name] = seq
	m.mu.Unlock()
	return nil
}
func (m *MemoryCheckpoint) Reset(ctx context.Context, name string) error {
	m.mu.Lock()
	delete(m.vals, name)
	m.mu.Unlock()
	return nil
}

type FileCheckpoint struct{ Path string }

func (f FileCheckpoint) Load(ctx context.Context, name string) (uint64, error) {
	b, err := os.ReadFile(f.Path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var all map[string]uint64
	if err := json.Unmarshal(b, &all); err != nil {
		return 0, err
	}
	return all[name], nil
}
func (f FileCheckpoint) Save(ctx context.Context, name string, seq uint64) error {
	all := map[string]uint64{}
	if b, err := os.ReadFile(f.Path); err == nil {
		_ = json.Unmarshal(b, &all)
	}
	all[name] = seq
	b, err := json.MarshalIndent(all, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir(f.Path), 0755); err != nil {
		return err
	}
	return os.WriteFile(f.Path, b, 0644)
}
func (f FileCheckpoint) Reset(ctx context.Context, name string) error {
	all := map[string]uint64{}
	if b, err := os.ReadFile(f.Path); err == nil {
		_ = json.Unmarshal(b, &all)
	}
	delete(all, name)
	b, err := json.MarshalIndent(all, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir(f.Path), 0755); err != nil {
		return err
	}
	return os.WriteFile(f.Path, b, 0644)
}

type BulkOptions struct {
	Name            string
	BatchSize       int
	CheckpointEvery int
	Checkpoint      CheckpointStore
	Resume          bool
	SkipBadRecords  bool
	Progress        func(BulkProgress)
}

type BulkProgress struct {
	SourceName string
	Seen       uint64
	Indexed    uint64
	Skipped    uint64
	LastSeq    uint64
	StartedAt  time.Time
	Took       time.Duration
}

type BulkStats struct {
	SourceName string        `json:"source_name"`
	Seen       uint64        `json:"seen"`
	Indexed    uint64        `json:"indexed"`
	Skipped    uint64        `json:"skipped"`
	LastSeq    uint64        `json:"last_seq"`
	Took       time.Duration `json:"took"`
}

// IndexFrom streams a Source into the index. It is safe for very large datasets
// because it reuses one SourceRecord and never buffers all rows in memory.
func (ix *Index) IndexFrom(ctx context.Context, src Source, opt BulkOptions) (BulkStats, error) {
	if opt.Name == "" {
		opt.Name = "source"
	}
	if opt.BatchSize <= 0 {
		opt.BatchSize = 4096
	}
	if opt.CheckpointEvery <= 0 {
		opt.CheckpointEvery = opt.BatchSize
	}
	started := time.Now()
	var resumeAfter uint64
	if opt.Resume && opt.Checkpoint != nil {
		v, err := opt.Checkpoint.Load(ctx, opt.Name)
		if err != nil {
			return BulkStats{}, err
		}
		resumeAfter = v
	}
	cur, err := src.Open(ctx)
	if err != nil {
		return BulkStats{}, err
	}
	defer cur.Close()

	rec := SourceRecord{Values: make([]SourceValue, 0, len(ix.fieldList))}
	batch := make([]SourceRecord, 0, opt.BatchSize)
	var stats BulkStats
	stats.SourceName = opt.Name
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		indexed, skipped, lastSeq, err := ix.indexSourceBatch(batch, opt.SkipBadRecords)
		stats.Indexed += indexed
		stats.Skipped += skipped
		if lastSeq > 0 {
			stats.LastSeq = lastSeq
		}
		for i := range batch {
			batch[i].Values = batch[i].Values[:0]
		}
		batch = batch[:0]
		if err != nil {
			return err
		}
		if opt.Checkpoint != nil && stats.LastSeq > 0 && stats.Indexed%uint64(opt.CheckpointEvery) == 0 {
			if err := opt.Checkpoint.Save(ctx, opt.Name, stats.LastSeq); err != nil {
				return err
			}
		}
		if opt.Progress != nil {
			opt.Progress(BulkProgress{SourceName: opt.Name, Seen: stats.Seen, Indexed: stats.Indexed, Skipped: stats.Skipped, LastSeq: stats.LastSeq, StartedAt: started, Took: time.Since(started)})
		}
		return nil
	}

	for cur.Next(ctx, &rec) {
		stats.Seen++
		if rec.Seq == 0 {
			rec.Seq = stats.Seen
		}
		if resumeAfter > 0 && rec.Seq <= resumeAfter {
			rec.Reset()
			continue
		}
		if rec.ID == "" {
			stats.Skipped++
			if !opt.SkipBadRecords {
				return stats, errors.New("source record missing id")
			}
			rec.Reset()
			continue
		}
		var slot SourceRecord
		if len(batch) < cap(batch) {
			batch = append(batch, SourceRecord{})
			slot = batch[len(batch)-1]
		} else {
			batch = append(batch, SourceRecord{})
			slot = batch[len(batch)-1]
		}
		slot.ID = rec.ID
		slot.Seq = rec.Seq
		slot.Values = append(slot.Values[:0], rec.Values...)
		batch[len(batch)-1] = slot
		if len(batch) >= opt.BatchSize {
			if err := flush(); err != nil {
				return stats, err
			}
		}
		rec.Reset()
	}
	if err := cur.Err(); err != nil {
		return stats, err
	}
	if err := flush(); err != nil {
		return stats, err
	}
	if opt.Checkpoint != nil && stats.LastSeq > 0 {
		if err := opt.Checkpoint.Save(ctx, opt.Name, stats.LastSeq); err != nil {
			return stats, err
		}
	}
	stats.Took = time.Since(started)
	return stats, nil
}

func (ix *Index) indexSourceBatch(batch []SourceRecord, skipBad bool) (indexed, skipped, lastSeq uint64, err error) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	for i := range batch {
		rec := &batch[i]
		if rec.ID == "" {
			skipped++
			if !skipBad {
				return indexed, skipped, lastSeq, errors.New("source record missing id")
			}
			continue
		}
		did := ix.reserveDocLocked(rec.ID)
		w := RowWriter{ix: ix, did: did}
		for j := range rec.Values {
			v := &rec.Values[j]
			switch v.Kind {
			case ValueKeyword:
				if v.Normalized {
					w.KeywordNormalized(v.Field, v.String)
				} else {
					w.Keyword(v.Field, v.String)
				}
			case ValueText:
				if v.Normalized {
					w.TextNormalized(v.Field, v.String)
				} else {
					w.Text(v.Field, v.String)
				}
			case ValueNumber, ValueTimeUnix:
				w.Float(v.Field, v.Number)
			case ValueVector:
				if int(v.Field) < len(ix.fieldList) {
					name := ix.fieldList[v.Field].name
					ix.fieldList[v.Field].fi.exists.AddUnsafe(w.did)
					ix.addVectorLocked(name, w.did, v.Vector)
				}
			}
			if w.err != nil {
				break
			}
		}
		if w.err != nil {
			skipped++
			if !skipBad {
				return indexed, skipped, lastSeq, w.err
			}
			continue
		}
		if !ix.cfg.DisableSource {
			ix.docs[did] = ix.sourceRecordDoc(rec)
		}
		ix.updateTupleCompositeFromSource(rec, did)
		ix.updateGenericCompositesFromSource(rec, did)
		indexed++
		lastSeq = rec.Seq
	}
	return indexed, skipped, lastSeq, nil
}

// sourceRecordDoc reconstructs a best-effort Document from a SourceRecord's
// typed values, keyed by schema field name. Bulk sources (SQL/CSV/JSONL/
// slice) emit typed SourceValue cells rather than a Document map, so without
// this, with_docs search/lookup would silently return nothing for any
// bulk-ingested record even though the fields are indexed and searchable.
func (ix *Index) sourceRecordDoc(rec *SourceRecord) Document {
	doc := make(Document, len(ix.fieldList))
	for i := range rec.Values {
		v := &rec.Values[i]
		if int(v.Field) >= len(ix.fieldList) {
			continue
		}
		name := ix.fieldList[v.Field].name
		if v.Source != nil {
			doc[name] = v.Source
			continue
		}
		switch v.Kind {
		case ValueKeyword, ValueText:
			doc[name] = v.String
		case ValueNumber, ValueTimeUnix:
			doc[name] = v.Number
		case ValueVector:
			doc[name] = v.Vector
		}
	}
	// Preserve the SELECT shape without overwriting populated values. Sparse
	// NULL columns cost one write; non-NULL columns are written only once.
	for i := range ix.fieldList {
		name := ix.fieldList[i].name
		if _, exists := doc[name]; !exists {
			doc[name] = nil
		}
	}
	return doc
}

func (ix *Index) indexSourceRecord(rec *SourceRecord) error {
	w := ix.BeginFast(rec.ID)
	did := w.did
	for i := range rec.Values {
		v := &rec.Values[i]
		switch v.Kind {
		case ValueKeyword:
			if v.Normalized {
				w.KeywordNormalized(v.Field, v.String)
			} else {
				w.Keyword(v.Field, v.String)
			}
		case ValueText:
			if v.Normalized {
				w.TextNormalized(v.Field, v.String)
			} else {
				w.Text(v.Field, v.String)
			}
		case ValueNumber, ValueTimeUnix:
			w.Float(v.Field, v.Number)
		case ValueVector:
			if int(v.Field) < len(ix.fieldList) {
				name := ix.fieldList[v.Field].name
				ix.fieldList[v.Field].fi.exists.AddUnsafe(w.did)
				ix.addVectorLocked(name, w.did, v.Vector)
			}
		}
	}
	if w.err == nil && !ix.cfg.DisableSource {
		ix.docs[did] = ix.sourceRecordDoc(rec)
	}
	err := w.Commit()
	if err == nil {
		ix.updateTupleCompositeFromSource(rec, did)
		ix.updateGenericCompositesFromSource(rec, did)
	}
	return err
}

// SliceSource is useful for tests and small embedded imports.
type SliceSource struct{ Records []SourceRecord }

func (s SliceSource) Open(ctx context.Context) (Cursor, error) {
	return &sliceCursor{records: s.Records}, nil
}

type sliceCursor struct {
	records []SourceRecord
	i       int
	err     error
}

func (c *sliceCursor) Next(ctx context.Context, dst *SourceRecord) bool {
	if c.i >= len(c.records) {
		return false
	}
	r := c.records[c.i]
	c.i++
	dst.Reset()
	dst.ID = r.ID
	dst.Seq = r.Seq
	dst.Values = append(dst.Values, r.Values...)
	return true
}
func (c *sliceCursor) Err() error   { return c.err }
func (c *sliceCursor) Close() error { return nil }

// ChannelSource streams records from producer goroutines.
type ChannelSource struct{ C <-chan SourceRecord }

func (s ChannelSource) Open(ctx context.Context) (Cursor, error) { return &channelCursor{c: s.C}, nil }

type channelCursor struct {
	c   <-chan SourceRecord
	err error
}

func (c *channelCursor) Next(ctx context.Context, dst *SourceRecord) bool {
	select {
	case <-ctx.Done():
		c.err = ctx.Err()
		return false
	case r, ok := <-c.c:
		if !ok {
			return false
		}
		dst.Reset()
		dst.ID = r.ID
		dst.Seq = r.Seq
		dst.Values = append(dst.Values, r.Values...)
		return true
	}
}
func (c *channelCursor) Err() error   { return c.err }
func (c *channelCursor) Close() error { return nil }

// SQLColumn binds a database query column to an index field.
type SQLColumn struct {
	Column     string
	Field      FieldID
	Kind       ValueKind
	Normalized bool
	Layout     string // optional time layout for ValueTimeUnix
}

type SQLSource struct {
	DB        *sql.DB
	Query     string
	Args      []any
	IDColumn  string
	SeqColumn string
	Columns   []SQLColumn
	FetchSize int
}

func (s SQLSource) Open(ctx context.Context) (Cursor, error) {
	if s.DB == nil {
		return nil, errors.New("nil sql DB")
	}
	rows, err := s.DB.QueryContext(ctx, s.Query, s.Args...)
	if err != nil {
		return nil, err
	}
	cols, err := rows.Columns()
	if err != nil {
		rows.Close()
		return nil, err
	}
	colIndex := map[string]int{}
	for i, c := range cols {
		colIndex[strings.ToLower(c)] = i
	}
	idIdx, ok := colIndex[strings.ToLower(s.IDColumn)]
	if !ok {
		rows.Close()
		return nil, fmt.Errorf("id column %q not found", s.IDColumn)
	}
	seqIdx := -1
	if s.SeqColumn != "" {
		if idx, ok := colIndex[strings.ToLower(s.SeqColumn)]; ok {
			seqIdx = idx
		}
	}
	bindings := make([]sqlBinding, 0, len(s.Columns))
	for _, c := range s.Columns {
		idx, ok := colIndex[strings.ToLower(c.Column)]
		if !ok {
			rows.Close()
			return nil, fmt.Errorf("column %q not found", c.Column)
		}
		bindings = append(bindings, sqlBinding{idx: idx, col: c})
	}
	vals := make([]sql.NullString, len(cols))
	dest := make([]any, len(cols))
	for i := range vals {
		dest[i] = &vals[i]
	}
	return &sqlCursor{rows: rows, vals: vals, dest: dest, idIdx: idIdx, seqIdx: seqIdx, bindings: bindings}, nil
}

type sqlBinding struct {
	idx int
	col SQLColumn
}
type sqlCursor struct {
	rows          *sql.Rows
	vals          []sql.NullString
	dest          []any
	idIdx, seqIdx int
	bindings      []sqlBinding
	n             uint64
	err           error
}

func (c *sqlCursor) Next(ctx context.Context, dst *SourceRecord) bool {
	if !c.rows.Next() {
		return false
	}
	if err := c.rows.Scan(c.dest...); err != nil {
		c.err = err
		return false
	}
	c.n++
	dst.Reset()
	if c.vals[c.idIdx].Valid {
		dst.ID = c.vals[c.idIdx].String
	}
	dst.Seq = c.n
	if c.seqIdx >= 0 && c.vals[c.seqIdx].Valid {
		if u, err := strconv.ParseUint(c.vals[c.seqIdx].String, 10, 64); err == nil {
			dst.Seq = u
		}
	}
	for _, b := range c.bindings {
		v := c.vals[b.idx]
		if !v.Valid || v.String == "" {
			continue
		}
		before := len(dst.Values)
		switch b.col.Kind {
		case ValueKeyword:
			dst.AddKeyword(b.col.Field, v.String, b.col.Normalized)
		case ValueText:
			dst.AddText(b.col.Field, v.String, b.col.Normalized)
		case ValueNumber:
			f, err := strconv.ParseFloat(v.String, 64)
			if err != nil {
				c.err = err
				return false
			}
			dst.AddNumber(b.col.Field, f)
		case ValueTimeUnix:
			unix, err := parseSourceTime(v.String, b.col.Layout)
			if err != nil {
				c.err = fmt.Errorf("column %q: %w", b.col.Column, err)
				return false
			}
			dst.AddUnixTime(b.col.Field, unix)
		}
		if len(dst.Values) > before {
			dst.Values[len(dst.Values)-1].Source = v.String
		}
	}
	return true
}
func (c *sqlCursor) Err() error {
	if c.err != nil {
		return c.err
	}
	return c.rows.Err()
}
func (c *sqlCursor) Close() error { return c.rows.Close() }

// SQLTableQuery builds a deterministic keyset-paginated query for large tables.
type SQLTableQuery struct {
	Table       string
	Columns     []string
	Where       string
	OrderColumn string
	LastValue   any
	Limit       int
}

func (q SQLTableQuery) SQL() (string, []any) {
	cols := "*"
	if len(q.Columns) > 0 {
		cols = strings.Join(q.Columns, ", ")
	}
	order := q.OrderColumn
	if order == "" {
		order = "id"
	}
	args := []any{}
	where := strings.TrimSpace(q.Where)
	if q.LastValue != nil {
		if where != "" {
			where += " AND "
		}
		where += order + " > ?"
		args = append(args, q.LastValue)
	}
	if where != "" {
		where = " WHERE " + where
	}
	limit := q.Limit
	if limit <= 0 {
		limit = 10000
	}
	return fmt.Sprintf("SELECT %s FROM %s%s ORDER BY %s ASC LIMIT %d", cols, q.Table, where, order, limit), args
}

// JSONLSource indexes newline-delimited JSON objects. It is flexible, not the fastest path.
type JSONLSource struct {
	R        io.Reader
	IDField  string
	Bindings []JSONBinding
}
type JSONBinding struct {
	FieldName  string
	Field      FieldID
	Kind       ValueKind
	Normalized bool
	Layout     string // optional time layout for ValueTimeUnix; defaults to RFC3339Nano
}

func (s JSONLSource) Open(ctx context.Context) (Cursor, error) {
	return &jsonlCursor{sc: bufio.NewScanner(s.R), idField: s.IDField, bindings: s.Bindings, seq: 0}, nil
}

type jsonlCursor struct {
	sc       *bufio.Scanner
	idField  string
	bindings []JSONBinding
	seq      uint64
	err      error
}

func (c *jsonlCursor) Next(ctx context.Context, dst *SourceRecord) bool {
	if !c.sc.Scan() {
		return false
	}
	var m map[string]any
	if err := json.Unmarshal(c.sc.Bytes(), &m); err != nil {
		c.err = err
		return false
	}
	c.seq++
	dst.Reset()
	dst.Seq = c.seq
	if v, ok := m[c.idField].(string); ok {
		dst.ID = v
	}
	for _, b := range c.bindings {
		v, ok := m[b.FieldName]
		if !ok || v == nil {
			continue
		}
		before := len(dst.Values)
		switch b.Kind {
		case ValueKeyword:
			dst.AddKeyword(b.Field, fmt.Sprint(v), b.Normalized)
		case ValueText:
			dst.AddText(b.Field, fmt.Sprint(v), b.Normalized)
		case ValueNumber:
			switch x := v.(type) {
			case float64:
				dst.AddNumber(b.Field, x)
			case int:
				dst.AddNumber(b.Field, float64(x))
			case string:
				if f, err := strconv.ParseFloat(x, 64); err == nil {
					dst.AddNumber(b.Field, f)
				}
			}
		case ValueTimeUnix:
			switch x := v.(type) {
			case string:
				unix, err := parseSourceTime(x, b.Layout)
				if err != nil {
					c.err = fmt.Errorf("field %q: %w", b.FieldName, err)
					return false
				}
				dst.AddUnixTime(b.Field, unix)
			case float64:
				dst.AddUnixTime(b.Field, int64(x))
			}
		}
		if len(dst.Values) > before {
			dst.Values[len(dst.Values)-1].Source = v
		}
	}
	return true
}
func (c *jsonlCursor) Err() error {
	if c.err != nil {
		return c.err
	}
	return c.sc.Err()
}
func (c *jsonlCursor) Close() error { return nil }

// CSVSource streams CSV data with a header row.
type CSVSource struct {
	R        io.Reader
	IDColumn string
	Bindings []CSVBinding
}
type CSVBinding struct {
	Column     string
	Field      FieldID
	Kind       ValueKind
	Normalized bool
	Layout     string
}

func (s CSVSource) Open(ctx context.Context) (Cursor, error) {
	r := csv.NewReader(s.R)
	header, err := r.Read()
	if err != nil {
		return nil, err
	}
	idx := map[string]int{}
	for i, h := range header {
		idx[strings.ToLower(h)] = i
	}
	idIdx, ok := idx[strings.ToLower(s.IDColumn)]
	if !ok {
		return nil, fmt.Errorf("id column %q not found", s.IDColumn)
	}
	bs := make([]csvBinding, 0, len(s.Bindings))
	for _, b := range s.Bindings {
		i, ok := idx[strings.ToLower(b.Column)]
		if !ok {
			return nil, fmt.Errorf("column %q not found", b.Column)
		}
		bs = append(bs, csvBinding{idx: i, b: b})
	}
	return &csvCursor{r: r, idIdx: idIdx, bindings: bs}, nil
}

type csvBinding struct {
	idx int
	b   CSVBinding
}
type csvCursor struct {
	r        *csv.Reader
	idIdx    int
	bindings []csvBinding
	seq      uint64
	err      error
}

func (c *csvCursor) Next(ctx context.Context, dst *SourceRecord) bool {
	row, err := c.r.Read()
	if errors.Is(err, io.EOF) {
		return false
	}
	if err != nil {
		c.err = err
		return false
	}
	c.seq++
	dst.Reset()
	dst.Seq = c.seq
	if c.idIdx < len(row) {
		dst.ID = row[c.idIdx]
	}
	for _, b := range c.bindings {
		if b.idx >= len(row) || row[b.idx] == "" {
			continue
		}
		v := row[b.idx]
		before := len(dst.Values)
		switch b.b.Kind {
		case ValueKeyword:
			dst.AddKeyword(b.b.Field, v, b.b.Normalized)
		case ValueText:
			dst.AddText(b.b.Field, v, b.b.Normalized)
		case ValueNumber:
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				c.err = err
				return false
			}
			dst.AddNumber(b.b.Field, f)
		case ValueTimeUnix:
			unix, err := parseSourceTime(v, b.b.Layout)
			if err != nil {
				c.err = fmt.Errorf("column %q: %w", b.b.Column, err)
				return false
			}
			dst.AddUnixTime(b.b.Field, unix)
		}
		if len(dst.Values) > before {
			dst.Values[len(dst.Values)-1].Source = v
		}
	}
	return true
}
func (c *csvCursor) Err() error   { return c.err }
func (c *csvCursor) Close() error { return nil }

func parseSourceTime(value, layout string) (int64, error) {
	value = strings.TrimSpace(value)
	if layout != "" {
		t, err := time.Parse(layout, value)
		if err != nil {
			return 0, fmt.Errorf("parse time %q with layout %q: %w", value, layout, err)
		}
		return t.Unix(), nil
	}
	if f, err := strconv.ParseFloat(value, 64); err == nil {
		return int64(f), nil
	}
	for _, candidate := range autoTimeLayouts {
		if t, err := time.Parse(candidate, value); err == nil {
			return t.Unix(), nil
		}
	}
	return 0, fmt.Errorf("parse time %q: expected Unix timestamp or a supported date/time format", value)
}

// TupleLookupSchema is optimized for domain-specific/group style lookup with
// exact term + group + date-of-service filters.
func TupleLookupSchema() Schema {
	return Schema{Fields: map[string]FieldOptions{
		"term":         {Kind: FieldKeyword, Lookup: true, Lowercase: true, Prefix: true},
		"group_id":     {Kind: FieldKeyword, Lookup: true},
		"date_key":     {Kind: FieldKeyword, Lookup: true},
		"partition_id": {Kind: FieldKeyword, Lookup: true},
		"entity_id":    {Kind: FieldKeyword, Lookup: true},
	}}
}

// TupleQuery builds: term=key-special AND group_id=4 AND date_key=2026-01-01.
func TupleQuery(term, groupID, date_key string) Query {
	filters := []Query{}
	if term != "" {
		filters = append(filters, Term{Field: "term", Value: strings.ToLower(term)})
	}
	if groupID != "" {
		filters = append(filters, Term{Field: "group_id", Value: groupID})
	}
	if date_key != "" {
		filters = append(filters, Term{Field: "date_key", Value: date_key})
	}
	if len(filters) == 0 {
		return MatchAll{}
	}
	return Bool{Filter: filters}
}

// ParseLookupQuery parses URL query strings such as term=key-special&group_id=4&date_key=2026-01-01.
// When the full record key is present it returns TupleCompositeQuery so
// indexes with the composite accelerator use one direct key lookup instead of a
// generic boolean intersection. If no composite accelerator exists, the query
// falls back automatically to the generic TupleQuery path.
func ParseLookupQuery(raw string) Query { return ParseLookupQueryFast(raw) }

// CompileLookupQuery validates URL-style field=value filters against the
// active schema and compiles each value using the field's storage type.
func (ix *Index) CompileDatasourceLookup(raw string) (Query, error) {
	vals, err := url.ParseQuery(strings.TrimPrefix(raw, "?"))
	if err != nil {
		return nil, fmt.Errorf("invalid lookup query: %w", err)
	}
	filters := make([]Query, 0, len(vals))
	for field, values := range vals {
		if field == "limit" || field == "offset" {
			continue
		}
		name, operator := lookupFieldOperator(field)
		opt, ok := ix.cfg.Schema.Fields[name]
		if !ok {
			return nil, fmt.Errorf("unknown filter field %q", name)
		}
		for _, value := range values {
			value = strings.TrimSpace(value)
			if value == "" && operator != "exists" && operator != "missing" && operator != "not_zero" {
				continue
			}
			q, err := compileDatasourceFilter(name, operator, value, opt)
			if err != nil {
				return nil, err
			}
			filters = append(filters, q)
		}
	}
	if len(filters) == 0 {
		return MatchAll{}, nil
	}
	return Bool{Filter: filters}, nil
}

func lookupFieldOperator(key string) (string, string) {
	parts := strings.SplitN(key, "__", 2)
	if len(parts) == 1 {
		return key, "eq"
	}
	return parts[0], strings.ToLower(parts[1])
}

func compileDatasourceFilter(field, op, value string, opt FieldOptions) (Query, error) {
	numeric := opt.Kind == FieldInt || opt.Kind == FieldFloat || opt.Kind == FieldTime
	parseNumber := func(raw string) (float64, error) {
		if opt.Kind == FieldTime {
			n, err := parseSourceTime(raw, "")
			return float64(n), err
		}
		n, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return 0, fmt.Errorf("filter %q requires a number: %w", field, err)
		}
		return n, nil
	}
	equal := func(raw string) (Query, error) {
		if numeric {
			n, err := parseNumber(raw)
			return Range{Field: field, GTE: n, LTE: n}, err
		}
		if opt.Kind == FieldText {
			return Simple(field, raw), nil
		}
		return Term{Field: field, Value: raw}, nil
	}
	switch op {
	case "eq", "equal", "equals":
		return equal(value)
	case "ne", "neq", "not_equal":
		q, err := equal(value)
		return Not{Q: q}, err
	case "exists":
		return Exists{Field: field}, nil
	case "missing", "not_exists":
		return Missing{Field: field}, nil
	case "not_zero":
		if !numeric {
			return nil, fmt.Errorf("operator %q requires a numeric field", op)
		}
		return Or{Range{Field: field, LT: 0}, Range{Field: field, GT: 0}}, nil
	case "gt", "gte", "lt", "lte":
		if !numeric {
			return nil, fmt.Errorf("operator %q requires a numeric or time field", op)
		}
		n, err := parseNumber(value)
		if err != nil {
			return nil, err
		}
		q := Range{Field: field}
		switch op {
		case "gt":
			q.GT = n
		case "gte":
			q.GTE = n
		case "lt":
			q.LT = n
		case "lte":
			q.LTE = n
		}
		return q, nil
	case "between":
		if !numeric {
			return nil, fmt.Errorf("operator %q requires a numeric or time field", op)
		}
		bounds := strings.SplitN(value, ",", 2)
		if len(bounds) != 2 {
			return nil, fmt.Errorf("filter %q between value must be min,max", field)
		}
		lo, err := parseNumber(strings.TrimSpace(bounds[0]))
		if err != nil {
			return nil, err
		}
		hi, err := parseNumber(strings.TrimSpace(bounds[1]))
		if err != nil {
			return nil, err
		}
		return Range{Field: field, GTE: lo, LTE: hi}, nil
	case "contains", "not_contains", "starts_with", "ends_with", "fuzzy":
		if numeric || opt.Kind == FieldVector {
			return nil, fmt.Errorf("operator %q requires a string field", op)
		}
		var q Query
		switch op {
		case "contains", "not_contains":
			if opt.Ngram {
				q = Contains{Field: field, Value: value}
			} else {
				q = sourceStringFilter{Field: field, Value: value, Operator: "contains"}
			}
		case "starts_with":
			if opt.Prefix {
				q = Prefix{Field: field, Value: value}
			} else {
				q = sourceStringFilter{Field: field, Value: value, Operator: op}
			}
		case "ends_with":
			if opt.Suffix {
				q = Suffix{Field: field, Value: value}
			} else {
				q = sourceStringFilter{Field: field, Value: value, Operator: op}
			}
		case "fuzzy":
			if opt.Fuzzy {
				q = Fuzzy{Field: field, Value: value, Distance: 1}
			} else {
				q = sourceStringFilter{Field: field, Value: value, Operator: op}
			}
		}
		if op == "not_contains" {
			return Not{Q: q}, nil
		}
		return q, nil
	case "in", "not_in":
		parts := strings.Split(value, ",")
		queries := make([]Query, 0, len(parts))
		for _, part := range parts {
			q, err := equal(strings.TrimSpace(part))
			if err != nil {
				return nil, err
			}
			queries = append(queries, q)
		}
		q := Query(Or(queries))
		if op == "not_in" {
			q = Not{Q: q}
		}
		return q, nil
	default:
		return nil, fmt.Errorf("unsupported operator %q for field %q", op, field)
	}
}

// PagedSQLSource streams very large database tables with keyset pagination.
// It repeatedly executes SELECT ... WHERE order_column > last ORDER BY order_column LIMIT page_size.
// Use this for 10M/100M+ rows instead of OFFSET pagination.
type PagedSQLSource struct {
	DB             *sql.DB
	Table          string
	Columns        []string
	Where          string
	OrderColumn    string
	StartAfter     uint64
	PageSize       int
	IDColumn       string
	SeqColumn      string
	ColumnBindings []SQLColumn
}

func (s PagedSQLSource) Open(ctx context.Context) (Cursor, error) {
	if s.DB == nil {
		return nil, errors.New("nil sql DB")
	}
	if s.Table == "" {
		return nil, errors.New("table required")
	}
	if s.OrderColumn == "" {
		s.OrderColumn = "id"
	}
	if s.IDColumn == "" {
		s.IDColumn = s.OrderColumn
	}
	if s.SeqColumn == "" {
		s.SeqColumn = s.OrderColumn
	}
	if s.PageSize <= 0 {
		s.PageSize = 100000
	}
	return &pagedSQLCursor{src: s, ctx: ctx, last: s.StartAfter}, nil
}

type pagedSQLCursor struct {
	src  PagedSQLSource
	ctx  context.Context
	last uint64
	cur  Cursor
	done bool
	err  error
}

func (c *pagedSQLCursor) Next(ctx context.Context, dst *SourceRecord) bool {
	for {
		if c.cur != nil && c.cur.Next(ctx, dst) {
			if dst.Seq > c.last {
				c.last = dst.Seq
			}
			return true
		}
		if c.cur != nil {
			if err := c.cur.Err(); err != nil {
				c.err = err
				return false
			}
			_ = c.cur.Close()
			c.cur = nil
		}
		if c.done {
			return false
		}
		query, args := c.nextQuery()
		src := SQLSource{DB: c.src.DB, Query: query, Args: args, IDColumn: c.src.IDColumn, SeqColumn: c.src.SeqColumn, Columns: c.src.ColumnBindings}
		cur, err := src.Open(ctx)
		if err != nil {
			c.err = err
			return false
		}
		// Detect an empty page by attempting the first Next immediately.
		if !cur.Next(ctx, dst) {
			if err := cur.Err(); err != nil {
				c.err = err
				_ = cur.Close()
				return false
			}
			_ = cur.Close()
			c.done = true
			return false
		}
		c.cur = cur
		if dst.Seq > c.last {
			c.last = dst.Seq
		}
		return true
	}
}
func (c *pagedSQLCursor) nextQuery() (string, []any) {
	cols := "*"
	if len(c.src.Columns) > 0 {
		cols = strings.Join(c.src.Columns, ", ")
	}
	where := strings.TrimSpace(c.src.Where)
	args := []any{}
	if c.last > 0 {
		if where != "" {
			where += " AND "
		}
		where += c.src.OrderColumn + " > ?"
		args = append(args, c.last)
	}
	if where != "" {
		where = " WHERE " + where
	}
	return fmt.Sprintf("SELECT %s FROM %s%s ORDER BY %s ASC LIMIT %d", cols, c.src.Table, where, c.src.OrderColumn, c.src.PageSize), args
}
func (c *pagedSQLCursor) Err() error { return c.err }
func (c *pagedSQLCursor) Close() error {
	if c.cur != nil {
		return c.cur.Close()
	}
	return nil
}
