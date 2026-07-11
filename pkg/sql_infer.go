package pkg

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"strings"
	"time"
)

// AutoColumn is a proposed column binding produced by schema inference: a
// database column mapped to a schema field name, a storage ValueKind, an
// optional time layout, and default FieldOptions. Field is resolved to a
// FieldID via BindSQLColumns once the Index (and therefore its schema) exists.
type AutoColumn struct {
	Column  string
	Field   string
	Kind    ValueKind
	Layout  string
	Options FieldOptions
}

// autoTimeLayouts are tried in order against sampled values to pick a layout
// for time-typed columns. RFC3339Nano is first because database/sql formats
// driver time.Time values that way when scanned into a *string/*NullString.
var autoTimeLayouts = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02 15:04:05.999999999Z07:00",
	"2006-01-02 15:04:05",
	"2006-01-02T15:04:05",
	"2006-01-02",
	"15:04:05",
}

var codeNameHints = []string{"code", "status", "type", "category", "key", "flag", "level", "state", "sku", "abbr"}
var textNameHints = []string{"desc", "description", "note", "comment", "title", "name", "summary", "address", "bio", "content", "text", "message"}

// InferSQLColumns samples up to sampleSize rows from query/args and proposes
// an AutoColumn for every result column except idColumn/seqColumn. It combines
// driver column type metadata (sql.ColumnType) with a look at sampled values
// to choose a storage kind, time layout, and default FieldOptions — so callers
// don't need to hand-write a Schema or a []SQLColumn for common SQL sources.
//
// query/args are executed as-is (no LIMIT is injected, since dialects differ);
// pass an already-bounded query, such as the first page of a keyset-paginated
// source, to keep sampling cheap.
func InferSQLColumns(ctx context.Context, db *sql.DB, query string, args []any, idColumn, seqColumn string, sampleSize int) ([]AutoColumn, error) {
	if db == nil {
		return nil, errors.New("nil sql DB")
	}
	if strings.TrimSpace(query) == "" {
		return nil, errors.New("sql query required")
	}
	if sampleSize <= 0 {
		sampleSize = 200
	}
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	names := make([]string, len(colTypes))
	for i, ct := range colTypes {
		names[i] = ct.Name()
	}

	samples := make([][]string, len(colTypes))
	vals := make([]sql.NullString, len(colTypes))
	dest := make([]any, len(colTypes))
	for i := range vals {
		dest[i] = &vals[i]
	}
	n := 0
	for n < sampleSize && rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}
		for i, v := range vals {
			if v.Valid && v.String != "" && len(samples[i]) < 25 {
				samples[i] = append(samples[i], v.String)
			}
		}
		n++
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	idLower := strings.ToLower(strings.TrimSpace(idColumn))
	seqLower := strings.ToLower(strings.TrimSpace(seqColumn))

	out := make([]AutoColumn, 0, len(colTypes))
	for i, ct := range colTypes {
		lname := strings.ToLower(names[i])
		if lname == idLower || (seqLower != "" && lname == seqLower) {
			continue
		}
		out = append(out, classifyColumn(names[i], ct.DatabaseTypeName(), samples[i]))
	}
	return out, nil
}

// classifyColumn picks a ValueKind, time layout, and FieldOptions for one
// column from its database type name and a handful of sampled string values.
// It is a pure function so the classification heuristics can be tested
// without a live database.
func classifyColumn(name, dbType string, samples []string) AutoColumn {
	dbType = strings.ToUpper(strings.TrimSpace(dbType))

	switch {
	case isNumericDBType(dbType):
		fieldKind := FieldFloat
		if isIntegerDBType(dbType) {
			fieldKind = FieldInt
		}
		return AutoColumn{
			Column: name, Field: name, Kind: ValueNumber,
			Options: FieldOptions{Kind: fieldKind, Sortable: true},
		}
	case isTimeDBType(dbType):
		layout := detectTimeLayout(samples)
		if layout == "" {
			return AutoColumn{
				Column: name, Field: name, Kind: ValueKeyword,
				Options: FieldOptions{Kind: FieldKeyword, Lookup: true},
			}
		}
		return AutoColumn{
			Column: name, Field: name, Kind: ValueTimeUnix, Layout: layout,
			Options: FieldOptions{Kind: FieldTime, Sortable: true},
		}
	case isBoolDBType(dbType):
		return AutoColumn{
			Column: name, Field: name, Kind: ValueKeyword,
			Options: FieldOptions{Kind: FieldKeyword, Lookup: true},
		}
	default:
		return classifyStringColumn(name, samples)
	}
}

// classifyStringColumn classifies a column already known to be string-typed
// (or whose type is unknown) using its name and the shape of sampled values:
// short/no-space values look like codes (keyword+prefix), long or
// multi-word values look like free text, name hints override both.
func classifyStringColumn(name string, samples []string) AutoColumn {
	lname := strings.ToLower(name)
	shape := shapeOf(samples)
	switch {
	// Name-based hints are a stronger signal than sampled value shape, so
	// they're checked first; a short sample of e.g. one placeholder value
	// shouldn't override an explicit "_desc"/"_name" column name.
	case containsAny(lname, textNameHints):
		return AutoColumn{
			Column: name, Field: name, Kind: ValueText,
			Options: FieldOptions{Kind: FieldText, Indexed: true, Lowercase: true, Fuzzy: true},
		}
	case containsAny(lname, codeNameHints):
		return AutoColumn{
			Column: name, Field: name, Kind: ValueKeyword,
			Options: FieldOptions{Kind: FieldKeyword, Lookup: true, Lowercase: true, Prefix: true, MinPrefix: 3, MaxPrefix: 5},
		}
	case shape.avgLen > 0 && shape.avgLen <= 24 && !shape.hasSpaces:
		return AutoColumn{
			Column: name, Field: name, Kind: ValueKeyword,
			Options: FieldOptions{Kind: FieldKeyword, Lookup: true, Lowercase: true, Prefix: true, MinPrefix: 3, MaxPrefix: 5},
		}
	case shape.avgLen > 40 || shape.hasSpaces:
		return AutoColumn{
			Column: name, Field: name, Kind: ValueText,
			Options: FieldOptions{Kind: FieldText, Indexed: true, Lowercase: true, Fuzzy: true},
		}
	default:
		return AutoColumn{
			Column: name, Field: name, Kind: ValueKeyword,
			Options: FieldOptions{Kind: FieldKeyword, Lookup: true, Lowercase: true},
		}
	}
}

// classifyFromValues infers a column's storage kind purely from its name and
// sampled string values, with no driver/schema type metadata available. It
// backs inference for value-oriented sources (CSV, JSON Lines) where every
// sampled value already arrives as text (or has been stringified).
//
// Name hints are checked before guessing from value shape: numeric-looking
// codes (CPT codes, ZIP codes, SKUs) are common and must stay keyword fields
// for exact-match/prefix lookup rather than becoming numeric fields just
// because every sampled value happens to parse as a number.
func classifyFromValues(name string, samples []string) AutoColumn {
	lname := strings.ToLower(name)
	if containsAny(lname, codeNameHints) {
		return AutoColumn{
			Column: name, Field: name, Kind: ValueKeyword,
			Options: FieldOptions{Kind: FieldKeyword, Lookup: true, Lowercase: true, Prefix: true, MinPrefix: 3, MaxPrefix: 5},
		}
	}
	if containsAny(lname, textNameHints) {
		return AutoColumn{
			Column: name, Field: name, Kind: ValueText,
			Options: FieldOptions{Kind: FieldText, Indexed: true, Lowercase: true, Fuzzy: true},
		}
	}
	if len(samples) > 0 && allNumeric(samples) {
		fieldKind := FieldFloat
		if allIntegers(samples) {
			fieldKind = FieldInt
		}
		return AutoColumn{
			Column: name, Field: name, Kind: ValueNumber,
			Options: FieldOptions{Kind: fieldKind, Sortable: true},
		}
	}
	if len(samples) > 0 && allBool(samples) {
		return AutoColumn{
			Column: name, Field: name, Kind: ValueKeyword,
			Options: FieldOptions{Kind: FieldKeyword, Lookup: true},
		}
	}
	if len(samples) > 0 {
		if layout := detectTimeLayout(samples); layout != "" {
			return AutoColumn{
				Column: name, Field: name, Kind: ValueTimeUnix, Layout: layout,
				Options: FieldOptions{Kind: FieldTime, Sortable: true},
			}
		}
	}
	return classifyStringColumn(name, samples)
}

func allNumeric(samples []string) bool {
	for _, s := range samples {
		if _, err := strconv.ParseFloat(s, 64); err != nil {
			return false
		}
	}
	return true
}

func allIntegers(samples []string) bool {
	for _, s := range samples {
		if _, err := strconv.ParseInt(s, 10, 64); err != nil {
			return false
		}
	}
	return true
}

func allBool(samples []string) bool {
	for _, s := range samples {
		if _, err := strconv.ParseBool(s); err != nil {
			return false
		}
	}
	return true
}

func containsAny(s string, hints []string) bool {
	for _, h := range hints {
		if strings.Contains(s, h) {
			return true
		}
	}
	return false
}

type sampleShape struct {
	avgLen    float64
	hasSpaces bool
}

func shapeOf(samples []string) sampleShape {
	if len(samples) == 0 {
		return sampleShape{}
	}
	total := 0
	spaces := 0
	for _, s := range samples {
		total += len(s)
		if strings.ContainsAny(s, " \t") {
			spaces++
		}
	}
	return sampleShape{avgLen: float64(total) / float64(len(samples)), hasSpaces: spaces > len(samples)/2}
}

func detectTimeLayout(samples []string) string {
	if len(samples) == 0 {
		return time.RFC3339Nano
	}
	for _, layout := range autoTimeLayouts {
		ok := true
		for _, s := range samples {
			if _, err := time.Parse(layout, s); err != nil {
				ok = false
				break
			}
		}
		if ok {
			return layout
		}
	}
	return ""
}

func isIntegerDBType(t string) bool {
	switch {
	case strings.Contains(t, "INT"): // INT2/INT4/INT8, INTEGER, SMALLINT, BIGINT, TINYINT, MEDIUMINT
		return true
	case strings.Contains(t, "SERIAL"):
		return true
	default:
		return false
	}
}

func isNumericDBType(t string) bool {
	if isIntegerDBType(t) {
		return true
	}
	switch {
	case strings.Contains(t, "NUMERIC"), strings.Contains(t, "DECIMAL"),
		strings.Contains(t, "REAL"), strings.Contains(t, "FLOAT"),
		strings.Contains(t, "DOUBLE"), strings.Contains(t, "MONEY"):
		return true
	default:
		return false
	}
}

func isTimeDBType(t string) bool {
	switch {
	case strings.Contains(t, "TIMESTAMP"), strings.Contains(t, "DATETIME"),
		strings.Contains(t, "DATE"), strings.Contains(t, "TIME"):
		return true
	default:
		return false
	}
}

func isBoolDBType(t string) bool {
	return t == "BOOL" || t == "BOOLEAN"
}

// AutoSchema builds a Schema from inferred columns, keyed by AutoColumn.Field.
func AutoSchema(cols []AutoColumn) Schema {
	fields := make(map[string]FieldOptions, len(cols))
	for _, c := range cols {
		fields[c.Field] = c.Options
	}
	return Schema{Fields: fields}
}

// BindSQLColumns resolves inferred columns against an already-created Index
// (whose schema must already contain each AutoColumn.Field, e.g. via
// AutoSchema) into []SQLColumn bindings ready for SQLSource, SQLQuerySource,
// PagedSQLQuerySource, or PagedSQLSource.
func BindSQLColumns(ix *Index, cols []AutoColumn) []SQLColumn {
	out := make([]SQLColumn, len(cols))
	for i, c := range cols {
		out[i] = SQLColumn{Column: c.Column, Field: ix.FieldID(c.Field), Kind: c.Kind, Layout: c.Layout}
	}
	return out
}

// mergeAutoSchema overlays inferred field options onto cfg without clobbering
// any field the caller already declared explicitly.
func mergeAutoSchema(cfg Config, cols []AutoColumn) Config {
	if cfg.Schema.Fields == nil {
		cfg.Schema = AutoSchema(cols)
		return cfg
	}
	for name, opt := range AutoSchema(cols).Fields {
		if _, exists := cfg.Schema.Fields[name]; !exists {
			cfg.Schema.Fields[name] = opt
		}
	}
	return cfg
}

// AutoPagedSQLQuery infers field kinds and schema by sampling the first page
// of a keyset-paginated SQL query, creates a new Index (merging the inferred
// schema under any fields cfg.Schema already declares explicitly), and
// returns the Index alongside a ready-to-use PagedSQLQuerySource — no manual
// Schema.Fields map, FieldID() calls, or Columns slice required.
func AutoPagedSQLQuery(ctx context.Context, cfg Config, db *sql.DB, page SQLPageFunc, idColumn, seqColumn string, pageSize int) (*Index, PagedSQLQuerySource, error) {
	if db == nil {
		return nil, PagedSQLQuerySource{}, errors.New("nil sql DB")
	}
	if page == nil {
		return nil, PagedSQLQuerySource{}, errors.New("sql page function required")
	}
	sampleSize := pageSize
	if sampleSize <= 0 || sampleSize > 200 {
		sampleSize = 200
	}
	sampleQuery, sampleArgs := page(0, sampleSize)
	cols, err := InferSQLColumns(ctx, db, sampleQuery, sampleArgs, idColumn, seqColumn, sampleSize)
	if err != nil {
		return nil, PagedSQLQuerySource{}, err
	}
	ix, err := New(mergeAutoSchema(cfg, cols))
	if err != nil {
		return nil, PagedSQLQuerySource{}, err
	}
	src := PagedSQLQuerySource{
		DB: db, Page: page, IDColumn: idColumn, SeqColumn: seqColumn,
		Columns: BindSQLColumns(ix, cols), PageSize: pageSize,
	}
	return ix, src, nil
}

// AutoSQLQuery infers field kinds and schema by sampling an arbitrary SQL
// query, creates a new Index (merging the inferred schema under any fields
// cfg.Schema already declares explicitly), and returns the Index alongside a
// ready-to-use SQLQuerySource.
func AutoSQLQuery(ctx context.Context, cfg Config, db *sql.DB, query string, args []any, idColumn, seqColumn string) (*Index, SQLQuerySource, error) {
	if db == nil {
		return nil, SQLQuerySource{}, errors.New("nil sql DB")
	}
	cols, err := InferSQLColumns(ctx, db, query, args, idColumn, seqColumn, 200)
	if err != nil {
		return nil, SQLQuerySource{}, err
	}
	ix, err := New(mergeAutoSchema(cfg, cols))
	if err != nil {
		return nil, SQLQuerySource{}, err
	}
	src := SQLQuerySource{
		DB: db, Query: query, Args: args, IDColumn: idColumn, SeqColumn: seqColumn,
		Columns: BindSQLColumns(ix, cols),
	}
	return ix, src, nil
}
