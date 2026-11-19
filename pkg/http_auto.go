package pkg

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/oarkflow/squealx"
)

// sqlSampleRequest is the shared shape for "connect to a SQL source and
// sample its columns" used by both InferColumns (read-only preview) and
// AutoIndex (preview + immediately create the index from it).
type sqlSampleRequest struct {
	Driver      string `json:"driver"`
	DSN         string `json:"dsn"`
	Source      string `json:"source"` // "sql_query" or "sql_table"
	Query       string `json:"query,omitempty"`
	Table       string `json:"table,omitempty"`
	Where       string `json:"where,omitempty"`
	OrderColumn string `json:"order_column,omitempty"`
	IDColumn    string `json:"id_column"`
	SeqColumn   string `json:"seq_column,omitempty"`
	SampleSize  int    `json:"sample_size,omitempty"`
	DataDir     string `json:"-"`
}

// AutoIndexColumn mirrors one detected column, using the same string kind
// names accepted by reload-sql/reload-table's "columns" list (see
// parseValueKind), so the response can be fed straight back into those
// endpoints to load the full dataset.
type AutoIndexColumn struct {
	Column    string `json:"column"`
	Field     string `json:"field"`
	Kind      string `json:"kind"`
	Layout    string `json:"layout,omitempty"`
	FieldKind string `json:"field_kind"`
}

// connectAndSample opens req's SQL source and infers one AutoColumn per
// result column (excluding IDColumn/SeqColumn). It is the shared plumbing
// behind /v1/infer-columns and /v1/auto-index.
func connectAndSample(ctx context.Context, req sqlSampleRequest) (*squealx.DB, []AutoColumn, error) {
	if req.Driver == "" || req.DSN == "" {
		return nil, nil, errors.New("driver and dsn required")
	}
	sampleSize := req.SampleSize
	if sampleSize <= 0 {
		sampleSize = 200
	}

	dsn, err := secureSQLiteDSN(req.Driver, req.DSN, req.DataDir)
	if err != nil {
		return nil, nil, err
	}
	db, err := squealx.Connect(squealxDriver(req.Driver), dsn, "")
	if err != nil {
		return nil, nil, err
	}

	var sampleQuery string
	switch req.Source {
	case "sql_table":
		if strings.TrimSpace(req.Table) == "" {
			db.Close()
			return nil, nil, errors.New("table required")
		}
		order := strings.TrimSpace(req.OrderColumn)
		var b strings.Builder
		b.WriteString("SELECT * FROM ")
		b.WriteString(req.Table)
		if strings.TrimSpace(req.Where) != "" {
			b.WriteString(" WHERE ")
			b.WriteString(req.Where)
		}
		if order != "" {
			b.WriteString(" ORDER BY ")
			b.WriteString(order)
			b.WriteString(" ASC")
		}
		b.WriteString(" LIMIT ")
		b.WriteString(strconv.Itoa(sampleSize))
		sampleQuery = b.String()
	default:
		if strings.TrimSpace(req.Query) == "" {
			db.Close()
			return nil, nil, errors.New("query required")
		}
		sampleQuery = req.Query
	}

	cols, err := InferSQLColumns(ctx, db.DB(), sampleQuery, nil, req.IDColumn, req.SeqColumn, sampleSize)
	if err != nil {
		db.Close()
		return nil, nil, err
	}
	return db, cols, nil
}

func autoColumnsWire(cols []AutoColumn) []AutoIndexColumn {
	out := make([]AutoIndexColumn, 0, len(cols))
	for _, c := range cols {
		out = append(out, AutoIndexColumn{
			Column: c.Column, Field: c.Field, Kind: valueKindName(c.Kind),
			Layout: c.Layout, FieldKind: fieldKindName(c.Options),
		})
	}
	return out
}

// InferColumnsResponse is the read-only column-detection preview: it neither
// creates nor mutates any index. The web UI uses this to populate the
// "column mappings" list for reload-sql/reload-table before the user commits
// to loading data, so those requests can never silently ship with an empty
// columns list.
type InferColumnsResponse struct {
	Columns []AutoIndexColumn `json:"columns"`
}

// AutoIndexRequest drives the combined "detect schema from a live SQL source,
// then create the index" flow used by the web UI's index-creation wizard. It
// samples SampleSize rows to infer field kinds via InferSQLColumns, so the
// caller never has to hand-write a Schema or column/kind mappings.
type AutoIndexRequest struct {
	ID string `json:"id"`
	sqlSampleRequest
}

type AutoIndexResponse struct {
	ID      string            `json:"id"`
	Columns []AutoIndexColumn `json:"columns"`
	Fields  map[string]any    `json:"fields"`
}

// schemaFieldsWire renders a compiled schema for the web UI: each field's
// numeric Kind (the wire format Config.Schema itself uses) plus a human
// -readable kind_name so the frontend doesn't need to hardcode the enum.
func schemaFieldsWire(fields map[string]FieldOptions) map[string]any {
	out := make(map[string]any, len(fields))
	for name, o := range fields {
		out[name] = map[string]any{
			"kind": int(o.Kind), "kind_name": fieldKindName(o),
			"indexed": o.Indexed, "lookup": o.Lookup, "term_search": o.TermSearch, "prefix": o.Prefix,
			"sortable": o.Sortable, "facetable": o.Facetable, "lowercase": o.Lowercase,
			"min_prefix": o.MinPrefix, "max_prefix": o.MaxPrefix,
		}
		if o.TimeFormat != "" {
			out[name].(map[string]any)["time_format"] = o.TimeFormat
		}
	}
	return out
}

func fieldKindName(o FieldOptions) string {
	k := o.Kind
	switch k {
	case FieldText:
		return "text"
	case FieldInt:
		return "int"
	case FieldFloat:
		return "float"
	case FieldBool:
		return "bool"
	case FieldTime:
		if o.TimeFormat != "" {
			return o.TimeFormat
		}
		return "datetimez"
	case FieldVector:
		return "vector"
	default:
		return "keyword"
	}
}

// valueKindName is the reverse of parseValueKind: it picks the canonical
// string for each ValueKind so responses can be fed straight back into
// reload-sql/reload-table's "columns" list.
func valueKindName(k ValueKind) string {
	switch k {
	case ValueText:
		return "text"
	case ValueNumber:
		return "number"
	case ValueTimeUnix:
		return "time"
	case ValueVector:
		return "vector"
	default:
		return "keyword"
	}
}
