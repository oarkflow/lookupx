package pkg

import "strings"

// ---------------------------------------------------------------------------
// BCL Configuration Types
// ---------------------------------------------------------------------------
//
// These structs define the shape of a lookupx BCL configuration file.
// BCL's native Unmarshal maps blocks to struct fields automatically.
//
// Example BCL:
//
//   datasource "products" {
//     type "sql_table"
//     driver "postgres"
//     dsn env("DATABASE_URL")
//     table "products"
//     columns {
//       column "sku"  field "sku"  kind "keyword"
//     }
//   }
//
//   index "products" {
//     schema "record"
//     datasource "products"
//     bulk {
//       batch_size 65536
//     }
//   }

// LookupXConfig is the root configuration parsed from a BCL file.
type LookupXConfig struct {
	Addr       string           `bcl:"addr"        json:"addr,omitempty"`
	DataDir    string           `bcl:"data_dir"    json:"data_dir,omitempty"`
	APIKeys    []string         `bcl:"api_keys"    json:"api_keys,omitempty"`
	Datasources []DatasourceDef `bcl:"datasource,block" json:"datasources"`
	Indexes    []IndexDef       `bcl:"index,block"      json:"indexes"`
}

// DatasourceDef represents a single datasource block in the BCL config.
// The block ID is captured from the BCL block syntax: datasource "products" { ... }
type DatasourceDef struct {
	ID       string              `bcl:",id"       json:"id"`
	Kind     string              `bcl:"kind"     json:"kind"`
	Driver   string              `bcl:"driver"   json:"driver,omitempty"`
	DSN      string              `bcl:"dsn"      json:"dsn,omitempty"`
	Table    string              `bcl:"table"    json:"table,omitempty"`
	View     string              `bcl:"view"     json:"view,omitempty"`
	Query    string              `bcl:"query"    json:"query,omitempty"`
	QueryFile string             `bcl:"query_file" json:"query_file,omitempty"`
	File     string              `bcl:"file"     json:"file,omitempty"`
	URL      string              `bcl:"url"      json:"url,omitempty"`
	Method   string              `bcl:"method"   json:"method,omitempty"`
	Auth     string              `bcl:"auth"     json:"auth,omitempty"`
	IDColumn string              `bcl:"id_column"  json:"id_column,omitempty"`
	IDField  string              `bcl:"id_field"   json:"id_field,omitempty"`
	SeqColumn string             `bcl:"seq_column" json:"seq_column,omitempty"`
	OrderColumn string           `bcl:"order_column" json:"order_column,omitempty"`
	Where    string              `bcl:"where"    json:"where,omitempty"`
	PageSize int                 `bcl:"page_size" json:"page_size,omitempty"`
	PageParam string             `bcl:"page_param" json:"page_param,omitempty"`
	LimitParam int               `bcl:"limit_param" json:"limit_param,omitempty"`
	TimeoutSeconds int           `bcl:"timeout_seconds" json:"timeout_seconds,omitempty"`
	Headers  map[string]string   `bcl:"headers"  json:"headers,omitempty"`
	Params   map[string]any      `bcl:"params"   json:"params,omitempty"`
	Columns  []ColumnDef         `bcl:"column,block" json:"columns"`
}

// IndexDef represents an index block in the BCL config.
type IndexDef struct {
	ID              string      `bcl:",id"        json:"id"`
	Schema          string      `bcl:"schema"    json:"schema,omitempty"`
	InitialCapacity int         `bcl:"initial_capacity" json:"initial_capacity,omitempty"`
	Persistent      bool        `bcl:"persistent" json:"persistent,omitempty"`
	DatasourceID    string      `bcl:"datasource" json:"datasource,omitempty"`
	Bulk            BulkDef     `bcl:"bulk"      json:"bulk"`
}

// BulkDef holds bulk ingestion parameters for an index.
type BulkDef struct {
	BatchSize        int    `bcl:"batch_size"        json:"batch_size,omitempty"`
	CheckpointEvery  int    `bcl:"checkpoint_every"  json:"checkpoint_every,omitempty"`
	CheckpointPath   string `bcl:"checkpoint_path"   json:"checkpoint_path,omitempty"`
	Resume           bool   `bcl:"resume"            json:"resume,omitempty"`
	SkipBadRecords   bool   `bcl:"skip_bad_records"  json:"skip_bad_records,omitempty"`
}

// ColumnDef maps a source column/field to an index field.
type ColumnDef struct {
	Column     string `bcl:"column"     json:"column,omitempty"`
	Field      string `bcl:"field"      json:"field,omitempty"`
	Kind       string `bcl:"kind"       json:"kind,omitempty"`
	Normalized bool   `bcl:"normalized" json:"normalized,omitempty"`
	Layout     string `bcl:"layout"     json:"layout,omitempty"`
}

// ToBulkOptions converts BulkDef to the internal BulkOptions.
func (b BulkDef) ToBulkOptions() BulkOptions {
	opt := BulkOptions{
		BatchSize:       b.BatchSize,
		CheckpointEvery: b.CheckpointEvery,
		Resume:          b.Resume,
		SkipBadRecords:  b.SkipBadRecords,
	}
	if opt.BatchSize <= 0 {
		opt.BatchSize = 65536
	}
	if opt.CheckpointEvery <= 0 {
		opt.CheckpointEvery = opt.BatchSize
	}
	if b.CheckpointPath != "" {
		opt.Checkpoint = FileCheckpoint{Path: b.CheckpointPath}
	}
	return opt
}

// ToConfig converts IndexDef to the internal Config.
func (idx IndexDef) ToConfig() Config {
	schema := resolveSchema(idx.Schema)
	return Config{
		Schema:          schema,
		DisableSource:   true,
		InitialCapacity: idx.InitialCapacity,
	}
}

func resolveSchema(name string) Schema {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "record", "tuple", "lookup", "":
		return TupleLookupSchema()
	default:
		return TupleLookupSchema()
	}
}

// ResolveColumns converts ColumnDef list into SQLColumn list with field name strings.
// The actual FieldID resolution happens later when the index schema is compiled.
func (d *DatasourceDef) ResolveColumns() []SQLColumn {
	out := make([]SQLColumn, 0, len(d.Columns))
	for _, c := range d.Columns {
		col := c.Column
		if col == "" {
			col = c.Field
		}
		field := c.Field
		if field == "" {
			field = col
		}
		out = append(out, SQLColumn{
			Column:     col,
			Field:      FieldID(0), // resolved later
			Kind:       parseValueKind(c.Kind),
			Normalized: c.Normalized,
			Layout:     c.Layout,
		})
	}
	return out
}

// ToMap converts a DatasourceDef to a config map suitable for the registry factory.
func (d *DatasourceDef) ToMap() map[string]any {
	m := map[string]any{
		"id":   d.ID,
		"kind": d.Kind,
	}
	if d.Driver != "" {
		m["driver"] = d.Driver
	}
	if d.DSN != "" {
		m["dsn"] = d.DSN
	}
	if d.Table != "" {
		m["table"] = d.Table
	}
	if d.View != "" {
		m["view"] = d.View
	}
	if d.Query != "" {
		m["query"] = d.Query
	}
	if d.QueryFile != "" {
		m["query_file"] = d.QueryFile
	}
	if d.File != "" {
		m["file"] = d.File
	}
	if d.URL != "" {
		m["url"] = d.URL
	}
	if d.Method != "" {
		m["method"] = d.Method
	}
	if d.Auth != "" {
		m["auth"] = d.Auth
	}
	if d.IDColumn != "" {
		m["id_column"] = d.IDColumn
	}
	if d.IDField != "" {
		m["id_field"] = d.IDField
	}
	if d.SeqColumn != "" {
		m["seq_column"] = d.SeqColumn
	}
	if d.OrderColumn != "" {
		m["order_column"] = d.OrderColumn
	}
	if d.Where != "" {
		m["where"] = d.Where
	}
	if d.PageSize > 0 {
		m["page_size"] = d.PageSize
	}
	if d.PageParam != "" {
		m["page_param"] = d.PageParam
	}
	if d.LimitParam > 0 {
		m["limit_param"] = d.LimitParam
	}
	if d.TimeoutSeconds > 0 {
		m["timeout_seconds"] = d.TimeoutSeconds
	}
	if len(d.Headers) > 0 {
		headers := make(map[string]any, len(d.Headers))
		for k, v := range d.Headers {
			headers[k] = v
		}
		m["headers"] = headers
	}
	if len(d.Columns) > 0 {
		cols := make([]any, 0, len(d.Columns))
		for _, c := range d.Columns {
			cm := map[string]any{}
			if c.Column != "" {
				cm["column"] = c.Column
			}
			if c.Field != "" {
				cm["field"] = c.Field
			}
			if c.Kind != "" {
				cm["kind"] = c.Kind
			}
			if c.Normalized {
				cm["normalized"] = true
			}
			if c.Layout != "" {
				cm["layout"] = c.Layout
			}
			cols = append(cols, cm)
		}
		m["columns"] = cols
	}
	return m
}
