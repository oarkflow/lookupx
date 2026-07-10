package pkg

import (
	"context"
	"errors"
	"fmt"
	"os"
)

// ---------------------------------------------------------------------------
// csv — CSV file datasource.
// ---------------------------------------------------------------------------

// CSVFileDatasource streams a CSV file with header-row column binding.
type CSVFileDatasource struct {
	id       string
	filePath string
	idColumn string
	bindings []CSVBinding
	params   map[string]any
}

func init() {
	GlobalRegistry.MustRegister("csv", newCSVFileDatasource)
	GlobalRegistry.MustRegister("jsonl", newJSONLFileDatasource)
}

func newCSVFileDatasource(config map[string]any, params map[string]any) (Datasource, error) {
	config = ApplyParams(config, params)
	id, err := ConfigString(config, "id")
	if err != nil {
		return nil, err
	}
	filePath, err := ConfigString(config, "file")
	if err != nil {
		return nil, fmt.Errorf("csv %s: %w", id, err)
	}
	idColumn := ConfigStringOr(config, "id_column", "id")

	bindings, err := parseCSVBindings(config)
	if err != nil {
		return nil, fmt.Errorf("csv %s: %w", id, err)
	}

	return &CSVFileDatasource{
		id:       id,
		filePath: filePath,
		idColumn: idColumn,
		bindings: bindings,
		params:   params,
	}, nil
}

func (d *CSVFileDatasource) ID() string   { return d.id }
func (d *CSVFileDatasource) Type() string { return "csv" }

func (d *CSVFileDatasource) Validate() error {
	if d.filePath == "" {
		return errors.New("csv: file required")
	}
	if d.idColumn == "" {
		return errors.New("csv: id_column required")
	}
	return nil
}

func (d *CSVFileDatasource) Open(ctx context.Context) (Cursor, error) {
	f, err := os.Open(d.filePath)
	if err != nil {
		return nil, fmt.Errorf("csv %s: open: %w", d.id, err)
	}
	src := CSVSource{
		R:        f,
		IDColumn: d.idColumn,
		Bindings: d.bindings,
	}
	cur, err := src.Open(ctx)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("csv %s: open cursor: %w", d.id, err)
	}
	return &fileClosingCursor{cur: cur, f: f}, nil
}

// ---------------------------------------------------------------------------
// jsonl — JSON Lines file datasource.
// ---------------------------------------------------------------------------

// JSONLFileDatasource streams a newline-delimited JSON file.
type JSONLFileDatasource struct {
	id       string
	filePath string
	idField  string
	bindings []JSONBinding
	params   map[string]any
}

func newJSONLFileDatasource(config map[string]any, params map[string]any) (Datasource, error) {
	config = ApplyParams(config, params)
	id, err := ConfigString(config, "id")
	if err != nil {
		return nil, err
	}
	filePath, err := ConfigString(config, "file")
	if err != nil {
		return nil, fmt.Errorf("jsonl %s: %w", id, err)
	}
	idField := ConfigStringOr(config, "id_field", "id")

	bindings, err := parseJSONLBindings(config)
	if err != nil {
		return nil, fmt.Errorf("jsonl %s: %w", id, err)
	}

	return &JSONLFileDatasource{
		id:       id,
		filePath: filePath,
		idField:  idField,
		bindings: bindings,
		params:   params,
	}, nil
}

func (d *JSONLFileDatasource) ID() string   { return d.id }
func (d *JSONLFileDatasource) Type() string { return "jsonl" }

func (d *JSONLFileDatasource) Validate() error {
	if d.filePath == "" {
		return errors.New("jsonl: file required")
	}
	if d.idField == "" {
		return errors.New("jsonl: id_field required")
	}
	return nil
}

func (d *JSONLFileDatasource) Open(ctx context.Context) (Cursor, error) {
	f, err := os.Open(d.filePath)
	if err != nil {
		return nil, fmt.Errorf("jsonl %s: open: %w", d.id, err)
	}
	src := JSONLSource{
		R:        f,
		IDField:  d.idField,
		Bindings: d.bindings,
	}
	cur, err := src.Open(ctx)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("jsonl %s: open cursor: %w", d.id, err)
	}
	return &fileClosingCursor{cur: cur, f: f}, nil
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

// fileClosingCursor wraps a Cursor and closes the underlying file when done.
type fileClosingCursor struct {
	cur Cursor
	f   *os.File
}

func (c *fileClosingCursor) Next(ctx context.Context, dst *SourceRecord) bool {
	return c.cur.Next(ctx, dst)
}
func (c *fileClosingCursor) Err() error { return c.cur.Err() }
func (c *fileClosingCursor) Close() error {
	err := c.cur.Close()
	c.f.Close()
	return err
}

// parseCSVBindings parses the "columns" list from BCL config into CSVBinding values.
func parseCSVBindings(config map[string]any) ([]CSVBinding, error) {
	list, err := ConfigList(config, "columns")
	if err != nil {
		return nil, err
	}
	if list == nil {
		return nil, nil
	}
	out := make([]CSVBinding, 0, len(list))
	for i, item := range list {
		m, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("columns[%d] must be an object, got %T", i, item)
		}
		col, err := ConfigString(m, "column")
		if err != nil {
			return nil, fmt.Errorf("columns[%d]: %w", i, err)
		}
		kind := ConfigStringOr(m, "kind", "keyword")
		normalized, _ := ConfigBool(m, "normalized")
		layout := ConfigStringOr(m, "layout", "")
		out = append(out, CSVBinding{
			Column:     col,
			Field:      FieldID(0), // resolved later
			Kind:       parseValueKind(kind),
			Normalized: normalized,
			Layout:     layout,
		})
	}
	return out, nil
}

// parseJSONLBindings parses the "columns" list from BCL config into JSONBinding values.
func parseJSONLBindings(config map[string]any) ([]JSONBinding, error) {
	list, err := ConfigList(config, "columns")
	if err != nil {
		return nil, err
	}
	if list == nil {
		return nil, nil
	}
	out := make([]JSONBinding, 0, len(list))
	for i, item := range list {
		m, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("columns[%d] must be an object, got %T", i, item)
		}
		fieldName, err := ConfigString(m, "field")
		if err != nil {
			// Fall back to "column" key for consistency with SQL config.
			fieldName, err = ConfigString(m, "column")
			if err != nil {
				return nil, fmt.Errorf("columns[%d]: %w", i, err)
			}
		}
		kind := ConfigStringOr(m, "kind", "keyword")
		normalized, _ := ConfigBool(m, "normalized")
		out = append(out, JSONBinding{
			FieldName:  fieldName,
			Field:      FieldID(0), // resolved later
			Kind:       parseValueKind(kind),
			Normalized: normalized,
		})
	}
	return out, nil
}
