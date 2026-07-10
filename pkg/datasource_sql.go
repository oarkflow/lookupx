package pkg

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
)

// ---------------------------------------------------------------------------
// sql_table — Keyset-paginated table scan datasource.
// ---------------------------------------------------------------------------

// SQLTableDatasource streams a database table using keyset pagination. It wraps
// PagedSQLSource with config-driven column binding and parameter support.
type SQLTableDatasource struct {
	id       string
	src      PagedSQLSource
	driver   string
	dsn      string
	params   map[string]any
}

func init() {
	GlobalRegistry.MustRegister("sql_table", newSQLTableDatasource)
	GlobalRegistry.MustRegister("sql_view", newSQLViewDatasource)
	GlobalRegistry.MustRegister("sql_query", newSQLQueryDatasource)
	GlobalRegistry.MustRegister("sql_file", newSQLFileDatasource)
}

func newSQLTableDatasource(config map[string]any, params map[string]any) (Datasource, error) {
	config = ApplyParams(config, params)
	id, err := ConfigString(config, "id")
	if err != nil {
		return nil, err
	}
	driver, err := ConfigString(config, "driver")
	if err != nil {
		return nil, fmt.Errorf("sql_table %s: %w", id, err)
	}
	dsn, err := ConfigString(config, "dsn")
	if err != nil {
		return nil, fmt.Errorf("sql_table %s: %w", id, err)
	}
	table, err := ConfigString(config, "table")
	if err != nil {
		return nil, fmt.Errorf("sql_table %s: %w", id, err)
	}
	idColumn := ConfigStringOr(config, "id_column", "id")
	seqColumn := ConfigStringOr(config, "seq_column", idColumn)
	orderColumn := ConfigStringOr(config, "order_column", "id")
	pageSize := ConfigIntOr(config, "page_size", 100000)
	where := ConfigStringOr(config, "where", "")
	selectCols, _ := ConfigStringList(config, "columns")

	cols, err := parseColumnBindings(config)
	if err != nil {
		return nil, fmt.Errorf("sql_table %s: %w", id, err)
	}

	return &SQLTableDatasource{
		id:     id,
		driver: driver,
		dsn:    dsn,
		params: params,
		src: PagedSQLSource{
			Table:          table,
			Columns:        selectCols,
			Where:          where,
			OrderColumn:    orderColumn,
			PageSize:       pageSize,
			IDColumn:       idColumn,
			SeqColumn:      seqColumn,
			ColumnBindings: cols,
		},
	}, nil
}

func (d *SQLTableDatasource) ID() string   { return d.id }
func (d *SQLTableDatasource) Type() string { return "sql_table" }

func (d *SQLTableDatasource) Validate() error {
	if d.driver == "" {
		return errors.New("sql_table: driver required")
	}
	if d.dsn == "" {
		return errors.New("sql_table: dsn required")
	}
	if d.src.Table == "" {
		return errors.New("sql_table: table required")
	}
	return nil
}

func (d *SQLTableDatasource) Open(ctx context.Context) (Cursor, error) {
	db, err := sql.Open(d.driver, d.dsn)
	if err != nil {
		return nil, fmt.Errorf("sql_table %s: open: %w", d.id, err)
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("sql_table %s: ping: %w", d.id, err)
	}
	d.src.DB = db
	cur, err := d.src.Open(ctx)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("sql_table %s: open cursor: %w", d.id, err)
	}
	return &dbClosingCursor{cur: cur, db: db}, nil
}

// ---------------------------------------------------------------------------
// sql_view — Database view query datasource.
// ---------------------------------------------------------------------------

// SQLViewDatasource reads from a database view using a simple (non-paged) query.
type SQLViewDatasource struct {
	id     string
	src    SQLSource
	driver string
	dsn    string
	params map[string]any
}

func newSQLViewDatasource(config map[string]any, params map[string]any) (Datasource, error) {
	config = ApplyParams(config, params)
	id, err := ConfigString(config, "id")
	if err != nil {
		return nil, err
	}
	driver, err := ConfigString(config, "driver")
	if err != nil {
		return nil, fmt.Errorf("sql_view %s: %w", id, err)
	}
	dsn, err := ConfigString(config, "dsn")
	if err != nil {
		return nil, fmt.Errorf("sql_view %s: %w", id, err)
	}
	view, err := ConfigString(config, "view")
	if err != nil {
		return nil, fmt.Errorf("sql_view %s: %w", id, err)
	}
	idColumn := ConfigStringOr(config, "id_column", "id")
	seqColumn := ConfigStringOr(config, "seq_column", "")
	where := ConfigStringOr(config, "where", "")
	args := buildQueryArgs(config)

	query := fmt.Sprintf("SELECT * FROM %s", view)
	if where != "" {
		query += " WHERE " + where
	}

	cols, err := parseColumnBindings(config)
	if err != nil {
		return nil, fmt.Errorf("sql_view %s: %w", id, err)
	}

	return &SQLViewDatasource{
		id:     id,
		driver: driver,
		dsn:    dsn,
		params: params,
		src: SQLSource{
			Query:     query,
			Args:      args,
			IDColumn:  idColumn,
			SeqColumn: seqColumn,
			Columns:   cols,
		},
	}, nil
}

func (d *SQLViewDatasource) ID() string   { return d.id }
func (d *SQLViewDatasource) Type() string { return "sql_view" }

func (d *SQLViewDatasource) Validate() error {
	if d.driver == "" {
		return errors.New("sql_view: driver required")
	}
	if d.dsn == "" {
		return errors.New("sql_view: dsn required")
	}
	return nil
}

func (d *SQLViewDatasource) Open(ctx context.Context) (Cursor, error) {
	db, err := sql.Open(d.driver, d.dsn)
	if err != nil {
		return nil, fmt.Errorf("sql_view %s: open: %w", d.id, err)
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("sql_view %s: ping: %w", d.id, err)
	}
	d.src.DB = db
	cur, err := d.src.Open(ctx)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("sql_view %s: open cursor: %w", d.id, err)
	}
	return &dbClosingCursor{cur: cur, db: db}, nil
}

// ---------------------------------------------------------------------------
// sql_query — Arbitrary SQL query datasource (joins, CTEs, etc.).
// ---------------------------------------------------------------------------

// SQLQueryDatasource executes an arbitrary SQL statement. It supports parameterized
// queries via the args config key.
type SQLQueryDatasource struct {
	id     string
	src    SQLQuerySource
	driver string
	dsn    string
	params map[string]any
}

func newSQLQueryDatasource(config map[string]any, params map[string]any) (Datasource, error) {
	config = ApplyParams(config, params)
	id, err := ConfigString(config, "id")
	if err != nil {
		return nil, err
	}
	driver, err := ConfigString(config, "driver")
	if err != nil {
		return nil, fmt.Errorf("sql_query %s: %w", id, err)
	}
	dsn, err := ConfigString(config, "dsn")
	if err != nil {
		return nil, fmt.Errorf("sql_query %s: %w", id, err)
	}
	queryStr, err := ConfigString(config, "query")
	if err != nil {
		return nil, fmt.Errorf("sql_query %s: %w", id, err)
	}
	queryFile := ConfigStringOr(config, "query_file", "")
	if queryFile != "" {
		b, readErr := os.ReadFile(queryFile)
		if readErr != nil {
			return nil, fmt.Errorf("sql_query %s: read query_file: %w", id, readErr)
		}
		queryStr = string(b)
	}
	idColumn := ConfigStringOr(config, "id_column", "id")
	seqColumn := ConfigStringOr(config, "seq_column", "")
	args := buildQueryArgs(config)

	cols, err := parseColumnBindings(config)
	if err != nil {
		return nil, fmt.Errorf("sql_query %s: %w", id, err)
	}

	return &SQLQueryDatasource{
		id:     id,
		driver: driver,
		dsn:    dsn,
		params: params,
		src: SQLQuerySource{
			Query:     queryStr,
			Args:      args,
			IDColumn:  idColumn,
			SeqColumn: seqColumn,
			Columns:   cols,
		},
	}, nil
}

func (d *SQLQueryDatasource) ID() string   { return d.id }
func (d *SQLQueryDatasource) Type() string { return "sql_query" }

func (d *SQLQueryDatasource) Validate() error {
	if d.driver == "" {
		return errors.New("sql_query: driver required")
	}
	if d.dsn == "" {
		return errors.New("sql_query: dsn required")
	}
	if d.src.Query == "" {
		return errors.New("sql_query: query required")
	}
	return nil
}

func (d *SQLQueryDatasource) Open(ctx context.Context) (Cursor, error) {
	db, err := sql.Open(d.driver, d.dsn)
	if err != nil {
		return nil, fmt.Errorf("sql_query %s: open: %w", d.id, err)
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("sql_query %s: ping: %w", d.id, err)
	}
	d.src.DB = db
	cur, err := d.src.Open(ctx)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("sql_query %s: open cursor: %w", d.id, err)
	}
	return &dbClosingCursor{cur: cur, db: db}, nil
}

// ---------------------------------------------------------------------------
// sql_file — SQL from .sql file with parameter interpolation.
// ---------------------------------------------------------------------------

// SQLFileDatasource reads a .sql file and executes it as a query. Supports
// parameter substitution via ${param_name} placeholders in the SQL.
type SQLFileDatasource struct {
	id       string
	filePath string
	driver   string
	dsn      string
	columns  []SQLColumn
	idColumn string
	seqCol   string
	params   map[string]any
	rawArgs  []any
}

func newSQLFileDatasource(config map[string]any, params map[string]any) (Datasource, error) {
	config = ApplyParams(config, params)
	id, err := ConfigString(config, "id")
	if err != nil {
		return nil, err
	}
	driver, err := ConfigString(config, "driver")
	if err != nil {
		return nil, fmt.Errorf("sql_file %s: %w", id, err)
	}
	dsn, err := ConfigString(config, "dsn")
	if err != nil {
		return nil, fmt.Errorf("sql_file %s: %w", id, err)
	}
	filePath, err := ConfigString(config, "file")
	if err != nil {
		return nil, fmt.Errorf("sql_file %s: %w", id, err)
	}
	idColumn := ConfigStringOr(config, "id_column", "id")
	seqColumn := ConfigStringOr(config, "seq_column", "")
	rawArgs := buildQueryArgs(config)

	cols, err := parseColumnBindings(config)
	if err != nil {
		return nil, fmt.Errorf("sql_file %s: %w", id, err)
	}

	return &SQLFileDatasource{
		id:       id,
		filePath: filePath,
		driver:   driver,
		dsn:      dsn,
		columns:  cols,
		idColumn: idColumn,
		seqCol:   seqColumn,
		params:   params,
		rawArgs:  rawArgs,
	}, nil
}

func (d *SQLFileDatasource) ID() string   { return d.id }
func (d *SQLFileDatasource) Type() string { return "sql_file" }

func (d *SQLFileDatasource) Validate() error {
	if d.driver == "" {
		return errors.New("sql_file: driver required")
	}
	if d.dsn == "" {
		return errors.New("sql_file: dsn required")
	}
	if d.filePath == "" {
		return errors.New("sql_file: file required")
	}
	return nil
}

func (d *SQLFileDatasource) Open(ctx context.Context) (Cursor, error) {
	b, err := os.ReadFile(d.filePath)
	if err != nil {
		return nil, fmt.Errorf("sql_file %s: read: %w", d.id, err)
	}
	query := interpolateParams(string(b), d.params)

	db, err := sql.Open(d.driver, d.dsn)
	if err != nil {
		return nil, fmt.Errorf("sql_file %s: open: %w", d.id, err)
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("sql_file %s: ping: %w", d.id, err)
	}

	src := SQLSource{
		Query:     query,
		Args:      d.rawArgs,
		IDColumn:  d.idColumn,
		SeqColumn: d.seqCol,
		Columns:   d.columns,
	}
	cur, err := src.Open(ctx)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("sql_file %s: open cursor: %w", d.id, err)
	}
	return &dbClosingCursor{cur: cur, db: db}, nil
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

// dbClosingCursor wraps a Cursor and closes the underlying *sql.DB when done.
type dbClosingCursor struct {
	cur Cursor
	db  *sql.DB
}

func (c *dbClosingCursor) Next(ctx context.Context, dst *SourceRecord) bool {
	return c.cur.Next(ctx, dst)
}
func (c *dbClosingCursor) Err() error { return c.cur.Err() }
func (c *dbClosingCursor) Close() error {
	err := c.cur.Close()
	c.db.Close()
	return err
}

// parseColumnBindings parses the "columns" list from BCL config into SQLColumn
// bindings. Each item is a map with keys: column, field, kind, normalized, layout.
func parseColumnBindings(config map[string]any) ([]SQLColumn, error) {
	list, err := ConfigList(config, "columns")
	if err != nil {
		return nil, err
	}
	if list == nil {
		return nil, nil
	}
	out := make([]SQLColumn, 0, len(list))
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
		out = append(out, SQLColumn{
			Column:     col,
			Field:      FieldID(0), // resolved later by the index
			Kind:       parseValueKind(kind),
			Normalized: normalized,
			Layout:     layout,
		})
	}
	return out, nil
}

// buildQueryArgs extracts positional query arguments from the config "args" list.
func buildQueryArgs(config map[string]any) []any {
	list, err := ConfigList(config, "args")
	if err != nil || list == nil {
		return nil
	}
	return list
}

// interpolateParams replaces ${name} placeholders in a string with values from params.
func interpolateParams(s string, params map[string]any) string {
	if len(params) == 0 || !strings.Contains(s, "${") {
		return s
	}
	for k, v := range params {
		placeholder := "${" + k + "}"
		replacement := fmt.Sprint(v)
		s = strings.ReplaceAll(s, placeholder, replacement)
	}
	return s
}

// ResolveSQLColumns resolves SQLColumn Field values using an Index's field resolver.
// Call this after the index schema is compiled and field IDs are available.
func ResolveSQLColumns(cols []SQLColumn, fieldResolver func(string) FieldID) []SQLColumn {
	out := make([]SQLColumn, len(cols))
	for i, c := range cols {
		out[i] = c
		out[i].Field = fieldResolver(c.Column)
	}
	return out
}
