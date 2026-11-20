package pkg

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/oarkflow/squealx"
)

// ---------------------------------------------------------------------------
// sql_table — Keyset-paginated table scan datasource.
// ---------------------------------------------------------------------------

// SQLTableDatasource streams a database table using keyset pagination. It wraps
// PagedSQLSource with config-driven column binding and parameter support.
type SQLTableDatasource struct {
	id     string
	src    PagedSQLSource
	driver string
	dsn    string
	params map[string]any
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
	pageSize := ConfigIntOr(config, "page_size", 10000)
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
	return d.open(ctx, 0)
}

// OpenAfter resumes a table source after its separate numeric order/sequence
// key. A document ID used directly as the order key may be textual, so it is
// not accepted as a durable numeric resume cursor.
func (d *SQLTableDatasource) OpenAfter(ctx context.Context, seq uint64) (Cursor, error) {
	if d.src.OrderColumn == "" || d.src.OrderColumn == d.src.IDColumn || d.src.SeqColumn != d.src.OrderColumn {
		return nil, errors.New("sql_table: restart resume requires a separate numeric order_column matching seq_column")
	}
	return d.open(ctx, seq)
}

func (d *SQLTableDatasource) ResumeIdentity() string {
	if d.src.OrderColumn == "" || d.src.OrderColumn == d.src.IDColumn || d.src.SeqColumn != d.src.OrderColumn {
		return ""
	}
	identity, err := json.Marshal(struct {
		ID, Driver, DSN, Table, Where, OrderColumn, IDColumn, SeqColumn string
		PageSize                                                        int
		Columns                                                         []string
		Bindings                                                        []SQLColumn
	}{d.id, d.driver, d.dsn, d.src.Table, d.src.Where, d.src.OrderColumn, d.src.IDColumn, d.src.SeqColumn, d.src.PageSize, d.src.Columns, d.src.ColumnBindings})
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(identity)
	return hex.EncodeToString(sum[:])
}

func (d *SQLTableDatasource) open(ctx context.Context, startAfter uint64) (Cursor, error) {
	db, err := squealx.Connect(squealxDriver(d.driver), d.dsn, "")
	if err != nil {
		return nil, fmt.Errorf("sql_table %s: open: %w", d.id, err)
	}
	configureBulkSQLPool(db.DB())
	source := d.src
	source.DB = db.DB()
	source.StartAfter = startAfter
	cur, err := source.Open(ctx)
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
	db, err := squealx.Connect(squealxDriver(d.driver), d.dsn, "")
	if err != nil {
		return nil, fmt.Errorf("sql_view %s: open: %w", d.id, err)
	}
	configureBulkSQLPool(db.DB())
	d.src.DB = db.DB()
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
	id       string
	src      SQLQuerySource
	pageSize int
	driver   string
	dsn      string
	params   map[string]any
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
	queryStr := ConfigStringOr(config, "query", "")
	queryFile := ConfigStringOr(config, "query_file", "")
	if strings.TrimSpace(queryStr) == "" && strings.TrimSpace(queryFile) == "" {
		return nil, fmt.Errorf("sql_query %s: query string or query_file is required", id)
	}
	if strings.TrimSpace(queryStr) != "" && strings.TrimSpace(queryFile) != "" {
		return nil, fmt.Errorf("sql_query %s: query and query_file cannot both be set", id)
	}
	if queryFile != "" {
		info, statErr := os.Stat(queryFile)
		if statErr != nil {
			return nil, fmt.Errorf("sql_query %s: stat query_file: %w", id, statErr)
		}
		if !info.Mode().IsRegular() || info.Size() > 1<<20 {
			return nil, fmt.Errorf("sql_query %s: query_file must be a regular file no larger than 1 MiB", id)
		}
		b, readErr := os.ReadFile(queryFile)
		if readErr != nil {
			return nil, fmt.Errorf("sql_query %s: read query_file: %w", id, readErr)
		}
		queryStr = string(b)
	}
	if err := validateSelectSQL(queryStr); err != nil {
		return nil, fmt.Errorf("sql_query %s: %w", id, err)
	}
	idColumn := ConfigStringOr(config, "id_column", "id")
	seqColumn := ConfigStringOr(config, "seq_column", "")
	pageSize := ConfigIntOr(config, "page_size", 0)
	if seqColumn != "" && pageSize == 0 {
		pageSize = 10000
	}
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
		pageSize: pageSize,
	}, nil
}

// validateSelectSQL accepts a single SELECT or CTE-backed SELECT statement.
// It ignores leading SQL comments and semicolons inside quoted strings.
func validateSelectSQL(query string) error {
	query = strings.TrimSpace(query)
	for {
		switch {
		case strings.HasPrefix(query, "--") || strings.HasPrefix(query, "#"):
			end := strings.IndexByte(query, '\n')
			if end < 0 {
				return fmt.Errorf("query must contain a SELECT statement")
			}
			query = strings.TrimSpace(query[end+1:])
		case strings.HasPrefix(query, "/*"):
			end := strings.Index(query[2:], "*/")
			if end < 0 {
				return fmt.Errorf("query has an unterminated SQL comment")
			}
			query = strings.TrimSpace(query[end+4:])
		default:
			goto commentsDone
		}
	}
commentsDone:
	upper := strings.ToUpper(query)
	keyword := ""
	for _, candidate := range []string{"SELECT", "WITH"} {
		if strings.HasPrefix(upper, candidate) && (len(query) == len(candidate) || query[len(candidate)] == ' ' || query[len(candidate)] == '\n' || query[len(candidate)] == '\t' || query[len(candidate)] == '(') {
			keyword = candidate
			break
		}
	}
	if keyword == "" {
		return fmt.Errorf("query must be a single SELECT statement (WITH is allowed)")
	}
	if keyword == "WITH" && !hasTopLevelSelect(query) {
		return fmt.Errorf("WITH query must end in a SELECT statement")
	}
	quote := byte(0)
	for i := 0; i < len(query); i++ {
		ch := query[i]
		if quote != 0 {
			if ch == '\\' && i+1 < len(query) {
				i++
				continue
			}
			if ch == quote {
				if i+1 < len(query) && query[i+1] == quote {
					i++
					continue
				}
				quote = 0
			}
			continue
		}
		if ch == '\'' || ch == '"' || ch == '`' {
			quote = ch
			continue
		}
		if ch == ';' && strings.TrimSpace(query[i+1:]) != "" {
			return fmt.Errorf("query must contain only one SELECT statement")
		}
	}
	return nil
}

func hasTopLevelSelect(query string) bool {
	depth, quote := 0, byte(0)
	for i := 0; i < len(query); {
		ch := query[i]
		if quote != 0 {
			if ch == '\\' && i+1 < len(query) {
				i += 2
				continue
			}
			if ch == quote {
				if i+1 < len(query) && query[i+1] == quote {
					i += 2
					continue
				}
				quote = 0
			}
			i++
			continue
		}
		if ch == '\'' || ch == '"' || ch == '`' {
			quote = ch
			i++
			continue
		}
		if ch == '-' && i+1 < len(query) && query[i+1] == '-' {
			if end := strings.IndexByte(query[i+2:], '\n'); end >= 0 {
				i += end + 3
				continue
			}
			return false
		}
		if ch == '/' && i+1 < len(query) && query[i+1] == '*' {
			if end := strings.Index(query[i+2:], "*/"); end >= 0 {
				i += end + 4
				continue
			}
			return false
		}
		if ch == '(' {
			depth++
			i++
			continue
		}
		if ch == ')' {
			if depth > 0 {
				depth--
			}
			i++
			continue
		}
		if depth == 0 && (ch == 's' || ch == 'S') && i+6 <= len(query) && strings.EqualFold(query[i:i+6], "SELECT") {
			beforeOK := i == 0 || !isSQLIdentByte(query[i-1])
			afterOK := i+6 == len(query) || !isSQLIdentByte(query[i+6])
			if beforeOK && afterOK {
				return true
			}
		}
		i++
	}
	return false
}

func isSQLIdentByte(ch byte) bool {
	return ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '_'
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
	if d.pageSize < 0 {
		return errors.New("sql_query: page_size cannot be negative")
	}
	if d.pageSize > 0 && d.src.SeqColumn == "" {
		return errors.New("sql_query: page_size requires a stable numeric seq_column")
	}
	if d.pageSize > 0 && d.src.IDColumn == "" {
		return errors.New("sql_query: keyset paging requires an id_column for stable document IDs")
	}
	if d.pageSize > 0 && !validSQLIdentifier(d.src.SeqColumn) {
		return fmt.Errorf("sql_query: seq_column %q must be a simple SELECT output identifier for keyset paging", d.src.SeqColumn)
	}
	if d.pageSize > 0 && hasSQLPageTokens(d.src.Query) {
		if err := validateSQLPageTemplate(d.src.Query, d.src.SeqColumn); err != nil {
			return fmt.Errorf("sql_query: %w", err)
		}
	}
	return nil
}

func (d *SQLQueryDatasource) Open(ctx context.Context) (Cursor, error) {
	return d.open(ctx, 0)
}

// OpenAfter resumes a configured keyset-paged query after the last committed
// sequence. Unpaged SQL queries cannot safely resume after a process restart.
func (d *SQLQueryDatasource) OpenAfter(ctx context.Context, seq uint64) (Cursor, error) {
	if d.pageSize <= 0 {
		return nil, errors.New("sql_query: restart resume requires seq_column and page_size")
	}
	return d.open(ctx, seq)
}

// ResumeIdentity fingerprints the paged SQL source without persisting its DSN
// or query arguments in the checkpoint file.
func (d *SQLQueryDatasource) ResumeIdentity() string {
	if d.pageSize <= 0 || strings.TrimSpace(d.src.SeqColumn) == "" {
		return ""
	}
	identity, err := json.Marshal(struct {
		ID, Driver, DSN, Query, IDColumn, SeqColumn string
		PageSize                                    int
		Args                                        []any
	}{d.id, d.driver, d.dsn, d.src.Query, d.src.IDColumn, d.src.SeqColumn, d.pageSize, d.src.Args})
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(identity)
	return hex.EncodeToString(sum[:])
}

func (d *SQLQueryDatasource) open(ctx context.Context, startAfter uint64) (Cursor, error) {
	db, err := squealx.Connect(squealxDriver(d.driver), d.dsn, "")
	if err != nil {
		return nil, fmt.Errorf("sql_query %s: open: %w", d.id, err)
	}
	source := d.src
	source.DB = db.DB()
	configureBulkSQLPool(source.DB)
	var cur Cursor
	if d.pageSize > 0 {
		baseQuery := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(source.Query), ";"))
		page := func(lastSeq uint64, limit int) (string, []any) {
			return buildSQLQueryPageForSource(baseQuery, source.Args, source.SeqColumn, d.driver, lastSeq, limit)
		}
		cur, err = (PagedSQLQuerySource{DB: source.DB, Page: page, IDColumn: source.IDColumn, SeqColumn: source.SeqColumn, Columns: source.Columns, PageSize: d.pageSize, StartAfter: startAfter}).Open(ctx)
	} else {
		if startAfter > 0 {
			db.Close()
			return nil, errors.New("sql_query: restart resume requires seq_column and page_size")
		}
		cur, err = source.Open(ctx)
	}
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

	db, err := squealx.Connect(squealxDriver(d.driver), d.dsn, "")
	if err != nil {
		return nil, fmt.Errorf("sql_file %s: open: %w", d.id, err)
	}
	configureBulkSQLPool(db.DB())

	src := SQLSource{
		Query:     query,
		Args:      d.rawArgs,
		IDColumn:  d.idColumn,
		SeqColumn: d.seqCol,
		Columns:   d.columns,
	}
	src.DB = db.DB()
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

// dbClosingCursor wraps a Cursor and closes the underlying *squealx.DB when done.
type dbClosingCursor struct {
	cur Cursor
	db  *squealx.DB
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
		fieldName := ConfigStringOr(m, "target_field", ConfigStringOr(m, "field", col))
		out = append(out, SQLColumn{
			Column:     col,
			FieldName:  fieldName,
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
		name := c.FieldName
		if name == "" {
			name = c.Column
		}
		out[i].Field = fieldResolver(name)
	}
	return out
}

// squealxDriver maps user-facing driver names to squealx driver identifiers.
func squealxDriver(driver string) string {
	switch strings.ToLower(driver) {
	case "postgres", "postgresql":
		return "pgx"
	case "sqlite3":
		return "sqlite"
	default:
		return driver
	}
}

func configureBulkSQLPool(db *sql.DB) {
	// A bulk scan is deliberately single-stream per datasource. Keeping one
	// physical connection avoids accidental connection storms against RDS.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
}

func validSQLIdentifier(value string) bool {
	if value == "" || !(value[0] == '_' || value[0] >= 'A' && value[0] <= 'Z' || value[0] >= 'a' && value[0] <= 'z') {
		return false
	}
	for i := 1; i < len(value); i++ {
		ch := value[i]
		if !(ch == '_' || ch == '$' || ch >= 'A' && ch <= 'Z' || ch >= 'a' && ch <= 'z' || ch >= '0' && ch <= '9') {
			return false
		}
	}
	return true
}
