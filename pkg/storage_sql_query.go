package pkg

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// SQLDialect controls placeholder rendering for generated SQL. Raw SQLSource /
// SQLQuerySource never rewrite placeholders; use the native placeholders expected
// by the driver. Generated builders use this dialect.
type SQLDialect string

const (
	SQLDialectQuestion   SQLDialect = "question"    // ?, ?, ?  (MySQL, SQLite)
	SQLDialectPostgres   SQLDialect = "postgres"    // $1, $2, $3
	SQLDialectSQLServer  SQLDialect = "sqlserver"   // @p1, @p2, @p3
	SQLDialectNamedColon SQLDialect = "named_colon" // :p1, :p2, :p3
)

func (d SQLDialect) Placeholder(n int) string {
	if n <= 0 {
		n = 1
	}
	switch d {
	case SQLDialectPostgres:
		return fmt.Sprintf("$%d", n)
	case SQLDialectSQLServer:
		return fmt.Sprintf("@p%d", n)
	case SQLDialectNamedColon:
		return fmt.Sprintf(":p%d", n)
	default:
		return "?"
	}
}

// SQLQuerySource is a named alias over SQLSource intended for arbitrary SQL
// queries. Use this for joins, CTEs, views, filtered queries, stored projections,
// and driver-specific SQL. The query must return the configured IDColumn and any
// SQLColumn bindings.
//
// Example:
//
//	lookup.SQLQuerySource{
//	    DB: db,
//	    Query: `SELECT e.id, c.code AS term, e.group_id, e.date_key
//	            FROM records e JOIN codes c ON c.id=e.code_id
//	            WHERE e.deleted_at IS NULL AND e.partition_id = ?`,
//	    Args: []any{partitionID},
//	    IDColumn: "id",
//	    SeqColumn: "id",
//	    Columns: []lookup.SQLColumn{...},
//	}
type SQLQuerySource struct {
	Name      string
	DB        *sql.DB
	Query     string
	Args      []any
	IDColumn  string
	SeqColumn string
	Columns   []SQLColumn
	FetchSize int
}

func (s SQLQuerySource) Open(ctx context.Context) (Cursor, error) {
	if strings.TrimSpace(s.Query) == "" {
		return nil, errors.New("sql query required")
	}
	return SQLSource{DB: s.DB, Query: s.Query, Args: s.Args, IDColumn: s.IDColumn, SeqColumn: s.SeqColumn, Columns: s.Columns, FetchSize: s.FetchSize}.Open(ctx)
}

// SQLPageFunc builds one keyset page. lastSeq is the last sequence value that
// was indexed. limit is the requested page size. The returned query may be any
// SQL supported by database/sql: joins, CTEs, materialized views, subqueries, or
// database-specific syntax.
type SQLPageFunc func(lastSeq uint64, limit int) (query string, args []any)

// PagedSQLQuerySource streams arbitrary SQL queries with keyset pagination.
// Use this when the source rows come from joins/CTEs or when the pagination
// predicate cannot be expressed by PagedSQLSource's Table/Where fields.
type PagedSQLQuerySource struct {
	DB         *sql.DB
	Page       SQLPageFunc
	IDColumn   string
	SeqColumn  string
	Columns    []SQLColumn
	PageSize   int
	StartAfter uint64
	FetchSize  int
}

func (s PagedSQLQuerySource) Open(ctx context.Context) (Cursor, error) {
	if s.DB == nil {
		return nil, errors.New("nil sql DB")
	}
	if s.Page == nil {
		return nil, errors.New("sql page function required")
	}
	if s.PageSize <= 0 {
		s.PageSize = 100000
	}
	return &pagedSQLQueryCursor{src: s, last: s.StartAfter}, nil
}

type pagedSQLQueryCursor struct {
	src  PagedSQLQuerySource
	last uint64
	cur  Cursor
	done bool
	err  error
}

func (c *pagedSQLQueryCursor) Next(ctx context.Context, dst *SourceRecord) bool {
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
		query, args := c.src.Page(c.last, c.src.PageSize)
		if strings.TrimSpace(query) == "" {
			c.done = true
			return false
		}
		src := SQLSource{DB: c.src.DB, Query: query, Args: args, IDColumn: c.src.IDColumn, SeqColumn: c.src.SeqColumn, Columns: c.src.Columns, FetchSize: c.src.FetchSize}
		cur, err := src.Open(ctx)
		if err != nil {
			c.err = err
			return false
		}
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
func (c *pagedSQLQueryCursor) Err() error { return c.err }
func (c *pagedSQLQueryCursor) Close() error {
	if c.cur != nil {
		return c.cur.Close()
	}
	return nil
}

// SQLSelect builds safe SELECT statements for generated table ingestion. It is
// intentionally small and predictable; for complex SQL prefer SQLQuerySource or
// PagedSQLQuerySource.
type SQLSelect struct {
	Dialect SQLDialect
	Table   string
	Columns []string
	Where   []SQLWhere
	OrderBy string
	Limit   int
}

type SQLWhere struct {
	Column string
	Op     string
	Value  any
}

func (s SQLSelect) Build() (string, []any, error) {
	if strings.TrimSpace(s.Table) == "" {
		return "", nil, errors.New("table required")
	}
	cols := "*"
	if len(s.Columns) > 0 {
		cols = strings.Join(s.Columns, ", ")
	}
	var b strings.Builder
	b.Grow(128)
	b.WriteString("SELECT ")
	b.WriteString(cols)
	b.WriteString(" FROM ")
	b.WriteString(s.Table)
	args := make([]any, 0, len(s.Where))
	if len(s.Where) > 0 {
		b.WriteString(" WHERE ")
		for i, w := range s.Where {
			if i > 0 {
				b.WriteString(" AND ")
			}
			op := strings.TrimSpace(w.Op)
			if op == "" {
				op = "="
			}
			b.WriteString(w.Column)
			b.WriteByte(' ')
			b.WriteString(op)
			b.WriteByte(' ')
			args = append(args, w.Value)
			b.WriteString(s.Dialect.Placeholder(len(args)))
		}
	}
	if s.OrderBy != "" {
		b.WriteString(" ORDER BY ")
		b.WriteString(s.OrderBy)
	}
	if s.Limit > 0 {
		b.WriteString(" LIMIT ")
		b.WriteString(fmt.Sprint(s.Limit))
	}
	return b.String(), args, nil
}

// TupleSQLQuery builds a parameterized query for record lookup tables.
// It can be used for initial filtered indexing or small targeted rebuilds.
type TupleSQLQueryOptions struct {
	Dialect     SQLDialect
	Table       string
	Columns     []string
	PartitionID  any
	GroupID  any
	DateKey         any
	Term        any
	OrderColumn string
	Limit       int
}

func TupleSQLQuery(o TupleSQLQueryOptions) (string, []any, error) {
	if o.Table == "" {
		o.Table = "record_lookup"
	}
	if len(o.Columns) == 0 {
		o.Columns = []string{"id", "term", "group_id", "date_key", "partition_id"}
	}
	where := make([]SQLWhere, 0, 4)
	if o.PartitionID != nil {
		where = append(where, SQLWhere{Column: "partition_id", Op: "=", Value: o.PartitionID})
	}
	if o.GroupID != nil {
		where = append(where, SQLWhere{Column: "group_id", Op: "=", Value: o.GroupID})
	}
	if o.DateKey != nil {
		where = append(where, SQLWhere{Column: "date_key", Op: "=", Value: o.DateKey})
	}
	if o.Term != nil {
		where = append(where, SQLWhere{Column: "term", Op: "=", Value: o.Term})
	}
	order := o.OrderColumn
	if order == "" {
		order = "id"
	}
	return SQLSelect{Dialect: o.Dialect, Table: o.Table, Columns: o.Columns, Where: where, OrderBy: order, Limit: o.Limit}.Build()
}

// TuplePagedSQLQuery returns a page function for large record query
// ingestion. The generated SQL uses keyset pagination on orderColumn and can be
// combined with fixed filters such as partition/group/date.
func TuplePagedSQLQuery(dialect SQLDialect, table string, columns []string, orderColumn string, fixedWhere []SQLWhere) SQLPageFunc {
	if table == "" {
		table = "record_lookup"
	}
	if len(columns) == 0 {
		columns = []string{"id", "term", "group_id", "date_key", "partition_id"}
	}
	if orderColumn == "" {
		orderColumn = "id"
	}
	return func(lastSeq uint64, limit int) (string, []any) {
		where := make([]SQLWhere, 0, len(fixedWhere)+1)
		where = append(where, fixedWhere...)
		if lastSeq > 0 {
			where = append(where, SQLWhere{Column: orderColumn, Op: ">", Value: lastSeq})
		}
		q, args, _ := SQLSelect{Dialect: dialect, Table: table, Columns: columns, Where: where, OrderBy: orderColumn + " ASC", Limit: limit}.Build()
		return q, args
	}
}
