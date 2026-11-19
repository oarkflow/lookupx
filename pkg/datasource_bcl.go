package pkg

import (
	"fmt"
	"strings"

	"github.com/oarkflow/bcl"
)

// This file contains the BCL adapter and query-object normalization helpers.
func unmarshalBCLRaw(src []byte, v any) error {
	return bcl.Unmarshal(src, v)
}

// marshalBCL serializes a Go value to BCL format.
func marshalBCL(v any) ([]byte, error) {
	return bcl.Marshal(v)
}

// unmarshalBCLOptions unmarshals BCL with custom options.
func unmarshalBCLOptions(src []byte, v any, opts *bcl.Options) error {
	return bcl.UnmarshalWithOptions(src, v, opts)
}

// applyBCLQueryObjects maps the explicit query { string "SELECT ..." } and
// query { file "queries/file.sql" } forms into the legacy datasource fields
// used by factories. Keeping the query object in BCL makes the source clear
// while preserving compatibility with existing datasource implementations.
func applyBCLQueryObjects(src []byte, cfg *LookupXConfig) error {
	doc, err := bcl.Parse(src)
	if err != nil {
		return fmt.Errorf("parse BCL query objects: %w", err)
	}
	byID := make(map[string]*DatasourceDef, len(cfg.Datasources))
	for i := range cfg.Datasources {
		byID[cfg.Datasources[i].ID] = &cfg.Datasources[i]
	}
	for _, node := range doc.Items {
		dsBlock, ok := node.(*bcl.Block)
		if !ok || !strings.EqualFold(dsBlock.Type, "datasource") {
			continue
		}
		ds := byID[dsBlock.ID]
		if ds == nil {
			continue
		}
		for _, child := range dsBlock.Body {
			var queryFields []bcl.Node
			switch item := child.(type) {
			case *bcl.Block:
				if !strings.EqualFold(item.Type, "query") {
					continue
				}
				queryFields = item.Body
			case *bcl.Assignment:
				if !strings.EqualFold(item.Name, "query") {
					continue
				}
				object, ok := item.Value.(*bcl.Object)
				if !ok {
					continue // Legacy query "SELECT ..." assignment.
				}
				if ds.Kind != "sql_query" {
					return fmt.Errorf("datasource %q uses a query object but kind is %q; query objects require sql_query", ds.ID, ds.Kind)
				}
				if ds.QuerySpec != nil {
					return fmt.Errorf("datasource %q defines more than one query object", ds.ID)
				}
				// bcl.Unmarshal sees the object as a string representation for the
				// legacy Query field; clear that representation before normalizing.
				ds.Query = ""
				queryFields = object.Fields
			default:
				continue
			}
			var kind, value string
			for _, entry := range queryFields {
				assignment, ok := entry.(*bcl.Assignment)
				if !ok {
					return fmt.Errorf("datasource %q query object only accepts file or string fields", ds.ID)
				}
				if !strings.EqualFold(assignment.Name, "file") && !strings.EqualFold(assignment.Name, "string") {
					return fmt.Errorf("datasource %q query object has unknown field %q; use file or string", ds.ID, assignment.Name)
				}
				literal, ok := assignment.Value.(*bcl.Literal)
				if !ok || literal.Type != "string" {
					return fmt.Errorf("datasource %q query %q must be a string literal", ds.ID, assignment.Name)
				}
				text, ok := literal.Data.(string)
				if !ok || strings.TrimSpace(text) == "" {
					return fmt.Errorf("datasource %q query %q cannot be empty", ds.ID, assignment.Name)
				}
				if len(text) > 1<<20 {
					return fmt.Errorf("datasource %q query exceeds the 1 MiB limit", ds.ID)
				}
				if kind != "" {
					return fmt.Errorf("datasource %q query must define exactly one of file or string", ds.ID)
				}
				kind, value = strings.ToLower(assignment.Name), text
			}
			if kind == "" {
				return fmt.Errorf("datasource %q query must define file or string", ds.ID)
			}
			if ds.QueryFile != "" {
				return fmt.Errorf("datasource %q cannot combine query object with query/query_file", ds.ID)
			}
			if kind == "file" {
				ds.QuerySpec = &SQLQuerySpec{File: value}
				ds.QueryFile = value
			} else {
				ds.QuerySpec = &SQLQuerySpec{String: value}
				ds.Query = value
			}
		}
	}
	return nil
}
