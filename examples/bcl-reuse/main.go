// Command bcl-reuse verifies BCL variable references and reusable block spreads.
package main

import (
	"fmt"
	"os"

	"github.com/oarkflow/bcl"
)

type config struct {
	Datasources []datasource `bcl:"datasource,block"`
	Indexes     []index      `bcl:"index,block"`
}

type datasource struct {
	ID      string   `bcl:",id"`
	Kind    string   `bcl:"kind"`
	DSN     string   `bcl:"dsn"`
	Columns []column `bcl:"column,block"`
}

type index struct {
	ID          string        `bcl:",id"`
	Schema      string        `bcl:"schema"`
	Datasource  string        `bcl:"datasource"`
	Fields      []schemaField `bcl:"schema_field,block"`
	LookupRules []lookupRule  `bcl:"lookup_rule,block"`
	Bulk        bulkSettings  `bcl:"bulk"`
}

type column struct {
	ID         string `bcl:",id"`
	Kind       string `bcl:"kind"`
	Normalized bool   `bcl:"normalized"`
}

type schemaField struct {
	ID         string `bcl:",id"`
	Kind       string `bcl:"kind"`
	Indexed    bool   `bcl:"indexed"`
	Stored     bool   `bcl:"stored"`
	Lookup     bool   `bcl:"lookup"`
	Lowercase  bool   `bcl:"lowercase"`
	Prefix     bool   `bcl:"prefix"`
	TermSearch bool   `bcl:"term_search"`
	IndexAlias string `bcl:"index_alias"`
}

type lookupRule struct {
	ID          string `bcl:",id"`
	TargetField string `bcl:"target_field"`
	Operator    string `bcl:"operator"`
}

type bulkSettings struct {
	BatchSize       int    `bcl:"batch_size"`
	CheckpointEvery int    `bcl:"checkpoint_every"`
	CheckpointPath  string `bcl:"checkpoint_path"`
	Resume          bool   `bcl:"resume"`
}

func main() {
	path := "examples/bcl-reuse/config.bcl"
	if len(os.Args) > 1 {
		path = os.Args[1]
	}
	data, err := os.ReadFile(path)
	if err != nil {
		fail("read config", err)
	}

	var cfg config
	if err := bcl.Unmarshal(data, &cfg); err != nil {
		fail("parse config", err)
	}
	if len(cfg.Datasources) == 0 || len(cfg.Indexes) == 0 {
		fail("verify config", fmt.Errorf("found %d datasources and %d indexes", len(cfg.Datasources), len(cfg.Indexes)))
	}

	wantDSN := os.Getenv("MYSQL_DSN")
	if wantDSN == "" {
		wantDSN = "example:example@tcp(127.0.0.1:3306)/lookupx?parseTime=true&loc=UTC"
	}
	for _, ds := range cfg.Datasources {
		if ds.DSN != wantDSN {
			fail("verify DSN", fmt.Errorf("datasource %q got %q, want %q", ds.ID, ds.DSN, wantDSN))
		}
		columns := make(map[string]column, len(ds.Columns))
		for _, col := range ds.Columns {
			columns[col.ID] = col
		}
		for _, id := range []string{"code", "description"} {
			if _, ok := columns[id]; !ok {
				fail("verify shared datasource columns", fmt.Errorf("datasource %q missing %q", ds.ID, id))
			}
		}
		if path == "examples/bcl-reuse/config.bcl" && ds.ID == "second" {
			if _, ok := columns["active"]; !ok {
				fail("verify datasource-local column", fmt.Errorf("datasource %q missing active", ds.ID))
			}
		}
	}

	for _, idx := range cfg.Indexes {
		if idx.Schema != "record" {
			fail("verify shared index defaults", fmt.Errorf("index %q schema is %q", idx.ID, idx.Schema))
		}
		fields := make(map[string]schemaField, len(idx.Fields))
		for _, field := range idx.Fields {
			fields[field.ID] = field
		}
		for _, id := range []string{"code", "code_no_period"} {
			field, ok := fields[id]
			if !ok || field.Kind != "keyword" || !field.Indexed || !field.Stored || !field.TermSearch {
				fail("verify shared schema fields", fmt.Errorf("index %q has invalid or missing field %q: %#v", idx.ID, id, field))
			}
		}
		for _, id := range []string{"description", "fulltext"} {
			field, ok := fields[id]
			if !ok || field.Kind != "text" || !field.Indexed || !field.Stored || !field.TermSearch {
				fail("verify shared schema fields", fmt.Errorf("index %q has invalid or missing field %q: %#v", idx.ID, id, field))
			}
		}
		wantAlias := ""
		if expectsDescriptionAlias(idx.ID) {
			wantAlias = "fulltext"
		}
		if got := fields["description"].IndexAlias; got != wantAlias {
			fail("verify schema field variant", fmt.Errorf("index %q description alias is %q, want %q", idx.ID, got, wantAlias))
		}
		if idx.Bulk.BatchSize != 32768 && idx.Bulk.BatchSize != 16384 {
			fail("verify shared bulk settings", fmt.Errorf("index %q has batch_size %d", idx.ID, idx.Bulk.BatchSize))
		}
		if expectsDOSRule(idx.ID) {
			found := false
			for _, rule := range idx.LookupRules {
				if rule.ID == "DOS" && rule.TargetField == "effective_date" && rule.Operator == "gte" {
					found = true
				}
			}
			if !found {
				fail("verify shared lookup rule", fmt.Errorf("index %q is missing the DOS >= effective_date rule", idx.ID))
			}
		}
		if idx.ID == "first_index" && idx.Bulk.CheckpointPath != "./data/first.json" {
			fail("verify bulk-local setting", fmt.Errorf("checkpoint path is %q", idx.Bulk.CheckpointPath))
		}
		if idx.ID == "second_index" && idx.Bulk.BatchSize != 16384 {
			fail("verify bulk-local setting", fmt.Errorf("second index batch_size is %d", idx.Bulk.BatchSize))
		}
	}

	fmt.Printf("OK: %d datasources and %d indexes resolve shared variables, columns, schema fields, defaults, and bulk settings\n", len(cfg.Datasources), len(cfg.Indexes))
}

func expectsDescriptionAlias(id string) bool {
	switch id {
	case "first_index", "cpt", "hcpcs", "special-fac", "special-pro":
		return true
	default:
		return false
	}
}

func expectsDOSRule(id string) bool {
	switch id {
	case "first_index", "cpt", "hcpcs", "dx-icd10", "dx-icd10-pcs", "special-fac", "special-pro":
		return true
	default:
		return false
	}
}

func fail(step string, err error) {
	fmt.Fprintf(os.Stderr, "%s: %v\n", step, err)
	os.Exit(1)
}
