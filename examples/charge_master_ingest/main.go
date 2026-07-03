// Command charge_master_ingest indexes tbl_charge_master from MySQL into a
// lookupx index and runs example searches.
//
// This example is intentionally configured for the 1M+ charge-master workload:
// it does not store source documents, does not build text-prefix/fuzzy indexes
// for long descriptions, and reads MySQL with keyset pages instead of one huge
// unbounded result set.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	lookup "github.com/oarkflow/lookupx/pkg"
)

const chargeMasterPageQuery = `
select tbl_charge_master.charge_master_uid AS id,
       tbl_charge_master.client_proc_desc AS ld,
       tbl_charge_master.cpt_hcpcs_code AS cpt_code,
       tbl_charge_master.effective_date AS effective_date,
       tbl_charge_master.end_effective_date AS end_effective_date,
       tbl_charge_master.work_item_uid AS work_item,
       tbl_charge_master.patient_status_uid AS patient_status,
       tbl_charge_master.charge_type AS charge_type
from tbl_charge_master
where tbl_charge_master.cpt_hcpcs_code not in
      ('99281', '99282', '99283', '99284', '99285', '99291')
  and tbl_charge_master.charge_master_uid > ?
order by tbl_charge_master.charge_master_uid asc
limit ?`

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func buildDSN() string {
	if dsn := os.Getenv("CM_MYSQL_DSN"); dsn != "" {
		return dsn
	}
	host := env("CM_MYSQL_HOST", "192.168.18.29")
	port := env("CM_MYSQL_PORT", "3306")
	user := env("CM_MYSQL_USER", "service")
	pass := env("CM_MYSQL_PASS", "Password@123")
	dbName := env("CM_MYSQL_DB", "cleardb")
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&timeout=10s&readTimeout=5m&writeTimeout=60s",
		user, pass, host, port, dbName)
}

func main() {
	db, err := sql.Open("mysql", buildDSN())
	if err != nil {
		log.Fatalf("open mysql: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(10 * time.Minute)

	pingCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		log.Fatalf("mysql ping failed: %v", err)
	}

	ix, err := lookup.New(lookup.Config{
		InitialCapacity: 1_600_000,
		DisableSource:   true,
		Clock:           lookup.SystemClock{},
		Schema: lookup.Schema{Fields: map[string]lookup.FieldOptions{
			// Do NOT enable Prefix/Fuzzy on this long text field during ingest.
			// Prefix/fuzzy should be a query-time feature or a separate suggester index.
			"ld": {Kind: lookup.FieldText, Indexed: true, Lowercase: true},

			// Prefix is useful and cheap on short CPT/HCPCS codes.
			"cpt_code":           {Kind: lookup.FieldKeyword, Lookup: true, Prefix: true, Lowercase: true},
			"effective_date":     {Kind: lookup.FieldKeyword, Lookup: true},
			"end_effective_date": {Kind: lookup.FieldKeyword, Lookup: true},
			"work_item":          {Kind: lookup.FieldKeyword, Lookup: true, Lowercase: true},
			"patient_status":     {Kind: lookup.FieldKeyword, Lookup: true, Lowercase: true},
			"charge_type":        {Kind: lookup.FieldKeyword, Lookup: true, Lowercase: true},
		}},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer ix.Close()

	ld := ix.FieldID("ld")
	cptCode := ix.FieldID("cpt_code")
	effectiveDate := ix.FieldID("effective_date")
	endEffectiveDate := ix.FieldID("end_effective_date")
	workItem := ix.FieldID("work_item")
	patientStatus := ix.FieldID("patient_status")
	chargeType := ix.FieldID("charge_type")

	src := lookup.PagedSQLQuerySource{
		DB:       db,
		PageSize: 50_000,
		Page: func(lastSeq uint64, limit int) (string, []any) {
			return chargeMasterPageQuery, []any{lastSeq, limit}
		},
		IDColumn:  "id",
		SeqColumn: "id",
		Columns: []lookup.SQLColumn{
			{Column: "ld", Field: ld, Kind: lookup.ValueText},
			{Column: "cpt_code", Field: cptCode, Kind: lookup.ValueKeyword},
			{Column: "effective_date", Field: effectiveDate, Kind: lookup.ValueKeyword},
			{Column: "end_effective_date", Field: endEffectiveDate, Kind: lookup.ValueKeyword},
			{Column: "work_item", Field: workItem, Kind: lookup.ValueKeyword},
			{Column: "patient_status", Field: patientStatus, Kind: lookup.ValueKeyword},
			{Column: "charge_type", Field: chargeType, Kind: lookup.ValueKeyword},
		},
	}

	start := time.Now()
	stats, err := ix.IndexFrom(context.Background(), src, lookup.BulkOptions{
		Name:      "tbl_charge_master",
		BatchSize: 16_384,
	})
	if err != nil {
		log.Fatalf("ingest failed: %v", err)
	}
	fmt.Printf("indexed=%d skipped=%d in %s stats=%+v\n", stats.Indexed, stats.Skipped, time.Since(start), ix.Stats())

	_, hits := ix.SearchInto(lookup.SearchRequest{Query: lookup.Term{Field: "cpt_code", Value: "99213"}, Limit: 20}, nil)
	fmt.Println("cpt_code=99213 hits:", len(hits))

	_, hits = ix.SearchInto(lookup.SearchRequest{Query: lookup.Bool{Must: []lookup.Query{lookup.Simple("ld", "office visit")}, Filter: []lookup.Query{lookup.Term{Field: "charge_type", Value: "professional"}}}, Limit: 20}, nil)
	fmt.Println("ld~'office visit' AND charge_type=professional hits:", len(hits))

	_, hits = ix.SearchInto(lookup.SearchRequest{Query: lookup.Prefix{Field: "cpt_code", Value: "992"}, Limit: 50}, nil)
	fmt.Println("cpt_code prefix '992' hits:", len(hits))
}
