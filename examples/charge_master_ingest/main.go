// Command charge_master_ingest indexes tbl_charge_master from MySQL (Linux) into a
// lookupx index and runs example searches.
//
// Build for Linux:
//
//	GOOS=linux GOARCH=amd64 go build -o charge_master_ingest .
//
// Environment variables:
//
//	CM_MYSQL_DSN   — full DSN (overrides host/port/user/pass/db)
//	CM_MYSQL_HOST  — default localhost
//	CM_MYSQL_PORT  — default 3306
//	CM_MYSQL_USER  — default service
//	CM_MYSQL_PASS  — default ""
//	CM_MYSQL_DB    — default cleardb
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	lookup "github.com/oarkflow/lookupx/pkg"
	"github.com/oarkflow/squealx/drivers/mysql"
)

const chargeMasterPageQuery1 = `
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
order by tbl_charge_master.charge_master_uid asc
`

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
	host := env("CM_MYSQL_HOST", "localhost")
	port := env("CM_MYSQL_PORT", "3306")
	user := env("CM_MYSQL_USER", "service")
	pass := env("CM_MYSQL_PASS", "")
	dbName := env("CM_MYSQL_DB", "cleardb")

	// service:@tcp(localhost:3306)/cleardb?charset=utf8mb4&timeout=10s&readTimeout=5m&writeTimeout=60s
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&timeout=10s&readTimeout=5m&writeTimeout=60s",
		user, pass, host, port, dbName)
}

func main() {
	db, err := mysql.Open(buildDSN(), "")
	if err != nil {
		log.Fatalf("open mysql: %v", err)
	}
	defer db.Close()

	rawDB := db.DB()
	rawDB.SetMaxOpenConns(2)
	rawDB.SetMaxIdleConns(1)
	rawDB.SetConnMaxLifetime(10 * time.Minute)

	// AutoPagedSQLQuery samples the first page of chargeMasterPageQuery to
	// detect each column's type (text vs keyword vs number vs date) from
	// driver column metadata and sampled values, builds the index schema from
	// that, and resolves the SQLColumn bindings — no manual Schema.Fields map,
	// ix.FieldID() calls, or Columns slice required.
	page := func(lastSeq uint64, limit int) (string, []any) {
		return chargeMasterPageQuery, []any{lastSeq, limit}
	}
	ix, src, err := lookup.AutoPagedSQLQuery(context.Background(), lookup.Config{
		InitialCapacity: 1_600_000,
		DisableSource:   true,
		AppendOnly:      true,
		Clock:           lookup.SystemClock{},
	}, db.DB(), page, "id", "id", 100_000)
	if err != nil {
		log.Fatal(err)
	}
	defer ix.Close()

	start := time.Now()
	stats, err := ix.IndexFrom(context.Background(), src, lookup.BulkOptions{
		Name:      "tbl_charge_master",
		BatchSize: 65_536,
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
