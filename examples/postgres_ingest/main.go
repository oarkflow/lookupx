// Command postgres_ingest indexes charge_master from PostgreSQL (macOS) into a
// lookupx index and runs example searches.
//
// Build for macOS:
//
//	GOOS=darwin GOARCH=arm64 go build -o postgres_ingest .
//
// PostgreSQL connection (defaults):
//
//	user=postgres password=postgres dbname=clear_dev host=localhost port=5432 sslmode=disable
//
// Environment variables:
//
//	PG_HOST     — default localhost
//	PG_PORT     — default 5432
//	PG_USER     — default postgres
//	PG_PASSWORD — default postgres
//	PG_DB       — default clear_dev
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	lookup "github.com/oarkflow/lookupx/pkg"
	"github.com/oarkflow/squealx/drivers/postgres"
)

// chargeMasterPageQuery reads from the public.charge_master table.
// Uses $1/$2 placeholders (pgx wire protocol).
const chargeMasterPageQuery = `
SELECT charge_master_id    AS id,
       client_proc_desc    AS ld,
       cpt_hcpcs_code      AS cpt_code,
       work_item_id        AS work_item,
       effective_date      AS effective_date,
       end_effective_date  AS end_effective_date,
       charge_type         AS charge_type,
       charge_amt          AS charge_amt,
       provider_category   AS provider_category,
       patient_status_id   AS patient_status
FROM   public.charge_master
WHERE  charge_master_id > $1
ORDER  BY charge_master_id ASC
LIMIT  $2`

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func buildDSN() string {
	if dsn := os.Getenv("PG_DSN"); dsn != "" {
		return dsn
	}
	host := env("PG_HOST", "localhost")
	port := env("PG_PORT", "5432")
	user := env("PG_USER", "postgres")
	pass := env("PG_PASSWORD", "postgres")
	dbName := env("PG_DB", "clear_dev")
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, pass, dbName)
}

func main() {
	db, err := postgres.Open(buildDSN(), "")
	if err != nil {
		log.Fatalf("open postgres: %v", err)
	}
	defer db.Close()

	rawDB := db.DB()
	rawDB.SetMaxOpenConns(4)
	rawDB.SetMaxIdleConns(2)
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
		Name:      "charge_master",
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
