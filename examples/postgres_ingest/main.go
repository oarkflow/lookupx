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

	"github.com/oarkflow/squealx/drivers/postgres"
	lookup "github.com/oarkflow/lookupx/pkg"
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

	ix, err := lookup.New(lookup.Config{
		InitialCapacity: 1_600_000,
		DisableSource:   true,
		AppendOnly:      true,
		Clock:           lookup.SystemClock{},
		Schema: lookup.Schema{Fields: map[string]lookup.FieldOptions{
			"ld":                {Kind: lookup.FieldText, Indexed: true, Lowercase: true},
			"cpt_code":          {Kind: lookup.FieldKeyword, Lookup: true, Prefix: true, MinPrefix: 3, MaxPrefix: 5, Lowercase: true},
			"effective_date":    {Kind: lookup.FieldKeyword, Lookup: true},
			"end_effective_date": {Kind: lookup.FieldKeyword, Lookup: true},
			"work_item":         {Kind: lookup.FieldKeyword, Lookup: true},
			"charge_type":       {Kind: lookup.FieldKeyword, Lookup: true, Lowercase: true},
			"charge_amt":        {Kind: lookup.FieldFloat},
			"provider_category": {Kind: lookup.FieldKeyword, Lookup: true, Lowercase: true},
			"patient_status":    {Kind: lookup.FieldKeyword, Lookup: true},
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
	chargeType := ix.FieldID("charge_type")
	chargeAmt := ix.FieldID("charge_amt")
	providerCategory := ix.FieldID("provider_category")
	patientStatus := ix.FieldID("patient_status")

	src := lookup.PagedSQLQuerySource{
		DB:       db.DB(),
		PageSize: 100_000,
		Page: func(lastSeq uint64, limit int) (string, []any) {
			return chargeMasterPageQuery, []any{lastSeq, limit}
		},
		IDColumn:  "id",
		SeqColumn: "id",
		Columns: []lookup.SQLColumn{
			{Column: "ld", Field: ld, Kind: lookup.ValueText},
			{Column: "cpt_code", Field: cptCode, Kind: lookup.ValueKeyword},
			{Column: "effective_date", Field: effectiveDate, Kind: lookup.ValueKeyword, Layout: "2006-01-02"},
			{Column: "end_effective_date", Field: endEffectiveDate, Kind: lookup.ValueKeyword, Layout: "2006-01-02"},
			{Column: "work_item", Field: workItem, Kind: lookup.ValueKeyword},
			{Column: "charge_type", Field: chargeType, Kind: lookup.ValueKeyword},
			{Column: "charge_amt", Field: chargeAmt, Kind: lookup.ValueNumber},
			{Column: "provider_category", Field: providerCategory, Kind: lookup.ValueKeyword},
			{Column: "patient_status", Field: patientStatus, Kind: lookup.ValueKeyword},
		},
	}

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
