package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/oarkflow/lookupx/lookup"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		return
	}
	switch os.Args[1] {
	case "serve":
		serveCmd(os.Args[2:])
	case "demo":
		demoCmd(os.Args[2:])
	case "search":
		searchCmd(os.Args[2:])
	case "persist-demo":
		persistDemoCmd(os.Args[2:])
	case "restore-search":
		restoreSearchCmd(os.Args[2:])
	case "validate":
		validateCmd(os.Args[2:])
	case "repair":
		repairCmd(os.Args[2:])
	case "compact":
		compactCmd(os.Args[2:])
	case "generations":
		generationsCmd(os.Args[2:])
	case "plan":
		planCmd(os.Args[2:])
	default:
		usage()
	}
}

func usage() {
	fmt.Print(`lookupx CLI

Commands:
  serve   Start HTTP API with multiple index IDs: dataset_a, dataset_b, dataset_c
  demo    Build demo Dataset/DatasetB/DatasetC indexes and print indexing/search latency
  search          Build one demo index and run a lookup query
  persist-demo    Build demo indexes, freeze, and persist them to disk
  restore-search  Load a persisted index from disk and run a lookup query
  validate        Validate a persisted index generation
  repair          Rebuild derived structures and persist a repaired generation
  compact         Remove old persisted generations
  generations     List persisted generations
  plan            Print a 1B-row partition/batch deployment plan

Examples:
  lookupx demo -rows 100000
  lookupx search -index dataset_a -rows 100000 -q 'term=key-0013&group_id=4&date_key=2026-01-01'
  lookupx serve -addr :8089 -demo -rows 100000
  lookupx persist-demo -rows 100000 -data ./data/indexes
  lookupx restore-search -index dataset_a -data ./data/indexes -q 'term=key-0013&group_id=4&date_key=2026-01-01'

HTTP after serve:
  GET  /v1/indexes
  GET  /v1/indexes/dataset_a/stats
  GET  /v1/indexes/dataset_a/lookup?term=key-0013&group_id=4&date_key=2026-01-01
  POST /v1/indexes/dataset_a/search
  POST /v1/indexes/dataset_a/reload
  POST /v1/indexes/dataset_a/reload-sql
  POST /v1/indexes/dataset_a/reload-table
`)
}

func serveCmd(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	addr := fs.String("addr", getenv("ADDR", ":8089"), "listen address")
	demo := fs.Bool("demo", false, "load deterministic demo data into dataset_a/dataset_b/dataset_c")
	rows := fs.Int("rows", 100000, "demo rows per index")
	apiKey := fs.String("api-key", getenv("LOOKUPX_API_KEY", ""), "optional API key")
	_ = fs.Parse(args)

	mgr := lookup.NewMultiIndexManager()
	if err := lookup.RegisterDemoIndexes(mgr, *rows); err != nil {
		log.Fatal(err)
	}
	if *demo {
		for _, id := range []string{"dataset_a", "dataset_b", "dataset_c"} {
			if err := loadDemo(mgr, id, *rows); err != nil {
				log.Fatal(err)
			}
		}
	}
	keys := []string{}
	if *apiKey != "" {
		keys = append(keys, *apiKey)
	}
	log.Printf("lookupx multi-index API listening on %s indexes=%v", *addr, []string{"dataset_a", "dataset_b", "dataset_c"})
	log.Fatal(http.ListenAndServe(*addr, &lookup.MultiServer{Manager: mgr, APIKeys: keys}))
}

func demoCmd(args []string) {
	fs := flag.NewFlagSet("demo", flag.ExitOnError)
	rows := fs.Int("rows", 100000, "rows per index")
	_ = fs.Parse(args)
	mgr := lookup.NewMultiIndexManager()
	if err := lookup.RegisterDemoIndexes(mgr, *rows); err != nil {
		log.Fatal(err)
	}
	for _, id := range []string{"dataset_a", "dataset_b", "dataset_c"} {
		if err := loadDemo(mgr, id, *rows); err != nil {
			log.Fatal(err)
		}
		ix, _ := mgr.Get(id)
		raw := sampleQuery(id)
		q := lookup.ParseLookupQuery(raw)
		hits, totalNS, avgNS := lookup.TimeSearch(ix, q, 5, 1000)
		fmt.Printf("index=%s query=%q hits=%d total_query_ns=%d avg_query_ns=%d loops=1000\n", id, raw, hits, totalNS, avgNS)
	}
}

func persistDemoCmd(args []string) {
	fs := flag.NewFlagSet("persist-demo", flag.ExitOnError)
	rows := fs.Int("rows", 100000, "rows per index")
	data := fs.String("data", "./data/indexes", "persistent index directory")
	_ = fs.Parse(args)
	mgr := lookup.NewMultiIndexManager()
	if err := lookup.RegisterDemoIndexes(mgr, *rows); err != nil {
		log.Fatal(err)
	}
	store := lookup.FileSegmentStore{Root: *data}
	for _, id := range []string{"dataset_a", "dataset_b", "dataset_c"} {
		if err := loadDemo(mgr, id, *rows); err != nil {
			log.Fatal(err)
		}
		ix, _ := mgr.Get(id)
		if err := ix.Freeze(); err != nil {
			log.Fatal(err)
		}
		man, err := ix.SavePersistent(context.Background(), store, id)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("persisted index=%s generation=%d docs=%d path=%s\n", id, man.Generation, man.Docs, man.Path)
	}
}

func restoreSearchCmd(args []string) {
	fs := flag.NewFlagSet("restore-search", flag.ExitOnError)
	id := fs.String("index", "dataset_a", "index id")
	data := fs.String("data", "./data/indexes", "persistent index directory")
	raw := fs.String("q", "term=key-0013&group_id=4&date_key=2026-01-01", "lookup query string")
	limit := fs.Int("limit", 5, "limit")
	_ = fs.Parse(args)
	ix, man, err := lookup.OpenPersistent(context.Background(), lookup.FileSegmentStore{Root: *data}, *id, lookup.Config{})
	if err != nil {
		log.Fatal(err)
	}
	started := time.Now()
	_, hits := ix.SearchInto(lookup.SearchRequest{Query: lookup.ParseLookupQuery(*raw), Limit: *limit}, nil)
	took := time.Since(started)
	fmt.Printf("loaded index=%s generation=%d query=%q hits=%d latency_ns=%d latency=%s\n", *id, man.Generation, *raw, len(hits), took.Nanoseconds(), took)
	for _, h := range hits {
		fmt.Println(h.ID)
	}
}

func searchCmd(args []string) {
	fs := flag.NewFlagSet("search", flag.ExitOnError)
	id := fs.String("index", "dataset_a", "index id")
	rows := fs.Int("rows", 100000, "demo rows")
	raw := fs.String("q", "term=key-0013&group_id=4&date_key=2026-01-01", "lookup query string")
	limit := fs.Int("limit", 5, "limit")
	_ = fs.Parse(args)
	mgr := lookup.NewMultiIndexManager()
	if err := lookup.RegisterDemoIndexes(mgr, *rows); err != nil {
		log.Fatal(err)
	}
	if err := loadDemo(mgr, *id, *rows); err != nil {
		log.Fatal(err)
	}
	ix, _ := mgr.Get(*id)
	started := time.Now()
	_, hits := ix.SearchInto(lookup.SearchRequest{Query: lookup.ParseLookupQuery(*raw), Limit: *limit}, nil)
	took := time.Since(started)
	fmt.Printf("index=%s query=%q hits=%d latency_ns=%d latency=%s\n", *id, *raw, len(hits), took.Nanoseconds(), took)
	for _, h := range hits {
		fmt.Println(h.ID)
	}
}

func loadDemo(mgr *lookup.MultiIndexManager, id string, rows int) error {
	mi, ok := mgr.Managed(id)
	if !ok {
		return fmt.Errorf("index %s not registered", id)
	}
	factory := func(ix *lookup.Index) (lookup.Source, error) { return demoSource(ix, id, rows), nil }
	started := time.Now()
	stats, err := mgr.ReloadFromFactory(context.Background(), id, factory, lookup.BulkOptions{Name: id, BatchSize: 65536})
	if err != nil {
		return err
	}
	_ = mi
	fmt.Printf("index=%s %s\n", id, lookup.LatencySummary(stats.Indexed, time.Since(started)))
	return nil
}

func demoSource(ix *lookup.Index, indexID string, rows int) lookup.Source {
	term := ix.FieldID("term")
	work := ix.FieldID("group_id")
	date_key := ix.FieldID("date_key")
	partition := ix.FieldID("partition_id")
	records := make([]lookup.SourceRecord, rows)
	for i := 1; i <= rows; i++ {
		code := codeFor(indexID, i)
		wi := strconv.Itoa((i % 10) + 1)
		day := fmt.Sprintf("2026-01-%02d", (i%28)+1)
		if i%1000 == 123 {
			code, wi, day = firstTerm(indexID), "4", "2026-01-01"
		}
		records[i-1].ID = fmt.Sprintf("%s-%06d", indexID, i)
		records[i-1].Seq = uint64(i)
		records[i-1].Values = append(records[i-1].Values,
			lookup.SourceValue{Field: term, Kind: lookup.ValueKeyword, String: strings.ToLower(code), Normalized: true},
			lookup.SourceValue{Field: work, Kind: lookup.ValueKeyword, String: wi, Normalized: true},
			lookup.SourceValue{Field: date_key, Kind: lookup.ValueKeyword, String: day, Normalized: true},
			lookup.SourceValue{Field: partition, Kind: lookup.ValueKeyword, String: strconv.Itoa((i % 200) + 1), Normalized: true},
		)
	}
	return lookup.SliceSource{Records: records}
}

func codeFor(indexID string, i int) string {
	switch strings.ToLower(indexID) {
	case "dataset_b":
		codes := [...]string{"A00.0", "E11.9", "I10", "J45.909", "M54.5", "R51", "S72.001A", "N39.0"}
		return codes[i%len(codes)]
	case "dataset_c":
		codes := [...]string{"A0428", "A0429", "E0114", "J1100", "J1885", "G0008", "Q9967", "L1830"}
		return codes[i%len(codes)]
	default:
		codes := [...]string{"key-0011", "key-0012", "key-0013", "key-0014", "key-0015", "key-3000", "key-0053", "key-5025", "key-6415", "key-1046"}
		return codes[i%len(codes)]
	}
}
func firstTerm(indexID string) string {
	if indexID == "dataset_b" {
		return "E11.9"
	}
	if indexID == "dataset_c" {
		return "A0428"
	}
	return "key-0013"
}
func sampleQuery(indexID string) string {
	return "term=" + firstTerm(indexID) + "&group_id=4&date_key=2026-01-01"
}
func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func validateCmd(args []string) {
	fs := flag.NewFlagSet("validate", flag.ExitOnError)
	id := fs.String("index", "dataset_a", "index id")
	data := fs.String("data", "./data/indexes", "persistent index directory")
	_ = fs.Parse(args)
	rep, err := lookup.ValidatePersistentIndex(context.Background(), lookup.FileSegmentStore{Root: *data}, *id, lookup.Config{})
	if err != nil && len(rep.Issues) == 0 {
		log.Fatal(err)
	}
	b, _ := jsonMarshalIndent(rep)
	fmt.Println(string(b))
	if !rep.OK {
		os.Exit(2)
	}
}

func repairCmd(args []string) {
	fs := flag.NewFlagSet("repair", flag.ExitOnError)
	id := fs.String("index", "dataset_a", "index id")
	data := fs.String("data", "./data/indexes", "persistent index directory")
	_ = fs.Parse(args)
	man, rep, err := lookup.RepairPersistentIndex(context.Background(), lookup.FileSegmentStore{Root: *data}, *id, lookup.Config{})
	if err != nil {
		log.Fatal(err)
	}
	b, _ := jsonMarshalIndent(map[string]any{"manifest": man, "validation": rep})
	fmt.Println(string(b))
}

func compactCmd(args []string) {
	fs := flag.NewFlagSet("compact", flag.ExitOnError)
	id := fs.String("index", "dataset_a", "index id")
	data := fs.String("data", "./data/indexes", "persistent index directory")
	keep := fs.Int("keep", 2, "generations to keep")
	_ = fs.Parse(args)
	removed, err := lookup.CompactPersistentGenerations(context.Background(), lookup.FileSegmentStore{Root: *data}, *id, lookup.GenerationPolicy{KeepLast: *keep})
	if err != nil {
		log.Fatal(err)
	}
	b, _ := jsonMarshalIndent(map[string]any{"removed": removed})
	fmt.Println(string(b))
}

func generationsCmd(args []string) {
	fs := flag.NewFlagSet("generations", flag.ExitOnError)
	id := fs.String("index", "dataset_a", "index id")
	data := fs.String("data", "./data/indexes", "persistent index directory")
	_ = fs.Parse(args)
	gens, err := lookup.ListIndexGenerations(context.Background(), lookup.FileSegmentStore{Root: *data}, *id)
	if err != nil {
		log.Fatal(err)
	}
	b, _ := jsonMarshalIndent(gens)
	fmt.Println(string(b))
}

func planCmd(args []string) {
	fs := flag.NewFlagSet("plan", flag.ExitOnError)
	id := fs.String("index", "dataset_a", "index id")
	rows := fs.Int64("rows", 1_000_000_000, "estimated rows")
	_ = fs.Parse(args)
	b, _ := jsonMarshalIndent(lookup.PlanBillionRowDeployment(*id, *rows, lookup.DefaultBillionRowBudget()))
	fmt.Println(string(b))
}

func jsonMarshalIndent(v any) ([]byte, error) {
	return lookup.JSONMarshalIndent(v)
}
