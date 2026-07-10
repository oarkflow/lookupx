package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"log"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	lookup "github.com/oarkflow/lookupx/pkg"
)

const (
	vectorField = "embedding"
	vectorDim   = 16
)

type product struct {
	ID          string    `json:"id"`
	Tenant      string    `json:"tenant"`
	Status      string    `json:"status"`
	SKU         string    `json:"sku"`
	Brand       string    `json:"brand"`
	Category    string    `json:"category"`
	Title       string    `json:"title"`
	Description string    `json:"description"`
	Price       float64   `json:"price"`
	Embedding   []float64 `json:"embedding"`
}

type scenarioResult struct {
	Name  string
	Took  time.Duration
	Total int
	Hits  []lookup.Hit
}

func main() {
	var (
		n        = flag.Int("n", 50000, "number of generated products to index")
		serve    = flag.Bool("serve", false, "start the example HTTP API after indexing")
		addr     = flag.String("addr", ":8090", "HTTP listen address used with -serve")
		dataDir  = flag.String("data", filepath.Join(os.TempDir(), "lookupx-vector-example"), "directory for snapshots and generated query files")
		snapshot = flag.Bool("snapshot", true, "save and reload a snapshot to demonstrate persistence")
	)
	flag.Parse()

	if *n < 1000 {
		*n = 1000
	}
	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		log.Fatal(err)
	}

	cfg := indexConfig(*n)
	fmt.Printf("lookupx vector search example\n")
	fmt.Printf("docs=%d dim=%d goroutines=%d data=%s\n", *n, vectorDim, runtime.GOMAXPROCS(0), *dataDir)
	fmt.Printf("vector options: metric=cosine m=%d ef_construction=%d ef_search=%d\n",
		cfg.Schema.Fields[vectorField].VectorM,
		cfg.Schema.Fields[vectorField].VectorEFConstruction,
		cfg.Schema.Fields[vectorField].VectorEFSearch,
	)

	started := time.Now()
	ix, err := buildIndex(cfg, *n)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("indexed in %s stats=%+v\n", time.Since(started).Round(time.Millisecond), ix.Stats())

	writeHTTPQueryFiles(*dataDir)
	runScenarios(ix)

	if *snapshot {
		path := filepath.Join(*dataDir, "products.snapshot.json")
		if err := ix.SaveSnapshot(path); err != nil {
			log.Fatal(err)
		}
		reloaded, err := lookup.New(cfg)
		if err != nil {
			log.Fatal(err)
		}
		if err := reloaded.LoadSnapshot(path); err != nil {
			log.Fatal(err)
		}
		res := search(reloaded, "snapshot reload exact vector", lookup.VectorQuery{
			Field: vectorField, Vector: embedText("fast portable developer laptop", vectorDim), K: 10, Metric: "cosine", Exact: true,
			Filter: lookup.Term{Field: "tenant", Value: "orgware"},
		}, 5)
		fmt.Printf("snapshot=%s reload_hits=%d first=%s\n", path, len(res.Hits), firstID(res.Hits))
	}

	if *serve {
		fmt.Println("\nHTTP API ready")
		fmt.Printf("  health: curl -s http://127.0.0.1%s/health\n", *addr)
		fmt.Printf("  stats:  curl -s http://127.0.0.1%s/stats\n", *addr)
		fmt.Printf("  ANN:    curl -s -X POST http://127.0.0.1%s/search -H 'Content-Type: application/json' --data @%s\n", *addr, filepath.Join(*dataDir, "query_ann.json"))
		fmt.Printf("  exact:  curl -s -X POST http://127.0.0.1%s/search -H 'Content-Type: application/json' --data @%s\n", *addr, filepath.Join(*dataDir, "query_exact.json"))
		log.Fatal(http.ListenAndServe(*addr, &lookup.Server{Index: ix}))
	}
}

func indexConfig(capacity int) lookup.Config {
	return lookup.Config{
		InitialCapacity: capacity,
		CollectTook:     true,
		StrictRecovery:  true,
		MaxRequestBytes: 8 << 20,
		MaxSearchLimit:  500,
		Schema: lookup.Schema{Fields: map[string]lookup.FieldOptions{
			"tenant": {Kind: lookup.FieldKeyword, Lookup: true, Lowercase: true, Facetable: true},
			"status": {Kind: lookup.FieldKeyword, Lookup: true, Lowercase: true, Facetable: true},
			"sku":    {Kind: lookup.FieldKeyword, Lookup: true, Lowercase: true, Prefix: true, Unique: true},
			"brand":  {Kind: lookup.FieldKeyword, Lookup: true, Lowercase: true, Facetable: true},
			"category": {
				Kind: lookup.FieldKeyword, Lookup: true, Lowercase: true, Prefix: true, Facetable: true,
			},
			"title": {
				Kind: lookup.FieldText, Indexed: true, Lookup: true, Lowercase: true,
				Prefix: true, Ngram: true, Fuzzy: true, Phrase: true, MinGram: 3, MaxGram: 4,
			},
			"description": {Kind: lookup.FieldText, Indexed: true, Lookup: true, Lowercase: true, Phrase: true},
			"price":       {Kind: lookup.FieldFloat, Sortable: true, Facetable: true},
			vectorField: {
				Kind: lookup.FieldVector, Dim: vectorDim,
				VectorMetric: "cosine", VectorM: 32, VectorEFConstruction: 256, VectorEFSearch: 128,
			},
		}},
	}
}

func buildIndex(cfg lookup.Config, n int) (*lookup.Index, error) {
	ix, err := lookup.New(cfg)
	if err != nil {
		return nil, err
	}
	const batchSize = 4096
	ids := make([]string, 0, batchSize)
	docs := make([]lookup.Document, 0, batchSize)
	flush := func() error {
		if len(ids) == 0 {
			return nil
		}
		if err := ix.BatchUpsertSlice(ids, docs); err != nil {
			return err
		}
		ids = ids[:0]
		docs = docs[:0]
		return nil
	}
	for i := 0; i < n; i++ {
		p := generateProduct(i)
		ids = append(ids, p.ID)
		docs = append(docs, lookup.Document{
			"tenant":      p.Tenant,
			"status":      p.Status,
			"sku":         p.SKU,
			"brand":       p.Brand,
			"category":    p.Category,
			"title":       p.Title,
			"description": p.Description,
			"price":       p.Price,
			vectorField:   p.Embedding,
		})
		if len(ids) == cap(ids) {
			if err := flush(); err != nil {
				return nil, err
			}
		}
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return ix, nil
}

func runScenarios(ix *lookup.Index) {
	fmt.Println("\nGo API scenarios")
	qLaptop := embedText("fast portable developer laptop workstation", vectorDim)
	baseFilter := lookup.Bool{Filter: []lookup.Query{
		lookup.Term{Field: "tenant", Value: "orgware"},
		lookup.Term{Field: "status", Value: "active"},
	}}
	ann := lookup.VectorQuery{Field: vectorField, Vector: qLaptop, K: 80, Metric: "cosine", EFSearch: 128, Oversample: 8, Filter: baseFilter}
	exact := ann
	exact.Exact = true

	annRes := search(ix, "filtered ANN cosine", ann, 10)
	exactRes := search(ix, "filtered exact cosine", exact, 10)
	printResult(ix, annRes, 5)
	fmt.Printf("id-overlap@10 vs exact: %.1f%%; exact_first=%s. Generated data has many equal-score duplicate vectors, so this is a conservative tie-sensitive check.\n", recallAt(annRes.Hits, exactRes.Hits, 10)*100, firstID(exactRes.Hits))

	hybridFilter := lookup.Bool{
		Must: []lookup.Query{lookup.Simple("title", "laptop fast")},
		Filter: []lookup.Query{
			lookup.Term{Field: "tenant", Value: "orgware"},
			lookup.Term{Field: "status", Value: "active"},
			lookup.Range{Field: "price", GTE: 600, LTE: 2600},
		},
	}
	hybrid := lookup.VectorQuery{Field: vectorField, Vector: qLaptop, K: 80, Metric: "cosine", EFSearch: 192, Oversample: 10, Filter: hybridFilter}
	printResult(ix, search(ix, "hybrid text+range+vector", hybrid, 10), 5)

	qChair := embedText("ergonomic office chair back support", vectorDim)
	chair := lookup.VectorQuery{Field: vectorField, Vector: qChair, K: 80, Metric: "cosine", EFSearch: 256, Oversample: 12, Filter: lookup.Bool{Filter: []lookup.Query{
		lookup.Term{Field: "category", Value: "chair"},
		lookup.Term{Field: "status", Value: "active"},
	}}}
	printResult(ix, search(ix, "category filtered chair ANN", chair, 5), 5)

	printResult(ix, search(ix, "same query with dot metric", lookup.VectorQuery{Field: vectorField, Vector: qLaptop, K: 40, Metric: "dot", EFSearch: 128, Filter: baseFilter}, 5), 3)
	printResult(ix, search(ix, "same query with L2 metric", lookup.VectorQuery{Field: vectorField, Vector: qLaptop, K: 40, Metric: "l2", EFSearch: 128, Filter: baseFilter}, 5), 3)

	updateAndDelete(ix)
	concurrentReadDemo(ix, qLaptop)
}

func search(ix *lookup.Index, name string, q lookup.VectorQuery, limit int) scenarioResult {
	started := time.Now()
	hits := make([]lookup.Hit, 0, limit)
	res, hits := ix.SearchInto(lookup.SearchRequest{Query: q, Limit: limit}, hits)
	return scenarioResult{Name: name, Took: time.Since(started), Total: res.Total, Hits: hits}
}

func printResult(ix *lookup.Index, res scenarioResult, limit int) {
	fmt.Printf("\n%-28s hits=%d total=%d took=%s\n", res.Name, len(res.Hits), res.Total, res.Took.Round(time.Microsecond))
	if limit > len(res.Hits) {
		limit = len(res.Hits)
	}
	for i := 0; i < limit; i++ {
		h := res.Hits[i]
		doc, _ := ix.Get(h.ID)
		fmt.Printf("  #%02d id=%s score=%.5f tenant=%v status=%v category=%v price=%v title=%v\n",
			i+1, h.ID, h.Score, doc["tenant"], doc["status"], doc["category"], doc["price"], doc["title"])
	}
}

func updateAndDelete(ix *lookup.Index) {
	id := "product-special-vector"
	doc := lookup.Document{
		"tenant":      "orgware",
		"status":      "active",
		"sku":         "sku-special-vector",
		"brand":       "Orgware",
		"category":    "laptop",
		"title":       "Orgware ultra fast developer laptop with private AI acceleration",
		"description": "updated live product used to verify vector replacement and delete handling",
		"price":       1899.0,
		vectorField:   embedText("ultra fast developer laptop private ai acceleration", vectorDim),
	}
	if err := ix.Upsert(id, doc); err != nil {
		log.Fatal(err)
	}
	printResult(ix, search(ix, "after live insert", lookup.VectorQuery{Field: vectorField, Vector: embedText("private AI developer laptop", vectorDim), K: 20, Metric: "cosine", Exact: true, Filter: lookup.Term{Field: "tenant", Value: "orgware"}}, 3), 3)

	doc["title"] = "Orgware ergonomic executive office chair with lumbar support"
	doc["category"] = "chair"
	doc["price"] = 499.0
	doc[vectorField] = embedText("ergonomic executive office chair lumbar support", vectorDim)
	if err := ix.Upsert(id, doc); err != nil {
		log.Fatal(err)
	}
	printResult(ix, search(ix, "after vector update", lookup.VectorQuery{Field: vectorField, Vector: embedText("ergonomic office chair", vectorDim), K: 20, Metric: "cosine", Exact: true, Filter: lookup.Term{Field: "tenant", Value: "orgware"}}, 3), 3)

	if err := ix.Delete(id); err != nil {
		log.Fatal(err)
	}
	res := search(ix, "after delete", lookup.VectorQuery{Field: vectorField, Vector: embedText("ergonomic office chair", vectorDim), K: 20, Metric: "cosine", Exact: true, Filter: lookup.Term{Field: "sku", Value: "sku-special-vector"}}, 3)
	fmt.Printf("\nafter delete hits=%d expected=0 stats_before_compact=%+v\n", len(res.Hits), ix.Stats())
	if err := ix.Compact(); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("after vector compact stats=%+v\n", ix.Stats())
}

func concurrentReadDemo(ix *lookup.Index, vector []float64) {
	workers := runtime.GOMAXPROCS(0)
	if workers < 2 {
		workers = 2
	}
	queriesPerWorker := 200
	latencies := make([]time.Duration, 0, workers*queriesPerWorker)
	ch := make(chan time.Duration, workers*queriesPerWorker)
	for w := 0; w < workers; w++ {
		go func(worker int) {
			filter := lookup.Bool{Filter: []lookup.Query{
				lookup.Term{Field: "tenant", Value: []string{"orgware", "acme", "nepware"}[worker%3]},
				lookup.Term{Field: "status", Value: "active"},
			}}
			q := lookup.VectorQuery{Field: vectorField, Vector: vector, K: 30, Metric: "cosine", EFSearch: 96, Oversample: 6, Filter: filter}
			buf := make([]lookup.Hit, 0, 10)
			for i := 0; i < queriesPerWorker; i++ {
				start := time.Now()
				_, buf = ix.SearchInto(lookup.SearchRequest{Query: q, Limit: 10}, buf)
				ch <- time.Since(start)
			}
		}(w)
	}
	for i := 0; i < workers*queriesPerWorker; i++ {
		latencies = append(latencies, <-ch)
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	fmt.Printf("\nconcurrent ANN reads workers=%d queries=%d p50=%s p95=%s p99=%s\n",
		workers, len(latencies), percentile(latencies, 50), percentile(latencies, 95), percentile(latencies, 99))
}

func generateProduct(i int) product {
	categories := []string{"laptop", "phone", "chair", "monitor", "keyboard", "database", "security", "router", "camera", "server"}
	brands := []string{"Apple", "Dell", "Lenovo", "HP", "Samsung", "Logitech", "Cisco", "Orgware", "Acme", "Nepware"}
	tenants := []string{"orgware", "acme", "nepware"}
	category := categories[i%len(categories)]
	brand := brands[(i*7+i/3)%len(brands)]
	tenant := tenants[i%len(tenants)]
	status := "active"
	if i%17 == 0 {
		status = "archived"
	} else if i%13 == 0 {
		status = "pending"
	}
	feature := featureFor(category, i)
	title := fmt.Sprintf("%s %s %s", brand, feature, category)
	desc := fmt.Sprintf("%s for %s workloads with reliable low latency performance", title, useCaseFor(category))
	price := 50 + float64((i*37)%3000) + float64(i%100)/100
	text := strings.Join([]string{brand, category, feature, desc, useCaseFor(category)}, " ")
	return product{
		ID:          "product-" + strconv.Itoa(i),
		Tenant:      tenant,
		Status:      status,
		SKU:         fmt.Sprintf("sku-%06d", i),
		Brand:       brand,
		Category:    category,
		Title:       title,
		Description: desc,
		Price:       price,
		Embedding:   embedText(text, vectorDim),
	}
}

func featureFor(category string, i int) string {
	features := map[string][]string{
		"laptop":   {"fast portable developer", "lightweight workstation", "AI ready business", "high memory coding"},
		"phone":    {"camera focused", "battery efficient", "secure business", "fast mobile"},
		"chair":    {"ergonomic office", "executive lumbar", "mesh back support", "adjustable comfort"},
		"monitor":  {"4k color accurate", "ultrawide productivity", "gaming high refresh", "office display"},
		"keyboard": {"mechanical developer", "silent office", "wireless compact", "ergonomic split"},
		"database": {"encrypted analytics", "high performance sql", "managed backup", "low latency storage"},
		"security": {"zero trust", "encrypted vault", "compliance audit", "identity access"},
		"router":   {"wifi mesh", "enterprise edge", "secure gateway", "high throughput"},
		"camera":   {"night vision", "office security", "4k streaming", "smart detection"},
		"server":   {"rack compute", "gpu inference", "storage dense", "virtualization host"},
	}
	xs := features[category]
	return xs[i%len(xs)]
}

func useCaseFor(category string) string {
	switch category {
	case "laptop":
		return "software engineering development portability"
	case "chair":
		return "office ergonomics comfort lumbar posture"
	case "database":
		return "sql analytics reporting transaction storage"
	case "security":
		return "audit encryption compliance access control"
	case "server":
		return "compute virtualization inference backend"
	default:
		return "business productivity professional enterprise"
	}
}

func embedText(text string, dim int) []float64 {
	v := make([]float64, dim)
	terms := strings.Fields(strings.ToLower(text))
	for _, term := range terms {
		term = strings.Trim(term, " ,.;:!?()[]{}\"'")
		if term == "" {
			continue
		}
		h := fnv.New64a()
		_, _ = h.Write([]byte(term))
		sum := h.Sum64()
		idx := int(sum % uint64(dim))
		weight := 1.0 + float64((sum>>16)&7)/10
		v[idx] += weight
		v[(idx+int((sum>>24)%uint64(dim)))%dim] += weight * 0.35
	}
	normalize(v)
	return v
}

func normalize(v []float64) {
	var sum float64
	for _, x := range v {
		sum += x * x
	}
	if sum == 0 {
		return
	}
	n := math.Sqrt(sum)
	for i := range v {
		v[i] /= n
	}
}

func recallAt(ann, exact []lookup.Hit, k int) float64 {
	if k > len(exact) {
		k = len(exact)
	}
	if k == 0 {
		return 0
	}
	seen := map[string]struct{}{}
	limit := k
	if limit > len(ann) {
		limit = len(ann)
	}
	for _, h := range ann[:limit] {
		seen[h.ID] = struct{}{}
	}
	match := 0
	for _, h := range exact[:k] {
		if _, ok := seen[h.ID]; ok {
			match++
		}
	}
	return float64(match) / float64(k)
}

func percentile(xs []time.Duration, p int) time.Duration {
	if len(xs) == 0 {
		return 0
	}
	idx := (len(xs)*p + 99) / 100
	if idx <= 0 {
		idx = 1
	}
	if idx > len(xs) {
		idx = len(xs)
	}
	return xs[idx-1].Round(time.Microsecond)
}

func firstID(hits []lookup.Hit) string {
	if len(hits) == 0 {
		return ""
	}
	return hits[0].ID
}

func writeHTTPQueryFiles(dir string) {
	queries := map[string]any{
		"query_ann.json": map[string]any{
			"type": "vector", "field": vectorField, "vector": embedText("fast portable developer laptop workstation", vectorDim),
			"k": 80, "limit": 10, "metric": "cosine", "ef_search": 128, "oversample": 8,
			"filter": []map[string]any{{"type": "term", "field": "tenant", "value": "orgware"}, {"type": "term", "field": "status", "value": "active"}},
		},
		"query_exact.json": map[string]any{
			"type": "vector", "field": vectorField, "vector": embedText("fast portable developer laptop workstation", vectorDim),
			"k": 80, "limit": 10, "metric": "cosine", "exact": true,
			"filter": []map[string]any{{"type": "term", "field": "tenant", "value": "orgware"}, {"type": "term", "field": "status", "value": "active"}},
		},
		"query_hybrid.json": map[string]any{
			"type": "vector", "field": vectorField, "vector": embedText("ergonomic office chair lumbar support", vectorDim),
			"k": 80, "limit": 10, "metric": "cosine", "ef_search": 256, "oversample": 12,
			"filter": []map[string]any{{"type": "term", "field": "status", "value": "active"}, {"type": "term", "field": "category", "value": "chair"}, {"type": "simple", "field": "title", "value": "office chair"}},
		},
	}
	for name, q := range queries {
		b, _ := json.MarshalIndent(q, "", "  ")
		_ = os.WriteFile(filepath.Join(dir, name), append(b, '\n'), 0o644)
	}
}
