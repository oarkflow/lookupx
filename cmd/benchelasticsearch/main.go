// benchelasticsearch runs the shared JSONL search workload against an
// Elasticsearch endpoint. It uses only the standard library.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

type row struct {
	ID     string            `json:"id"`
	Fields map[string]string `json:"fields"`
}
type query struct {
	Type   string            `json:"type"`
	Name   string            `json:"name"`
	Fields map[string]string `json:"fields"`
	Field  string            `json:"field,omitempty"`
	Value  string            `json:"value,omitempty"`
	GTE    string            `json:"gte,omitempty"`
	LTE    string            `json:"lte,omitempty"`
	Offset int               `json:"offset,omitempty"`
	Limit  int               `json:"limit"`
}
type qresult struct {
	Name            string  `json:"name"`
	AvgClientNanos  int64   `json:"avg_client_nanos"`
	AvgServerTookMS float64 `json:"avg_server_took_ms"`
	Hits            int     `json:"hits"`
	Loops           int     `json:"loops"`
}
type result struct {
	Engine          string    `json:"engine"`
	Rows            int       `json:"rows"`
	IndexNanos      int64     `json:"index_nanos"`
	IndexRowsPerSec float64   `json:"index_rows_per_sec"`
	StoreBytes      int64     `json:"store_bytes"`
	HeapUsedBytes   int64     `json:"heap_used_bytes"`
	WarmupLoops     int       `json:"warmup_loops"`
	Queries         []qresult `json:"queries"`
}

func main() {
	endpoint := flag.String("url", "http://localhost:9200", "Elasticsearch base URL")
	index := flag.String("index", "lookupx-bench", "index name; replaced before each run")
	dataPath := flag.String("data", "benchdata/dataset.jsonl", "shared JSONL dataset")
	queryPath := flag.String("queries", "benchdata/queries.jsonl", "shared query manifest")
	fieldsCSV := flag.String("fields", "field_a,field_b,field_c", "indexed fields")
	loops := flag.Int("loops", 1000, "search repetitions per query")
	apiKey := flag.String("api-key", os.Getenv("ELASTIC_API_KEY"), "optional Elasticsearch API key")
	flag.Parse()
	fields := split(*fieldsCSV)
	client := &http.Client{Timeout: 2 * time.Minute}
	do := func(method, path string, body []byte, contentType string) []byte {
		req, err := http.NewRequest(method, strings.TrimRight(*endpoint, "/")+path, bytes.NewReader(body))
		check(err)
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}
		if *apiKey != "" {
			req.Header.Set("Authorization", "ApiKey "+*apiKey)
		}
		resp, err := client.Do(req)
		check(err)
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		check(err)
		if method == "DELETE" && resp.StatusCode == http.StatusNotFound {
			return b
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Errorf("Elasticsearch %s %s: %s: %s", method, path, resp.Status, b))
		}
		return b
	}
	do("DELETE", "/"+*index, nil, "")
	readHeapUsed := func() int64 {
		var nodeStats struct {
			Nodes map[string]struct {
				JVM struct {
					Mem struct {
						HeapUsed int64 `json:"heap_used_in_bytes"`
					} `json:"mem"`
				} `json:"jvm"`
			} `json:"nodes"`
		}
		check(json.Unmarshal(do("GET", "/_nodes/stats/jvm", nil, ""), &nodeStats))
		var used int64
		for _, node := range nodeStats.Nodes {
			used += node.JVM.Mem.HeapUsed
		}
		return used
	}
	props := map[string]any{"id": map[string]any{"type": "keyword"}, "__composite": map[string]any{"type": "keyword"}}
	for _, f := range fields {
		typ := "keyword"
		switch f {
		case "event_dos":
			typ = "date"
		case "work_item_id", "charge_amount":
			typ = "long"
		case "client_proc_desc":
			typ = "text"
		}
		props[f] = map[string]any{"type": typ}
	}
	settings, _ := json.Marshal(map[string]any{"settings": map[string]any{"number_of_replicas": 0, "refresh_interval": "-1"}, "mappings": map[string]any{"_source": map[string]any{"enabled": false}, "properties": props}})
	do("PUT", "/"+*index, settings, "application/json")
	f, err := os.Open(*dataPath)
	check(err)
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 8*1024*1024)
	start := time.Now()
	rows := 0
	bulk := make([]byte, 0, 4*1024*1024)
	flush := func() {
		if len(bulk) > 0 {
			response := do("POST", "/_bulk", bulk, "application/x-ndjson")
			var status struct {
				Errors bool `json:"errors"`
			}
			check(json.Unmarshal(response, &status))
			if status.Errors {
				panic("Elasticsearch bulk indexing reported item errors: " + string(response))
			}
			bulk = bulk[:0]
		}
	}
	for scanner.Scan() {
		var r row
		check(json.Unmarshal(scanner.Bytes(), &r))
		comp := make([]string, len(fields))
		for i, field := range fields {
			v := strings.ToLower(r.Fields[field])
			r.Fields[field] = v
			comp[i] = v
		}
		action, _ := json.Marshal(map[string]any{"index": map[string]string{"_index": *index, "_id": r.ID}})
		docFields := map[string]any{"id": r.ID, "__composite": strings.Join(comp, "\x1f")}
		for k, v := range r.Fields {
			docFields[k] = v
		}
		doc, _ := json.Marshal(docFields)
		bulk = append(bulk, action...)
		bulk = append(bulk, '\n')
		bulk = append(bulk, doc...)
		bulk = append(bulk, '\n')
		rows++
		if len(bulk) >= 4*1024*1024 {
			flush()
		}
	}
	check(scanner.Err())
	flush()
	do("POST", "/"+*index+"/_refresh", nil, "")
	indexNanos := time.Since(start).Nanoseconds()
	var indexStats struct {
		Indices map[string]struct {
			Primaries struct {
				Store struct {
					Size int64 `json:"size_in_bytes"`
				} `json:"store"`
			} `json:"primaries"`
		} `json:"indices"`
	}
	check(json.Unmarshal(do("GET", "/"+*index+"/_stats/store", nil, ""), &indexStats))
	var storeBytes int64
	for _, idx := range indexStats.Indices {
		storeBytes += idx.Primaries.Store.Size
	}
	heapUsed := readHeapUsed()
	qs := readQueries(*queryPath)
	qr := make([]qresult, 0, len(qs))
	const warmupLoops = 100
	for _, q := range qs {
		field, value := q.Field, strings.ToLower(q.Value)
		if field == "" && q.Fields != nil {
			for _, f := range fields {
				if v, ok := q.Fields[f]; ok {
					field, value = f, strings.ToLower(v)
					break
				}
			}
		}
		var queryDSL any
		switch q.Type {
		case "term", "page":
			queryDSL = map[string]any{"term": map[string]any{field: value}}
		case "prefix":
			queryDSL = map[string]any{"prefix": map[string]any{field: value}}
		case "contains":
			queryDSL = map[string]any{"wildcard": map[string]any{field: map[string]any{"value": "*" + value + "*"}}}
		case "phrase":
			queryDSL = map[string]any{"match_phrase": map[string]any{field: q.Value}}
		case "text":
			queryDSL = map[string]any{"match": map[string]any{field: map[string]any{"query": q.Value, "operator": "or"}}}
		case "range":
			queryDSL = map[string]any{"range": map[string]any{field: map[string]any{"gte": q.GTE, "lte": q.LTE}}}
		default:
			vals := make([]string, len(fields))
			for i, f := range fields {
				vals[i] = strings.ToLower(q.Fields[f])
			}
			queryDSL = map[string]any{"term": map[string]any{"__composite": strings.Join(vals, "\x1f")}}
		}
		limit := q.Limit
		if limit <= 0 {
			limit = 10
		}
		offset := 0
		if q.Type == "page" {
			offset = q.Offset
		}
		body, _ := json.Marshal(map[string]any{"size": limit, "from": offset, "track_total_hits": false, "query": queryDSL})
		for i := 0; i < warmupLoops; i++ {
			do("POST", "/"+*index+"/_search", body, "application/json")
		}
		var hits int
		var serverTookMS int64
		started := time.Now()
		for i := 0; i < *loops; i++ {
			var response struct {
				Took int64 `json:"took"`
				Hits struct {
					Hits []json.RawMessage `json:"hits"`
				} `json:"hits"`
			}
			check(json.Unmarshal(do("POST", "/"+*index+"/_search", body, "application/json"), &response))
			hits = len(response.Hits.Hits)
			serverTookMS += response.Took
		}
		qr = append(qr, qresult{Name: q.Name, AvgClientNanos: time.Since(started).Nanoseconds() / int64(*loops), AvgServerTookMS: float64(serverTookMS) / float64(*loops), Hits: hits, Loops: *loops})
	}
	out := result{Engine: "elasticsearch", Rows: rows, IndexNanos: indexNanos, IndexRowsPerSec: float64(rows) / (float64(indexNanos) / 1e9), StoreBytes: storeBytes, HeapUsedBytes: heapUsed, WarmupLoops: warmupLoops, Queries: qr}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	check(enc.Encode(out))
}
func check(err error) {
	if err != nil {
		panic(err)
	}
}
func split(s string) []string {
	parts := strings.Split(s, ",")
	out := parts[:0]
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
func readQueries(path string) []query {
	f, err := os.Open(path)
	check(err)
	defer f.Close()
	sc := bufio.NewScanner(f)
	var out []query
	for sc.Scan() {
		var q query
		check(json.Unmarshal(sc.Bytes(), &q))
		out = append(out, q)
	}
	check(sc.Err())
	return out
}
