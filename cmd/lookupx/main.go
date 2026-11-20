package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	lookup "github.com/oarkflow/lookupx/pkg"

	// Blank-imported so their database/sql drivers ("pgx", "mysql", "sqlite")
	// are registered for the reload-sql/reload-table HTTP endpoints, which
	// connect by driver name at request time (see pkg/datasource_sql.go).
	_ "github.com/oarkflow/squealx/drivers/mysql"
	_ "github.com/oarkflow/squealx/drivers/postgres"
	_ "github.com/oarkflow/squealx/drivers/sqlite"
)

func main() {
	loadDotEnvFile(".env")
	if len(os.Args) < 2 {
		serveCmd(nil)
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
  serve           Start HTTP API server (create indexes via API or web UI)
  demo            Build demo indexes and print indexing/search latency
  search          Build one demo index and run a lookup query
  persist-demo    Build demo indexes, freeze, and persist them to disk
  restore-search  Load a persisted index from disk and run a lookup query
  validate        Validate a persisted index generation
  repair          Rebuild derived structures and persist a repaired generation
  compact         Remove old persisted generations
  generations     List persisted generations
  plan            Print a 1B-row partition/batch deployment plan

Examples:
  lookupx serve -addr 0.0.0.0:8089
  lookupx demo -rows 100000
  lookupx search -index my_index -rows 100000 -q 'term=key-0013&group_id=4&date_key=2026-01-01'
  lookupx persist-demo -rows 100000 -data ./data/indexes
  lookupx restore-search -index my_index -data ./data/indexes -q 'term=key-0013&group_id=4&date_key=2026-01-01'

HTTP API:
  GET  /v1/indexes                         List all indexes
  POST /v1/indexes                         Create a new index
  DELETE /v1/indexes                       Delete an index
  GET  /v1/indexes/{id}/stats              Get index stats
  GET  /v1/indexes/{id}/lookup?k=v&...     Lookup query
  POST /v1/indexes/{id}/search             Search with JSON body
  POST /v1/indexes/{id}/reload             Reload from registered source
  POST /v1/indexes/{id}/reload-sql         Reload from SQL connection
  POST /v1/indexes/{id}/reload-table       Reload from SQL table (paged)
  POST /v1/indexes/{id}/reload-file        Reload from CSV, TSV, XLSX, JSON, or JSON Lines upload
  GET  /v1/indexes/{id}/generations        List persisted generations
  POST /v1/indexes/{id}/restore            Restore a generation
  POST /v1/indexes/{id}/freeze             Freeze index for read acceleration
  POST /v1/indexes/{id}/compact            Compact old generations
  GET  /v1/indexes/{id}/validate           Validate persisted index
  POST /v1/indexes/{id}/repair             Repair persisted index
  /                                        Web frontend (when -web flag is set)
`)
}

func serveCmd(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	addr := fs.String("addr", getenv("ADDR", "0.0.0.0:8089"), "listen address")
	configPath := fs.String("config", getenv("LOOKUPX_CONFIG", "config/lookupx.bcl"), "BCL configuration file; set empty to start without configured indexes")
	apiKey := fs.String("api-key", getenv("LOOKUPX_API_KEY", ""), "optional API key")
	webDir := fs.String("web", getenv("LOOKUPX_WEB_DIR", ""), "serve frontend files from this directory (default: ./web)")
	dataDir := fs.String("data", getenv("LOOKUPX_DATA_DIR", "./data/indexes"), "persistent index directory")
	storage := fs.String("storage", getenv("LOOKUPX_STORAGE", "disk"), "index storage: disk (partitioned low-memory) or memory")
	adminUser := getenv("LOOKUPX_ADMIN_USERNAME", "")
	adminPassword := getenv("LOOKUPX_ADMIN_PASSWORD", "")
	adminUser = strings.TrimSpace(adminUser)
	if adminUser == "" || strings.TrimSpace(adminPassword) == "" {
		// An incomplete pair is not a bootstrap credential; let the browser
		// create the first admin account instead.
		adminUser, adminPassword = "", ""
	}
	sessionSecret := getenv("LOOKUPX_SESSION_SECRET", "")
	sessionTrustedProxies := splitNonEmptyCSV(getenv("LOOKUPX_SESSION_TRUSTED_PROXIES", ""))
	sessionSecure, err := strconv.ParseBool(getenv("LOOKUPX_SESSION_SECURE", "true"))
	if err != nil {
		log.Fatalf("LOOKUPX_SESSION_SECURE must be true or false: %v", err)
	}
	_ = fs.Parse(args)
	*storage = strings.ToLower(strings.TrimSpace(*storage))
	if *storage != "memory" && *storage != "disk" {
		log.Fatal("-storage must be memory or disk")
	}
	if len(sessionSecret) < 32 {
		log.Fatal("LOOKUPX_SESSION_SECRET must contain at least 32 characters in .env")
	}

	cacheBytes, err := strconv.ParseInt(getenv("LOOKUPX_PARTITION_CACHE_BYTES", "268435456"), 10, 64)
	if err != nil {
		log.Fatalf("LOOKUPX_PARTITION_CACHE_BYTES must be a nonnegative integer byte count: %v", err)
	}
	if err := lookup.ConfigurePartitionCache(cacheBytes); err != nil {
		log.Fatal(err)
	}

	ctx, cancelStartup := context.WithCancel(context.Background())
	var mgr *lookup.MultiIndexManager
	var startupConfig *lookup.LookupXConfig
	keys := []string{}
	if strings.TrimSpace(*configPath) != "" {
		cfg, err := lookup.LoadBCLConfig(*configPath, nil)
		if err != nil {
			log.Fatalf("load BCL config %s: %v", *configPath, err)
		}
		mgr, err = lookup.BuildFromBCLConfig(ctx, cfg, nil)
		if err != nil {
			log.Fatalf("build indexes from BCL config: %v", err)
		}
		startupConfig = cfg
		if *addr == "0.0.0.0:8089" && cfg.Addr != "" {
			*addr = cfg.Addr
		}
		if *dataDir == "./data/indexes" && cfg.DataDir != "" {
			*dataDir = cfg.DataDir
		}
		for _, key := range cfg.APIKeys {
			if strings.TrimSpace(key) != "" {
				keys = append(keys, key)
			}
		}
	} else {
		mgr = lookup.NewMultiIndexManager()
	}
	if *apiKey != "" {
		keys = append(keys, *apiKey)
	}
	app := lookup.NewFHApp(&lookup.MultiServer{Manager: mgr, APIKeys: keys, AdminUser: adminUser, AdminPassword: adminPassword, SessionSecret: sessionSecret, SessionSecure: sessionSecure, SessionTrustedProxies: sessionTrustedProxies, ConfigPath: *configPath, WebDir: *webDir, DataDir: *dataDir, Storage: *storage})
	listenResult := make(chan error, 1)
	go func() { listenResult <- app.ListenWithGracefulShutdown(*addr) }()
	if err := waitForListener(*addr, listenResult, 15*time.Second); err != nil {
		cancelStartup()
		log.Fatal(err)
	}
	lookup.LogKV("server_listening", "address", *addr, "storage", *storage, "indexes", len(mgr.List()), "web_console", "/")
	if startupConfig != nil {
		go warmConfiguredIndexes(ctx, mgr, startupConfig, *storage, *dataDir)
	} else if *storage == "disk" {
		go func() {
			if err := mgr.RestorePersistent(ctx, lookup.FileSegmentStore{Root: *dataDir}); err != nil && ctx.Err() == nil {
				lookup.LogKV("index_restore_failed", "error", err)
			}
		}()
	}
	listenErr := <-listenResult
	cancelStartup()
	if listenErr != nil {
		log.Fatal(listenErr)
	}
}

func waitForListener(addr string, listenResult <-chan error, timeout time.Duration) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("parse listen address %q: %w", addr, err)
	}
	if host == "" || host == "0.0.0.0" {
		host = "127.0.0.1"
	}
	if host == "::" {
		host = "::1"
	}
	probe := net.JoinHostPort(host, port)
	deadline := time.NewTimer(timeout)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer deadline.Stop()
	defer ticker.Stop()
	for {
		conn, dialErr := net.DialTimeout("tcp", probe, 100*time.Millisecond)
		if dialErr == nil {
			_ = conn.Close()
			return nil
		}
		select {
		case listenErr := <-listenResult:
			if listenErr == nil {
				return fmt.Errorf("server stopped before accepting connections")
			}
			return fmt.Errorf("start HTTP server: %w", listenErr)
		case <-deadline.C:
			return fmt.Errorf("server did not start listening on %s within %s", addr, timeout)
		case <-ticker.C:
		}
	}
}

func warmConfiguredIndexes(ctx context.Context, mgr *lookup.MultiIndexManager, cfg *lookup.LookupXConfig, storage, dataDir string) {
	if storage == "disk" {
		defer func() {
			// Once ingestion is idle, return its temporary heap pages to the OS.
			// This runs once after startup builds, never per partition or request.
			debug.FreeOSMemory()
			var memory runtime.MemStats
			runtime.ReadMemStats(&memory)
			lookup.LogKV("index_startup_complete", "indexes", len(cfg.Indexes), "heap_bytes", memory.HeapAlloc, "heap_released_bytes", memory.HeapReleased)
		}()
	}
	indexes := mgr.List()
	indexConfigs := make(map[string]lookup.Config, len(cfg.Indexes))
	datasources := make(map[string]lookup.DatasourceDef, len(cfg.Datasources))
	for _, def := range cfg.Indexes {
		indexConfigs[def.ID] = def.ToConfig()
	}
	for _, ds := range cfg.Datasources {
		datasources[ds.ID] = ds
	}
	unpagedSQLIndexes := make([]string, 0)
	for _, index := range cfg.Indexes {
		ds, ok := datasources[index.DatasourceID]
		if ok && strings.EqualFold(ds.Kind, "sql_query") && strings.TrimSpace(ds.SeqColumn) == "" {
			unpagedSQLIndexes = append(unpagedSQLIndexes, index.ID)
		}
	}
	if len(unpagedSQLIndexes) > 0 {
		lookup.LogKV("sql_indexing_notice",
			"status", "indexing continues",
			"indexes", strings.Join(unpagedSQLIndexes, ","),
			"reason", "these SQL datasources do not configure seq_column",
			"behavior", "LookupX streams each SELECT as written. If RDS disconnects, it retries from row 1; after four retries, that index build stops.",
			"hint", "Return a unique, increasing numeric key in the SELECT, then set seq_column and page_size in that datasource.",
			"custom_sql_hint", "For expensive SQL, page the SELECT with key > {{lookupx_after}} ORDER BY key ASC LIMIT {{lookupx_limit}}.")
	}

	if storage == "memory" {
		store := lookup.FileSegmentStore{Root: dataDir}
		for _, info := range indexes {
			if ctx.Err() != nil {
				return
			}
			started := time.Now()
			if err := mgr.RestorePersistentIndex(ctx, store, info.ID, indexConfigs[info.ID]); err == nil {
				ready, _ := mgr.Managed(info.ID)
				if ready != nil {
					lookup.LogKV("index_ready", "index", info.ID, "state", "restored", "storage", "memory", "generation", ready.Generation, "docs", ready.Index.Stats().Docs, "elapsed", time.Since(started))
				}
			} else if !os.IsNotExist(err) && ctx.Err() == nil {
				lookup.LogKV("index_restore_failed", "index", info.ID, "error", err)
			}
		}
	}

	// Disk mode uses partition catalogs so a large source is not accumulated in
	// one in-memory Index. Keep the regular persistent restore path for memory
	// mode, where a complete in-memory index is intentional.
	for _, info := range indexes {
		if ctx.Err() != nil {
			return
		}
		started := time.Now()
		lookup.LogKV("index_load_started", "index", info.ID, "storage", storage, "background", true)
		if storage == "disk" {
			managed, ok := mgr.Managed(info.ID)
			if !ok || managed.Source == nil {
				lookup.LogKV("index_load_failed", "index", info.ID, "error", "index has no configured source")
				continue
			}
			schemaConfig := managed.Config
			schemaConfig.InitialCapacity = 0
			ix, err := lookup.New(schemaConfig)
			if err != nil {
				lookup.LogKV("index_load_failed", "index", info.ID, "error", err)
				continue
			}
			src, err := managed.Source(ix)
			if err != nil {
				_ = ix.Close()
				lookup.LogKV("index_load_failed", "index", info.ID, "error", err)
				continue
			}
			// The datasource now has resolved FieldIDs. Release this temporary
			// schema index before the builder allocates its first partition.
			_ = ix.Close()
			bulk := managed.Bulk
			if bulk.BatchSize <= 0 {
				bulk.BatchSize = lookup.LowMemoryBillionBudget().DefaultBatchSize
			}
			mgr.SetBuildProgress(info.ID, lookup.BulkProgress{SourceName: info.ID, StartedAt: started}, true, "")
			stopProgress := startIndexProgress(ctx, mgr, info.ID)
			catalogPath := filepath.Join(dataDir, managed.ID, "partitions.json")
			catalog, catalogErr := lookup.LoadPartitionDataset(catalogPath)
			publishProgress := os.IsNotExist(catalogErr)
			if catalogErr == nil && !catalog.Complete && catalog.Fingerprint != "" {
				progress, progressErr := lookup.LoadPartitionDataset(catalogPath + ".progress.json")
				publishProgress = progressErr == nil && progress.BuildID == catalog.BuildID && progress.Fingerprint == catalog.Fingerprint
			}
			_, hasResumeIdentity := src.(lookup.SourceIdentity)
			_, supportsResume := src.(lookup.ResumableSource)
			if hasResumeIdentity && supportsResume {
				if identity, ok := src.(lookup.SourceIdentity); ok && identity.ResumeIdentity() != "" {
					bulk.Resume = true
				}
			}
			partitionOptions := lookup.PartitionedBuildOptions{
				IndexID: info.ID, Store: lookup.FileMMapSegmentStore{Root: dataDir},
				CatalogPath: catalogPath, DatasetRoot: dataDir, PublishProgress: publishProgress,
				Config: managed.Config, RowsPerPartition: uint64(lookup.LowMemoryBillionBudget().TargetRowsPerPartition),
				Bulk: bulk, Freeze: true,
				Progress: func(p lookup.PartitionedBuildProgress) {
					mgr.SetBuildProgress(info.ID, lookup.BulkProgress{SourceName: info.ID, Seen: p.Seen, Indexed: p.Indexed, Skipped: p.Skipped, LastSeq: p.LastSeq, Partition: p.PartitionNo + 1, Partitions: p.Partitions, CurrentPartitionRows: p.CurrentRows, StartedAt: started, Took: p.Took}, true, "")
				},
			}
			var stats lookup.PartitionedBuildStats
			for attempt := 1; ; attempt++ {
				stats, err = lookup.BuildPartitionedPersistent(ctx, src, partitionOptions)
				if err == nil || !lookup.IsTransientSQLError(err) || attempt >= 5 || ctx.Err() != nil {
					break
				}
				delay := time.Duration(1<<min(attempt-1, 4)) * time.Second
				lookup.LogKV("index_source_retry", "index", info.ID, "attempt", attempt+1, "max_attempts", 5, "delay", delay, "error", err)
				timer := time.NewTimer(delay)
				select {
				case <-ctx.Done():
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					err = ctx.Err()
				case <-timer.C:
				}
				if ctx.Err() != nil {
					break
				}
			}
			stopProgress()
			if removed, cleanupErr := cleanupUnreferencedPartitionBuilds(dataDir, info.ID); cleanupErr != nil {
				lookup.LogKV("index_partition_cleanup_failed", "index", info.ID, "error", cleanupErr)
			} else if removed > 0 {
				lookup.LogKV("index_partition_cleanup", "index", info.ID, "removed_builds", removed)
			}
			if err != nil {
				mgr.SetBuildProgress(info.ID, lookup.BulkProgress{SourceName: info.ID, Seen: stats.Seen, Indexed: stats.Indexed, Skipped: stats.Skipped, LastSeq: stats.LastSeq, Partitions: stats.Partitions, StartedAt: started, Took: time.Since(started)}, false, err.Error())
				if ctx.Err() == nil {
					lookup.LogKV("index_load_failed", "index", info.ID, "error", err, "elapsed", time.Since(started))
				}
				continue
			}
			mgr.SetBuildProgress(info.ID, lookup.BulkProgress{SourceName: info.ID, Seen: stats.Seen, Indexed: stats.Indexed, Skipped: stats.Skipped, LastSeq: stats.LastSeq, Partition: stats.Partitions, Partitions: stats.Partitions, StartedAt: started, Took: time.Since(started)}, false, "")
			lookup.LogKV("index_ready", "index", info.ID, "state", "partitioned", "storage", "disk", "rows_seen", stats.Seen, "rows_indexed", stats.Indexed, "rows_skipped", stats.Skipped, "partitions", stats.Partitions, "elapsed", time.Since(started))
			continue
		}
		stopProgress := startIndexProgress(ctx, mgr, info.ID)
		stats, err := mgr.Reload(ctx, info.ID)
		stopProgress()
		if err != nil {
			if ctx.Err() == nil {
				lookup.LogKV("index_load_failed", "index", info.ID, "error", err, "elapsed", time.Since(started))
			}
			continue
		}
		ready, _ := mgr.Managed(info.ID)
		if ready == nil {
			continue
		}
		indexFields := []any{"index", info.ID, "state", "indexed", "storage", storage, "rows_seen", stats.Seen, "rows_indexed", stats.Indexed, "rows_skipped", stats.Skipped, "docs", ready.Index.Stats().Docs, "rows_per_second", fmt.Sprintf("%.1f", stats.RowsPerSecond), "elapsed", time.Since(started)}
		lookup.LogKV("index_ready", indexFields...)
	}
}

// cleanupUnreferencedPartitionBuilds removes partition indexes left behind by
// retries or replaced builds. The published catalog is the source of truth:
// every partition it references remains available to active queries.
func cleanupUnreferencedPartitionBuilds(dataDir, indexID string) (int, error) {
	catalogPath := filepath.Join(dataDir, strings.ToLower(strings.TrimSpace(indexID)), "partitions.json")
	dataset, err := lookup.LoadPartitionDataset(catalogPath)
	if err != nil && !os.IsNotExist(err) {
		return 0, fmt.Errorf("read published partition catalog: %w", err)
	}
	keep := make(map[string]struct{}, len(dataset.Generations))
	for _, manifest := range dataset.Generations {
		keep[strings.ToLower(strings.TrimSpace(manifest.IndexID))] = struct{}{}
	}
	// Keep committed resume partitions as well as the serving generation.
	// They may belong to a replacement build that failed before publication.
	progress, progressErr := lookup.LoadPartitionDataset(catalogPath + ".progress.json")
	if progressErr != nil && !os.IsNotExist(progressErr) {
		return 0, fmt.Errorf("read partition build checkpoint: %w", progressErr)
	}
	if progressErr == nil && progress.Fingerprint != "" {
		for _, manifest := range progress.Generations {
			keep[strings.ToLower(strings.TrimSpace(manifest.IndexID))] = struct{}{}
		}
	}
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	prefix := strings.ToLower(strings.TrimSpace(indexID)) + "-b"
	removed := 0
	for _, entry := range entries {
		name := strings.ToLower(entry.Name())
		if !entry.IsDir() || !strings.HasPrefix(name, prefix) {
			continue
		}
		if _, referenced := keep[name]; referenced {
			continue
		}
		if err := os.RemoveAll(filepath.Join(dataDir, entry.Name())); err != nil {
			return removed, fmt.Errorf("remove unreferenced partition %q: %w", entry.Name(), err)
		}
		removed++
	}
	return removed, nil
}

// startIndexProgress renders a single updating terminal line. When output is
// redirected, it emits periodic structured progress events instead.
func startIndexProgress(ctx context.Context, mgr *lookup.MultiIndexManager, id string) func() {
	stop := make(chan struct{})
	done := make(chan struct{})
	info, err := os.Stderr.Stat()
	terminal := err == nil && info.Mode()&os.ModeCharDevice != 0
	go func() {
		defer close(done)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		spin := []string{"|", "/", "-", "\\"}
		lastLog := time.Now()
		drawn := false
		observedReload := false
		for {
			select {
			case <-ctx.Done():
				if terminal && drawn {
					fmt.Fprintln(os.Stderr)
				}
				return
			case <-stop:
				if terminal && drawn {
					fmt.Fprintln(os.Stderr)
				}
				return
			case now := <-ticker.C:
				var current *lookup.ManagedIndexInfo
				for _, item := range mgr.List() {
					if item.ID == id {
						copy := item
						current = &copy
						break
					}
				}
				if current == nil {
					continue
				}
				progress := current.Progress
				elapsed := now.Sub(progress.StartedAt)
				if elapsed < 0 {
					elapsed = 0
				}
				rate := float64(0)
				if elapsed > 0 {
					rate = float64(progress.Seen) / elapsed.Seconds()
				}
				partition := ""
				if progress.Partition > 0 {
					partition = fmt.Sprintf("  partition %s (%s rows)", formatProgressNumber(uint64(progress.Partition)), formatProgressNumber(progress.CurrentPartitionRows))
				}
				if terminal {
					fmt.Fprintf(os.Stderr, "\r\033[36m%s\033[0m  %-18s  %s rows seen  %s indexed  %s skipped  %s rows/s%s  %s elapsed\033[K",
						spin[int(now.Unix())%len(spin)], id, formatProgressNumber(progress.Seen), formatProgressNumber(progress.Indexed), formatProgressNumber(progress.Skipped), formatProgressNumber(uint64(rate)), partition, formatProgressDuration(elapsed))
					drawn = true
				} else if now.Sub(lastLog) >= 5*time.Second {
					lookup.LogKV("index_load_progress", "index", id, "rows_seen", progress.Seen, "rows_indexed", progress.Indexed, "rows_skipped", progress.Skipped, "partition", progress.Partition, "completed_partitions", progress.Partitions, "current_partition_rows", progress.CurrentPartitionRows, "rows_per_second", fmt.Sprintf("%.1f", rate), "elapsed", elapsed.Round(time.Second))
					lastLog = now
				}
				if current.Reloading {
					observedReload = true
				} else if observedReload {
					return
				}
			}
		}
	}()
	return func() {
		close(stop)
		<-done
	}
}

func formatProgressNumber(n uint64) string {
	value := strconv.FormatUint(n, 10)
	if len(value) <= 3 {
		return value
	}
	first := len(value) % 3
	if first == 0 {
		first = 3
	}
	var formatted strings.Builder
	formatted.Grow(len(value) + len(value)/3)
	formatted.WriteString(value[:first])
	for i := first; i < len(value); i += 3 {
		formatted.WriteByte(',')
		formatted.WriteString(value[i : i+3])
	}
	return formatted.String()
}

func formatProgressDuration(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	seconds := int64(d.Round(time.Second).Seconds())
	return fmt.Sprintf("%02d:%02d:%02d", seconds/3600, (seconds/60)%60, seconds%60)
}

func splitNonEmptyCSV(value string) []string {
	var values []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			values = append(values, item)
		}
	}
	return values
}

// loadDotEnvFile loads simple KEY=VALUE entries without replacing values already
// supplied by the process environment. It runs before BCL env() interpolation.
func loadDotEnvFile(path string) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimPrefix(line, "export ")
		key, value, ok := strings.Cut(line, "=")
		key, value = strings.TrimSpace(key), strings.TrimSpace(value)
		if !ok || key == "" {
			continue
		}
		if len(value) >= 2 && ((value[0] == '"' && value[len(value)-1] == '"') || (value[0] == '\'' && value[len(value)-1] == '\'')) {
			value = value[1 : len(value)-1]
		}
		if _, exists := os.LookupEnv(key); !exists {
			_ = os.Setenv(key, value)
		}
	}
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
	id := fs.String("index", "", "index id (required)")
	data := fs.String("data", "./data/indexes", "persistent index directory")
	raw := fs.String("q", "term=key-0013&group_id=4&date_key=2026-01-01", "lookup query string")
	limit := fs.Int("limit", 5, "limit")
	_ = fs.Parse(args)
	if *id == "" {
		log.Fatal("-index flag is required")
	}
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
	id := fs.String("index", "", "index id (required)")
	rows := fs.Int("rows", 100000, "demo rows")
	raw := fs.String("q", "term=key-0013&group_id=4&date_key=2026-01-01", "lookup query string")
	limit := fs.Int("limit", 5, "limit")
	_ = fs.Parse(args)
	if *id == "" {
		log.Fatal("-index flag is required")
	}
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
	id := fs.String("index", "", "index id (required)")
	data := fs.String("data", "./data/indexes", "persistent index directory")
	_ = fs.Parse(args)
	if *id == "" {
		log.Fatal("-index flag is required")
	}
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
	id := fs.String("index", "", "index id (required)")
	data := fs.String("data", "./data/indexes", "persistent index directory")
	_ = fs.Parse(args)
	if *id == "" {
		log.Fatal("-index flag is required")
	}
	man, rep, err := lookup.RepairPersistentIndex(context.Background(), lookup.FileSegmentStore{Root: *data}, *id, lookup.Config{})
	if err != nil {
		log.Fatal(err)
	}
	b, _ := jsonMarshalIndent(map[string]any{"manifest": man, "validation": rep})
	fmt.Println(string(b))
}

func compactCmd(args []string) {
	fs := flag.NewFlagSet("compact", flag.ExitOnError)
	id := fs.String("index", "", "index id (required)")
	data := fs.String("data", "./data/indexes", "persistent index directory")
	keep := fs.Int("keep", 2, "generations to keep")
	_ = fs.Parse(args)
	if *id == "" {
		log.Fatal("-index flag is required")
	}
	removed, err := lookup.CompactPersistentGenerations(context.Background(), lookup.FileSegmentStore{Root: *data}, *id, lookup.GenerationPolicy{KeepLast: *keep})
	if err != nil {
		log.Fatal(err)
	}
	b, _ := jsonMarshalIndent(map[string]any{"removed": removed})
	fmt.Println(string(b))
}

func generationsCmd(args []string) {
	fs := flag.NewFlagSet("generations", flag.ExitOnError)
	id := fs.String("index", "", "index id (required)")
	data := fs.String("data", "./data/indexes", "persistent index directory")
	_ = fs.Parse(args)
	if *id == "" {
		log.Fatal("-index flag is required")
	}
	gens, err := lookup.ListIndexGenerations(context.Background(), lookup.FileSegmentStore{Root: *data}, *id)
	if err != nil {
		log.Fatal(err)
	}
	b, _ := jsonMarshalIndent(gens)
	fmt.Println(string(b))
}

func planCmd(args []string) {
	fs := flag.NewFlagSet("plan", flag.ExitOnError)
	id := fs.String("index", "", "index id (required)")
	rows := fs.Int64("rows", 1_000_000_000, "estimated rows")
	_ = fs.Parse(args)
	if *id == "" {
		log.Fatal("-index flag is required")
	}
	b, _ := jsonMarshalIndent(lookup.PlanBillionRowDeployment(*id, *rows, lookup.DefaultBillionRowBudget()))
	fmt.Println(string(b))
}

func jsonMarshalIndent(v any) ([]byte, error) {
	return lookup.JSONMarshalIndent(v)
}
