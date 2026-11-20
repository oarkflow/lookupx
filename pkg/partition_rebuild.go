package pkg

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// RebuildPartitionedIndex performs a full partitioned rebuild of a single
// disk-storage index from its configured datasource, replacing the
// previously served partitions. It mirrors the startup build path in
// cmd/lookupx (streams the source, writes fresh partitions, retries
// transient SQL errors, cleans up superseded partition builds, then warms
// the new partitions into the serving cache) so a server restart and an
// admin-triggered rebuild produce identical results. Progress is reported
// through the manager's existing Reloading/Progress fields, so callers that
// already poll ManagedIndexInfo see live progress for free.
func RebuildPartitionedIndex(ctx context.Context, mgr *MultiIndexManager, dataDir, indexID string) (PartitionedBuildStats, error) {
	managed, ok := mgr.Managed(indexID)
	if !ok {
		return PartitionedBuildStats{}, fmt.Errorf("index %q not found", indexID)
	}
	if managed.Source == nil {
		return PartitionedBuildStats{}, fmt.Errorf("index %q has no configured source", indexID)
	}
	started := time.Now()
	schemaConfig := managed.Config
	schemaConfig.InitialCapacity = 0
	ix, err := New(schemaConfig)
	if err != nil {
		return PartitionedBuildStats{}, err
	}
	src, err := managed.Source(ix)
	if err != nil {
		_ = ix.Close()
		return PartitionedBuildStats{}, err
	}
	// The datasource now has resolved FieldIDs. Release this temporary
	// schema index before the builder allocates its first partition.
	_ = ix.Close()

	bulk := managed.Bulk
	if bulk.BatchSize <= 0 {
		bulk.BatchSize = LowMemoryBillionBudget().DefaultBatchSize
	}
	mgr.SetBuildProgress(indexID, BulkProgress{SourceName: indexID, StartedAt: started}, true, "")

	catalogPath := filepath.Join(dataDir, cleanIndexID(indexID), "partitions.json")
	catalog, catalogErr := LoadPartitionDataset(catalogPath)
	publishProgress := os.IsNotExist(catalogErr)
	if catalogErr == nil && !catalog.Complete && catalog.Fingerprint != "" {
		progress, progressErr := LoadPartitionDataset(catalogPath + ".progress.json")
		publishProgress = progressErr == nil && progress.BuildID == catalog.BuildID && progress.Fingerprint == catalog.Fingerprint
	}
	if identity, ok := src.(SourceIdentity); ok {
		if _, resumable := src.(ResumableSource); resumable && identity.ResumeIdentity() != "" {
			bulk.Resume = true
		}
	}
	partitionOptions := PartitionedBuildOptions{
		IndexID: indexID, Store: FileMMapSegmentStore{Root: dataDir},
		CatalogPath: catalogPath, DatasetRoot: dataDir, PublishProgress: publishProgress,
		Config: managed.Config, RowsPerPartition: uint64(LowMemoryBillionBudget().TargetRowsPerPartition),
		Bulk: bulk, Freeze: true,
		Progress: func(p PartitionedBuildProgress) {
			mgr.SetBuildProgress(indexID, BulkProgress{SourceName: indexID, Seen: p.Seen, Indexed: p.Indexed, Skipped: p.Skipped, LastSeq: p.LastSeq, Partition: p.PartitionNo + 1, Partitions: p.Partitions, CurrentPartitionRows: p.CurrentRows, StartedAt: started, Took: p.Took}, true, "")
		},
	}

	var stats PartitionedBuildStats
	for attempt := 1; ; attempt++ {
		stats, err = BuildPartitionedPersistent(ctx, src, partitionOptions)
		if err == nil || !IsTransientSQLError(err) || attempt >= 5 || ctx.Err() != nil {
			break
		}
		delay := time.Duration(1<<min(attempt-1, 4)) * time.Second
		LogKV("index_source_retry", "index", indexID, "attempt", attempt+1, "max_attempts", 5, "delay", delay, "error", err)
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

	if removed, cleanupErr := CleanupUnreferencedPartitionBuilds(dataDir, indexID); cleanupErr != nil {
		LogKV("index_partition_cleanup_failed", "index", indexID, "error", cleanupErr)
	} else if removed > 0 {
		LogKV("index_partition_cleanup", "index", indexID, "removed_builds", removed)
	}

	if err != nil {
		mgr.SetBuildProgress(indexID, BulkProgress{SourceName: indexID, Seen: stats.Seen, Indexed: stats.Indexed, Skipped: stats.Skipped, LastSeq: stats.LastSeq, Partitions: stats.Partitions, StartedAt: started, Took: time.Since(started)}, false, err.Error())
		return stats, err
	}
	mgr.SetBuildProgress(indexID, BulkProgress{SourceName: indexID, Seen: stats.Seen, Indexed: stats.Indexed, Skipped: stats.Skipped, LastSeq: stats.LastSeq, Partition: stats.Partitions, Partitions: stats.Partitions, StartedAt: started, Took: time.Since(started)}, false, "")
	LogKV("index_ready", "index", indexID, "state", "partitioned", "storage", "disk", "rows_seen", stats.Seen, "rows_indexed", stats.Indexed, "rows_skipped", stats.Skipped, "partitions", stats.Partitions, "elapsed", time.Since(started))

	warmStarted := time.Now()
	warmed, warmErr := WarmPartitions(ctx, FileMMapSegmentStore{Root: dataDir}, stats.Manifests, managed.Config)
	if warmErr != nil {
		LogKV("index_partitions_warm_failed", "index", indexID, "warmed", warmed, "error", warmErr, "elapsed", time.Since(warmStarted))
	} else {
		LogKV("index_partitions_warmed", "index", indexID, "partitions", warmed, "elapsed", time.Since(warmStarted))
	}
	return stats, nil
}

// CleanupUnreferencedPartitionBuilds removes partition indexes left behind
// by retries or replaced builds. The published catalog is the source of
// truth: every partition it references remains available to active queries.
func CleanupUnreferencedPartitionBuilds(dataDir, indexID string) (int, error) {
	catalogPath := filepath.Join(dataDir, cleanIndexID(indexID), "partitions.json")
	dataset, err := LoadPartitionDataset(catalogPath)
	if err != nil && !os.IsNotExist(err) {
		return 0, fmt.Errorf("read published partition catalog: %w", err)
	}
	keep := make(map[string]struct{}, len(dataset.Generations))
	for _, manifest := range dataset.Generations {
		keep[cleanIndexID(manifest.IndexID)] = struct{}{}
	}
	// Keep committed resume partitions as well as the serving generation.
	// They may belong to a replacement build that failed before publication.
	progress, progressErr := LoadPartitionDataset(catalogPath + ".progress.json")
	if progressErr != nil && !os.IsNotExist(progressErr) {
		return 0, fmt.Errorf("read partition build checkpoint: %w", progressErr)
	}
	if progressErr == nil && progress.Fingerprint != "" {
		for _, manifest := range progress.Generations {
			keep[cleanIndexID(manifest.IndexID)] = struct{}{}
		}
	}
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	prefix := cleanIndexID(indexID) + "-b"
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

var errRebuildRequiresDiskStorage = errors.New("rebuild only applies to disk-storage partitioned indexes")
