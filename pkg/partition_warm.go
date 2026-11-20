package pkg

import "context"

// WarmPartitions proactively opens every manifest's serving partition into
// the shared partition cache (servingPartitions) and immediately releases
// it. This pays the cold mmap-decode cost once, in the background, right
// after a build/reload finishes, instead of inline with the first real
// user query that happens to land on a given partition.
//
// Each partition is released back to the cache rather than pinned, so a
// warmed-but-idle partition is still evictable under memory pressure like
// any other cache entry; warming only changes when the decode cost is paid,
// not the low-memory retention policy itself.
func WarmPartitions(ctx context.Context, store PersistentStore, manifests []PersistentManifest, cfg Config) (warmed int, err error) {
	if store == nil {
		return 0, nil
	}
	signature := partitionConfigSignature(cfg)
	for _, man := range manifests {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return warmed, ctxErr
		}
		_, release, openErr := servingPartitions.acquire(ctx, store, man, cfg, signature)
		if openErr != nil {
			err = openErr
			continue
		}
		release()
		warmed++
	}
	return warmed, err
}
