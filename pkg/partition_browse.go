package pkg

import (
	"context"
	"errors"
	"math"
	"time"
)

func unfilteredPartitionQuery(q Query) bool {
	switch v := q.(type) {
	case nil, MatchAll:
		return true
	case *MatchAll:
		return v != nil
	default:
		return false
	}
}

// Browsing has no relevance scores to merge. Use catalog partition order and
// each partition's live DocID order, matching ordinary in-memory MatchAll
// pagination. Only open partitions needed for the page. Immutable manifests
// supply counts unless TTL makes the visible population time-dependent.
func browsePartitionedPersistent(ctx context.Context, store PersistentStore, manifests []PersistentManifest, cfg Config, req SearchRequest, dst []Hit, signature [32]byte) (Result, error) {
	started := time.Now()
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	ttl := false
	for _, field := range cfg.Schema.Fields {
		if field.TTLField {
			ttl = true
			break
		}
	}
	total := 0
	if !req.SkipTotal && !ttl {
		for _, man := range manifests {
			if man.Docs > uint64(math.MaxInt-total) {
				return Result{}, errors.New("partitioned browse total exceeds platform integer range")
			}
			total += int(man.Docs)
		}
	}
	schema := summarySchema(cfg)
	dst = dst[:0]
	offset := req.Offset
	for _, man := range manifests {
		if err := ctx.Err(); err != nil {
			return Result{}, err
		}
		needPage := len(dst) < req.Limit
		if !needPage && (req.SkipTotal || !ttl) {
			break
		}
		// With no TTL, the catalog's live count can skip entire partitions for a
		// deep offset without opening their dictionaries or stored columns.
		if !ttl && needPage && uint64(offset) >= man.Docs {
			offset -= int(man.Docs)
			continue
		}
		if !ttl && needPage {
			if hits, ok := previewPartitionPage(man, schema, offset, req.Limit-len(dst), req.WithDocs); ok {
				dst = append(dst, hits...)
				offset = 0
				continue
			}
		}
		select {
		case partitionSearchSlots <- struct{}{}:
		case <-ctx.Done():
			return Result{}, ctx.Err()
		}
		ix, release, err := servingPartitions.acquire(ctx, store, man, cfg, signature)
		if err != nil {
			<-partitionSearchSlots
			return Result{}, err
		}
		visible := int(man.Docs)
		if ttl {
			visible = ix.Count(MatchAll{})
			if !req.SkipTotal {
				if visible > math.MaxInt-total {
					release()
					<-partitionSearchSlots
					return Result{}, errors.New("partitioned browse total exceeds platform integer range")
				}
				total += visible
			}
		}
		if needPage && offset < visible {
			localReq := SearchRequest{Query: MatchAll{}, Limit: req.Limit - len(dst), Offset: offset, SkipTotal: true, WithDocs: req.WithDocs}
			_, hits := ix.SearchInto(localReq, nil)
			detachPartitionHits(hits)
			dst = append(dst, hits...)
			offset = 0
		} else if needPage {
			offset -= visible
		}
		release()
		<-partitionSearchSlots
	}
	return Result{Hits: dst, Total: total, Took: time.Since(started).Nanoseconds()}, nil
}
