package pkg

import (
	"context"
	"fmt"
	"strings"
)

// A latest-per-group rule must select dates across the whole dataset before
// each partition applies its page limit. Merging partition-local top hits can
// otherwise return an older date, or omit a group's true latest record.
type partitionLatestQuery struct {
	rule   latestPerGroupQuery
	maxima map[string]float64
}

func latestGroupKey(ix *Index, fields []string, id DocID) (string, bool) {
	var b strings.Builder
	for _, field := range fields {
		if int(id) >= ix.stringColumnLen(field) {
			return "", false
		}
		raw, _ := ix.stringValue(field, id)
		value := normalize(raw, ix.cfg.Schema.Fields[field].Lowercase)
		fmt.Fprintf(&b, "%d:", len(value))
		b.WriteString(value)
	}
	return b.String(), true
}
func (q partitionLatestQuery) eval(ix *Index) *Bitmap {
	candidates := q.rule.Candidate.eval(ix)
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	dates, exists := ix.numericDense[q.rule.DateField], ix.numericExists[q.rule.DateField]
	out := NewBitmap()
	candidates.Each(func(id DocID) bool {
		if id == 0 || int(id) >= len(dates) || exists == nil || !exists.Has(id) || ix.isDeletedOrExpiredLocked(id) {
			return true
		}
		if key, ok := latestGroupKey(ix, q.rule.GroupBy, id); ok {
			if max, found := q.maxima[key]; found && dates[id] == max {
				out.Add(id)
			}
		}
		return true
	})
	return out
}
func preparePartitionLatest(ctx context.Context, store PersistentStore, manifests []PersistentManifest, cfg Config, rule latestPerGroupQuery, signature [32]byte) (Query, error) {
	maxima := make(map[string]float64)
	for _, man := range manifests {
		select {
		case partitionSearchSlots <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		ix, release, err := servingPartitions.acquire(ctx, store, man, cfg, signature)
		if err != nil {
			<-partitionSearchSlots
			return nil, err
		}
		candidates := rule.Candidate.eval(ix)
		ix.mu.RLock()
		dates, exists := ix.numericDense[rule.DateField], ix.numericExists[rule.DateField]
		candidates.Each(func(id DocID) bool {
			if id == 0 || int(id) >= len(dates) || exists == nil || !exists.Has(id) || ix.isDeletedOrExpiredLocked(id) {
				return true
			}
			key, ok := latestGroupKey(ix, rule.GroupBy, id)
			if !ok {
				return true
			}
			if current, ok := maxima[key]; !ok || dates[id] > current {
				maxima[key] = dates[id]
			}
			return true
		})
		ix.mu.RUnlock()
		release()
		<-partitionSearchSlots
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	return partitionLatestQuery{rule: rule, maxima: maxima}, nil
}
