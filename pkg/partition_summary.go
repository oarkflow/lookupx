package pkg

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"hash/fnv"
)

// PartitionSummary is a conservative membership filter. A negative result can
// skip a partition; a positive result always runs the normal query. Missing or
// incompatible summaries fall back to searching, preserving old catalogs.
type PartitionSummary struct {
	Schema     string            `json:"schema"`
	Fields     map[string][]byte `json:"fields"`
	Terms      int               `json:"terms,omitempty"`
	TermsKnown bool              `json:"terms_known,omitempty"`
}

func summarySchema(cfg Config) string {
	b, _ := json.Marshal(cfg.Schema)
	s := sha256.Sum256(b)
	return stringHex(s[:])
}
func stringHex(b []byte) string {
	const digits = "0123456789abcdef"
	out := make([]byte, len(b)*2)
	for i, v := range b {
		out[2*i] = digits[v>>4]
		out[2*i+1] = digits[v&15]
	}
	return string(out)
}
func bloomHashes(s string) (uint64, uint64) {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	a := h.Sum64()
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], a)
	_, _ = h.Write(b[:])
	return a, h.Sum64() | 1
}
func bloomAdd(bits []byte, s string) {
	a, b := bloomHashes(s)
	n := uint64(len(bits) * 8)
	for i := uint64(0); i < 7; i++ {
		p := (a + i*b) % n
		bits[p/8] |= 1 << (p % 8)
	}
}
func bloomHas(bits []byte, s string) bool {
	if len(bits) == 0 {
		return true
	}
	a, b := bloomHashes(s)
	n := uint64(len(bits) * 8)
	for i := uint64(0); i < 7; i++ {
		p := (a + i*b) % n
		if bits[p/8]&(1<<(p%8)) == 0 {
			return false
		}
	}
	return true
}
func (ix *Index) partitionSummary() *PartitionSummary {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	s := &PartitionSummary{Schema: summarySchema(ix.cfg), Fields: make(map[string][]byte), TermsKnown: true}
	seen := make(map[*fieldIndex]struct{}, len(ix.fields))
	for name, fi := range ix.fields {
		if _, ok := seen[fi]; ok {
			continue
		}
		seen[fi] = struct{}{}
		opt := ix.cfg.Schema.Fields[name]
		if opt.Kind != FieldKeyword && opt.Kind != FieldText && opt.Kind != FieldBool {
			continue
		}
		n := (len(fi.terms)+len(fi.termOne)+len(fi.unique))*10/8 + 64
		s.Terms += len(fi.terms) + len(fi.termOne)
		if n > 8192 {
			n = 8192
		}
		bits := make([]byte, n)
		for term := range fi.terms {
			bloomAdd(bits, term)
		}
		for term := range fi.termOne {
			bloomAdd(bits, term)
		}
		for term := range fi.unique {
			bloomAdd(bits, term)
		}
		s.Fields[name] = bits
	}
	return s
}
func (s *PartitionSummary) mayMatch(q Query, cfg Config) bool {
	if s == nil || s.Schema != summarySchema(cfg) {
		return true
	}
	return s.match(q, cfg)
}
func (s *PartitionSummary) term(field, value string, cfg Config) bool {
	bits, ok := s.Fields[field]
	if !ok {
		return true
	}
	return bloomHas(bits, normalizeSpecialString(field, value, cfg.Schema.Fields[field].Lowercase))
}
func (s *PartitionSummary) match(q Query, cfg Config) bool {
	switch x := q.(type) {
	case Term:
		return s.term(x.Field, x.Value, cfg)
	case *Term:
		if x != nil {
			return s.term(x.Field, x.Value, cfg)
		}
	case Terms:
		for _, v := range x.Values {
			if s.term(x.Field, v, cfg) {
				return true
			}
		}
		return false
	case And:
		for _, v := range x {
			if !s.match(v, cfg) {
				return false
			}
		}
		return true
	case Or:
		for _, v := range x {
			if s.match(v, cfg) {
				return true
			}
		}
		return false
	case Bool:
		for _, v := range x.Must {
			if !s.match(v, cfg) {
				return false
			}
		}
		for _, v := range x.Filter {
			if !s.match(v, cfg) {
				return false
			}
		}
		required := x.MinShouldMatch
		if required == 0 && len(x.Must) == 0 && len(x.Filter) == 0 && len(x.Should) > 0 {
			required = 1
		}
		possible := 0
		for _, v := range x.Should {
			if s.match(v, cfg) {
				possible++
			}
		}
		return possible >= required
	case GlobalTerm:
		fields := x.Fields
		if len(fields) == 0 {
			for name, opt := range cfg.Schema.Fields {
				if opt.TermSearch {
					fields = append(fields, name)
				}
			}
		}
		if len(fields) == 0 {
			return true
		}
		if x.Fuzzy {
			for _, field := range fields {
				if cfg.Schema.Fields[field].Fuzzy {
					return true
				}
			}
		}
		for _, word := range x.Words {
			possible := false
			for _, field := range fields {
				if s.term(field, word, cfg) {
					possible = true
					break
				}
			}
			if !possible {
				return false
			}
		}
		return true
	case latestPerGroupQuery:
		return s.match(x.Candidate, cfg)
	case partitionLatestQuery:
		return s.match(x.rule.Candidate, cfg)
	}
	return true
}
