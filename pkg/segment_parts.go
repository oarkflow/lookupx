package pkg

import (
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// FileMMapSegmentStore is a production-oriented file layout that separates
// metadata from the binary payload and records checksums. The payload is still
// decoded into the current in-memory engine on open, but the directory/manifest
// structure is compatible with later mmap readers and object-store replication.
type FileMMapSegmentStore struct{ Root string }

func (s FileMMapSegmentStore) dir(indexID string) string {
	return filepath.Join(s.Root, cleanIndexID(indexID))
}

func (s FileMMapSegmentStore) SaveIndex(ctx context.Context, indexID string, ix *Index) (PersistentManifest, error) {
	if s.Root == "" {
		return PersistentManifest{}, errors.New("store root required")
	}
	if ix == nil {
		return PersistentManifest{}, errors.New("nil index")
	}
	_ = ix.Freeze()
	base := s.dir(indexID)
	if err := os.MkdirAll(base, 0755); err != nil {
		return PersistentManifest{}, err
	}
	gen := uint64(time.Now().UnixNano())
	genName := fmt.Sprintf("generation-%020d", gen)
	genDir := filepath.Join(base, genName)
	if err := os.MkdirAll(genDir, 0755); err != nil {
		return PersistentManifest{}, err
	}
	dump, err := ix.persistentDump()
	if err != nil {
		return PersistentManifest{}, err
	}
	payload := filepath.Join(genDir, "segment.dat")
	f, err := os.Create(payload + ".tmp")
	if err != nil {
		return PersistentManifest{}, err
	}
	if err = gob.NewEncoder(f).Encode(dump); err == nil {
		err = f.Sync()
		if err == nil {
			err = f.Close()
		}
	} else {
		_ = f.Close()
	}
	if err != nil {
		return PersistentManifest{}, err
	}
	if err = os.Rename(payload+".tmp", payload); err != nil {
		return PersistentManifest{}, err
	}
	checksum, _ := ChecksumFile(payload)
	stats := ix.Stats()
	genMan := IndexGeneration{IndexID: cleanIndexID(indexID), Generation: gen, CreatedAt: time.Now().UTC(), Docs: uint64(stats.Docs), LiveDocs: uint64(stats.LiveDocs), DeletedDocs: uint64(stats.DeletedDocs), Layout: LayoutMMapParts, Path: genDir, Checksum: checksum, Frozen: ix.IsFrozen()}
	gb, _ := json.MarshalIndent(genMan, "", "  ")
	if err := os.WriteFile(filepath.Join(genDir, "generation.json"), gb, 0644); err != nil {
		return PersistentManifest{}, err
	}
	man := PersistentManifest{IndexID: cleanIndexID(indexID), Generation: gen, CreatedAt: genMan.CreatedAt, Docs: uint64(stats.LiveDocs), Path: genDir, Frozen: genMan.Frozen, Format: string(LayoutMMapParts)}
	mb, _ := json.MarshalIndent(man, "", "  ")
	if err := os.WriteFile(filepath.Join(genDir, "manifest.json"), mb, 0644); err != nil {
		return PersistentManifest{}, err
	}
	return man, os.WriteFile(filepath.Join(base, "CURRENT"), []byte(genName), 0644)
}

func (s FileMMapSegmentStore) LoadIndex(ctx context.Context, indexID string, cfg Config) (*Index, PersistentManifest, error) {
	base := s.dir(indexID)
	cur, err := os.ReadFile(filepath.Join(base, "CURRENT"))
	if err != nil {
		return nil, PersistentManifest{}, err
	}
	genDir := filepath.Join(base, strings.TrimSpace(string(cur)))
	mb, err := os.ReadFile(filepath.Join(genDir, "manifest.json"))
	if err != nil {
		return nil, PersistentManifest{}, err
	}
	var man PersistentManifest
	if err = json.Unmarshal(mb, &man); err != nil {
		return nil, man, err
	}
	f, err := os.Open(filepath.Join(genDir, "segment.dat"))
	if err != nil {
		return nil, man, err
	}
	defer f.Close()
	var dump persistentDump
	if err = gob.NewDecoder(f).Decode(&dump); err != nil {
		return nil, man, err
	}
	if cfg.Schema.Fields != nil {
		dump.Config.Schema = cfg.Schema
	}
	ix, err := New(dump.Config)
	if err != nil {
		return nil, man, err
	}
	if err = ix.restorePersistentDump(&dump); err != nil {
		_ = ix.Close()
		return nil, man, err
	}
	return ix, man, nil
}

func (s FileMMapSegmentStore) ListGenerations(ctx context.Context, indexID string) ([]PersistentManifest, error) {
	base := s.dir(indexID)
	ents, err := os.ReadDir(base)
	if err != nil {
		return nil, err
	}
	out := []PersistentManifest{}
	for _, e := range ents {
		if !e.IsDir() || !strings.HasPrefix(e.Name(), "generation-") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(base, e.Name(), "manifest.json"))
		if err != nil {
			continue
		}
		var m PersistentManifest
		if json.Unmarshal(b, &m) == nil {
			out = append(out, m)
		}
	}
	return out, nil
}
