package pkg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// FileMMapSegmentStore writes mapped snapshots for partition serving. LoadIndex
// returns a mutable heap copy for callers that need updates; the partition query
// path maps posting and column arrays directly from immutable generation files.
// Legacy gob/gzip snapshots remain readable.
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
	payload := filepath.Join(genDir, "segment.dat")
	f, err := os.Create(payload + ".tmp")
	if err != nil {
		return PersistentManifest{}, err
	}
	if err = ix.encodeMapped(f); err == nil {
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
	if err := ix.writePartitionPreview(filepath.Join(genDir, "browse.gob"), gen); err != nil {
		return PersistentManifest{}, err
	}
	checksum, _ := ChecksumFile(payload)
	stats := ix.Stats()
	genMan := IndexGeneration{IndexID: cleanIndexID(indexID), Generation: gen, CreatedAt: time.Now().UTC(), Docs: uint64(stats.Docs), LiveDocs: uint64(stats.LiveDocs), DeletedDocs: uint64(stats.DeletedDocs), Layout: LayoutMMapParts, Path: genDir, Checksum: checksum, Frozen: ix.IsFrozen()}
	gb, _ := json.MarshalIndent(genMan, "", "  ")
	if err := writeSyncedFile(filepath.Join(genDir, "generation.json"), gb, 0644); err != nil {
		return PersistentManifest{}, err
	}
	man := PersistentManifest{IndexID: cleanIndexID(indexID), Generation: gen, CreatedAt: genMan.CreatedAt, Docs: uint64(stats.LiveDocs), Path: genDir, Frozen: genMan.Frozen, Format: string(LayoutMMapParts)}
	man.Summary = ix.partitionSummary()
	mb, _ := json.MarshalIndent(man, "", "  ")
	if err := writeSyncedFile(filepath.Join(genDir, "manifest.json"), mb, 0644); err != nil {
		return PersistentManifest{}, err
	}
	// Persist the completed generation directory before publishing its pointer.
	// A crash must leave the previous CURRENT usable, never a half-written one.
	if err := syncDir(genDir); err != nil {
		return PersistentManifest{}, err
	}
	if err := syncDir(base); err != nil {
		return PersistentManifest{}, err
	}
	if err := replaceCurrentPointer(base, genName); err != nil {
		return PersistentManifest{}, err
	}
	return man, syncDir(base)
}

func writeSyncedFile(path string, data []byte, mode os.FileMode) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func replaceCurrentPointer(base, generation string) error {
	tmp, err := os.CreateTemp(base, ".CURRENT-*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0644); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.WriteString(generation); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, filepath.Join(base, "CURRENT")); err != nil {
		return err
	}
	return nil
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
	magic := make([]byte, len(mappedMagic))
	_, _ = f.ReadAt(magic, 0)
	if isMappedMagic(string(magic)) {
		var data []byte
		data, err = io.ReadAll(f)
		if err == nil {
			err = decodeMapped(data, &dump, false)
		}
	} else {
		err = decodePersistent(f, &dump)
	}
	if err != nil {
		return nil, man, err
	}
	if cfg.Schema.Fields != nil {
		dump.Config.Schema = cfg.Schema
	}
	dump.Config.InitialCapacity = 0
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
