package main

import (
	lookup "github.com/oarkflow/lookupx/pkg"
	"os"
	"path/filepath"
	"testing"
)

func TestCleanupPreservesPublishedAndResumablePartitions(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{"cpt-bactive-p000000", "cpt-bresume-p000000", "cpt-borphan-p000000", "other-bkeep-p000000"} {
		if err := os.MkdirAll(filepath.Join(root, name), 0755); err != nil {
			t.Fatal(err)
		}
	}
	catalog := filepath.Join(root, "cpt", "partitions.json")
	published := lookup.PartitionDataset{BuildID: "active", Complete: true, Generations: []lookup.PersistentManifest{{IndexID: "cpt-bactive-p000000"}}}
	if err := published.Save(catalog); err != nil {
		t.Fatal(err)
	}
	progress := lookup.PartitionDataset{BuildID: "resume", Fingerprint: "stable-source", Generations: []lookup.PersistentManifest{{IndexID: "cpt-bresume-p000000"}}}
	if err := progress.Save(catalog + ".progress.json"); err != nil {
		t.Fatal(err)
	}
	removed, err := cleanupUnreferencedPartitionBuilds(root, "cpt")
	if err != nil || removed != 1 {
		t.Fatalf("removed=%d err=%v", removed, err)
	}
	for _, name := range []string{"cpt-bactive-p000000", "cpt-bresume-p000000", "other-bkeep-p000000"} {
		if _, err := os.Stat(filepath.Join(root, name)); err != nil {
			t.Fatalf("deleted referenced partition %s: %v", name, err)
		}
	}
	if _, err := os.Stat(filepath.Join(root, "cpt-borphan-p000000")); !os.IsNotExist(err) {
		t.Fatalf("orphan still exists: %v", err)
	}
}
