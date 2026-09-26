package store

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestMutateAtomicityAndReload(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	ctx := context.Background()

	s, err := New(ctx, path)
	if err != nil {
		t.Fatal(err)
	}

	// Successful mutation persists.
	if err := s.Mutate(ctx, func(st *State) error {
		st.Topics["orders"] = []VersionRecord{{Version: 1, SchemaID: "aaa", Raw: "{}"}}
		st.Checkpoints["orders"] = map[string]int{"c1": 1}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// Failing mutation must not touch memory or disk state.
	wantErr := errors.New("boom")
	if err := s.Mutate(ctx, func(st *State) error {
		st.Topics["orders"] = append(st.Topics["orders"], VersionRecord{Version: 2})
		return wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("expected propagated error, got %v", err)
	}
	snap := s.Snapshot()
	if len(snap.Topics["orders"]) != 1 {
		t.Fatalf("in-memory state changed after failed mutation: %+v", snap.Topics)
	}
	if onDisk, err := os.ReadFile(path); err != nil || filepath.Ext(path) != ".json" {
		t.Fatal(err)
	} else if len(onDisk) == 0 {
		t.Fatal("state file unexpectedly empty")
	}

	// Reload from disk restores the committed state exactly.
	s2, err := New(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	got := s2.Snapshot()
	if len(got.Topics["orders"]) != 1 || got.Topics["orders"][0].SchemaID != "aaa" {
		t.Fatalf("reloaded topics wrong: %+v", got.Topics)
	}
	if got.Checkpoints["orders"]["c1"] != 1 {
		t.Fatalf("reloaded checkpoints wrong: %+v", got.Checkpoints)
	}

	// No temp files are left behind.
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".tmp" {
			t.Fatalf("temporary file left behind: %s", e.Name())
		}
	}
}

func TestMutateWriteFailureKeepsOldState(t *testing.T) {
	ctx := context.Background()
	// A path whose directory is a regular file makes the temp-file creation
	// fail deterministically after a valid initial store exists elsewhere.
	dir := t.TempDir()
	goodPath := filepath.Join(dir, "state.json")
	s, err := New(ctx, goodPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Mutate(ctx, func(st *State) error {
		st.Topics["t"] = []VersionRecord{{Version: 1}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// Point a second store at an impossible path (parent is a file).
	blocker := filepath.Join(dir, "afile")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	s3, err := New(ctx, filepath.Join(blocker, "state.json"))
	if err != nil {
		// New itself may fail reading; construct an empty store manually.
		s3 = &Store{path: filepath.Join(blocker, "state.json"), state: emptyState(), loaded: true}
	}
	err = s3.Mutate(ctx, func(st *State) error {
		st.Topics["t"] = []VersionRecord{{Version: 9}}
		return nil
	})
	if err == nil {
		t.Fatal("expected write failure when parent is a file")
	}
	if got := s3.Snapshot().Topics["t"]; len(got) == 1 && got[0].Version == 9 {
		t.Fatal("failed write must not install the candidate state in memory")
	}
}

func TestCorruptFileIsExplicitError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	if err := os.WriteFile(path, []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := New(context.Background(), path); err == nil {
		t.Fatal("corrupt state file must be an explicit error, not silently reset")
	} else if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt, got %v", err)
	}
}

func TestSnapshotIsolation(t *testing.T) {
	ctx := context.Background()
	s, _ := New(ctx, filepath.Join(t.TempDir(), "s.json"))
	_ = s.Mutate(ctx, func(st *State) error {
		st.Topics["t"] = []VersionRecord{{Version: 1, SchemaID: "x"}}
		return nil
	})
	snap := s.Snapshot()
	snap.Topics["t"][0].SchemaID = "tampered"
	snap2 := s.Snapshot()
	if snap2.Topics["t"][0].SchemaID != "x" {
		t.Fatal("mutating a snapshot must not affect store state")
	}
}
