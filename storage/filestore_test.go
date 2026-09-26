package storage

import (
	"context"
	"path/filepath"
	"testing"
)

func TestSaveLoadRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "registry.json")
	st := NewFileStore(path)
	ctx := context.Background()

	s := Normalize(State{})
	s.Topics["t"] = TopicConfig{Topic: "t", Compatibility: "BACKWARD", NextVersion: 2}
	s.Versions["t"] = []VersionedSchema{{Topic: "t", Version: 1, Fingerprint: "abc"}}
	s.Confirmations["t"] = map[string]int{"c1": 1}

	if err := st.Save(ctx, s); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, err := st.Load(ctx)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.Topics["t"].NextVersion != 2 || len(got.Versions["t"]) != 1 || got.Confirmations["t"]["c1"] != 1 {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
}

func TestLoadMissingFileIsEmpty(t *testing.T) {
	st := NewFileStore(filepath.Join(t.TempDir(), "absent.json"))
	got, err := st.Load(context.Background())
	if err != nil {
		t.Fatalf("load missing: %v", err)
	}
	if len(got.Topics) != 0 || len(got.Versions) != 0 || len(got.Confirmations) != 0 {
		t.Fatalf("expected empty state, got %+v", got)
	}
}

func TestFailedSaveKeepsOldSnapshot(t *testing.T) {
	// Point the store at a path whose parent is a regular file, so writes
	// fail during MkdirAll/CreateTemp: the previous snapshot must survive.
	dir := t.TempDir()
	blocker := filepath.Join(dir, "not-a-dir")
	if err := writeTestFile(blocker, []byte("x")); err != nil {
		t.Fatalf("create blocker: %v", err)
	}
	st := NewFileStore(filepath.Join(blocker, "registry.json"))
	ctx := context.Background()

	good := Normalize(State{})
	goodStore := NewFileStore(filepath.Join(dir, "good.json"))
	good.Topics["t"] = TopicConfig{Topic: "t", NextVersion: 1}
	if err := goodStore.Save(ctx, good); err != nil {
		t.Fatalf("seed good store: %v", err)
	}
	if err := st.Save(ctx, good); err == nil {
		t.Fatal("expected save to fail at an invalid path")
	}
	// The blocker file content must be untouched (no partial state written).
	data, err := readTestFile(blocker)
	if err != nil || string(data) != "x" {
		t.Fatalf("existing file corrupted: %q err=%v", data, err)
	}
}
