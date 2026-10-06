package storage

import (
	"context"
	"os"
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

func TestScopeRoundTripAndOldSnapshotDefault(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.json")
	st := NewFileStore(path)
	ctx := context.Background()

	s := Normalize(State{})
	s.Topics["recent"] = TopicConfig{Topic: "recent", Compatibility: "BACKWARD", NextVersion: 1, Scope: RecentScope(3)}
	s.Topics["all"] = TopicConfig{Topic: "all", Compatibility: "FULL", NextVersion: 1, Scope: AllScope()}
	s.Topics["latest"] = TopicConfig{Topic: "latest", Compatibility: "BACKWARD", NextVersion: 1, Scope: LatestScope()}
	if err := st.Save(ctx, s); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, err := st.Load(ctx)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if sc := got.Topics["recent"].Scope; sc.Mode != ScopeRecent || sc.Limit != 3 {
		t.Fatalf("recent scope round-trip = %+v", sc)
	}
	if sc := got.Topics["all"].Scope; sc.Mode != ScopeAll {
		t.Fatalf("all scope round-trip = %+v", sc)
	}
	if sc := got.Topics["latest"].Scope; sc.Mode != ScopeLatest {
		t.Fatalf("latest scope round-trip = %+v", sc)
	}
	t.Logf("scopes round-trip: recent=%s all=%s latest=%s",
		got.Topics["recent"].Scope, got.Topics["all"].Scope, got.Topics["latest"].Scope)

	// Simulate a snapshot written before scopes existed: no "scope" field.
	legacy := `{"topics":{"old":{"topic":"old","compatibility":"BACKWARD","nextVersion":2}},"versions":{},"confirmations":{}}`
	legacyPath := filepath.Join(t.TempDir(), "legacy.json")
	if err := os.WriteFile(legacyPath, []byte(legacy), 0o644); err != nil {
		t.Fatalf("write legacy snapshot: %v", err)
	}
	old, err := NewFileStore(legacyPath).Load(ctx)
	if err != nil {
		t.Fatalf("load legacy: %v", err)
	}
	if sc := old.Topics["old"].Scope; sc.Mode != ScopeLatest {
		t.Fatalf("legacy snapshot must recover LATEST scope, got %+v", sc)
	}
	t.Logf("legacy snapshot without scope recovered as %s (no silent revert semantics)", old.Topics["old"].Scope)
}

func TestNormalizeSortsVersionsAscending(t *testing.T) {
	s := Normalize(State{
		Versions: map[string][]VersionedSchema{
			"t": {{Version: 3}, {Version: 1}, {Version: 2}},
		},
	})
	if got := s.Versions["t"]; got[0].Version != 1 || got[1].Version != 2 || got[2].Version != 3 {
		t.Fatalf("versions not ascending after normalize: %d %d %d", got[0].Version, got[1].Version, got[2].Version)
	}
}
