package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

// fileStore persists the whole registry state as one JSON document.
// Writes go to a temp file in the same directory, are fsynced and then
// atomically renamed, so a failed write never leaves a torn snapshot and
// an in-memory update that cannot be persisted is discarded by the caller.
type fileStore struct {
	mu   sync.Mutex
	path string
}

// NewFileStore creates a JSON file backed store at path.
func NewFileStore(path string) Store {
	return &fileStore{path: path}
}

func Normalize(s State) State {
	if s.Topics == nil {
		s.Topics = map[string]TopicConfig{}
	}
	if s.Versions == nil {
		s.Versions = map[string][]VersionedSchema{}
	}
	if s.Confirmations == nil {
		s.Confirmations = map[string]map[string]int{}
	}
	return s
}

// Load reads the snapshot. A missing file yields an empty state; any other
// read or decode problem (including a corrupt document) is an error.
func (f *fileStore) Load(_ context.Context) (State, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	data, err := os.ReadFile(f.path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return Normalize(State{}), nil
		}
		return State{}, fmt.Errorf("load state: %w", err)
	}
	var s State
	if err := json.Unmarshal(data, &s); err != nil {
		return State{}, fmt.Errorf("decode state snapshot at %s: %w", f.path, err)
	}
	return Normalize(s), nil
}

// Save atomically replaces the snapshot.
func (f *fileStore) Save(_ context.Context, s State) (err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	s = Normalize(s)
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return fmt.Errorf("encode state: %w", err)
	}

	dir := filepath.Dir(f.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create state directory: %w", err)
	}

	tmp, err := os.CreateTemp(dir, ".registry-state-*.tmp")
	if err != nil {
		return fmt.Errorf("create temp state file: %w", err)
	}
	tmpName := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpName)
		}
	}()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write temp state file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("fsync temp state file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp state file: %w", err)
	}
	if err := os.Rename(tmpName, f.path); err != nil {
		return fmt.Errorf("rename temp state file: %w", err)
	}
	cleanup = false

	// Best effort directory fsync so the rename itself is durable.
	if d, derr := os.Open(dir); derr == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}
