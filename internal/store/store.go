// Package store provides local, crash-safe persistence for the registry state:
// schema versions per topic and consumer acknowledgement checkpoints.
//
// The whole registry state lives in one JSON document that is rewritten
// atomically (temp file + fsync + rename), so a failed write can never leave a
// partially updated state behind: readers always see either the old complete
// document or the new complete document.
package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

// stateFormatVersion is the on-disk document format marker.
const stateFormatVersion = 1

// VersionRecord is one registered schema version under a topic.
type VersionRecord struct {
	Version  int    `json:"version"`
	SchemaID string `json:"schema_id"` // content identity (canonical hash)
	Raw      string `json:"raw"`       // original schema document
}

// State is the complete persisted registry document.
type State struct {
	FormatVersion int                        `json:"format_version"`
	Topics        map[string][]VersionRecord `json:"topics"`
	Checkpoints   map[string]map[string]int  `json:"checkpoints"`
}

// Store is an in-memory snapshot backed by one atomic file.
type Store struct {
	mu     sync.Mutex
	path   string
	state  *State
	loaded bool
}

// ErrCorrupt indicates the state file cannot be read or decoded.
var ErrCorrupt = errors.New("corrupt registry state file")

// New opens (or creates) the store at path, loading any existing state.
// A missing file yields an empty store; an existing but undecodable file is an
// explicit error rather than a silent reset.
func New(_ context.Context, path string) (*Store, error) {
	if path == "" {
		return nil, errors.New("store: path must not be empty")
	}
	s := &Store{path: path, state: emptyState()}

	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			s.loaded = true
			return s, nil
		}
		return nil, fmt.Errorf("store: read %s: %w", path, err)
	}
	if len(bytes.TrimSpace(data)) == 0 {
		// An empty file is treated as a freshly initialized store.
		s.loaded = true
		return s, nil
	}
	var st State
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, fmt.Errorf("%w: %s: %v", ErrCorrupt, path, err)
	}
	if st.FormatVersion != stateFormatVersion {
		return nil, fmt.Errorf("%w: %s: unsupported format_version %d", ErrCorrupt, path, st.FormatVersion)
	}
	st.normalize()
	s.state = &st
	s.loaded = true
	return s, nil
}

func emptyState() *State {
	return &State{
		FormatVersion: stateFormatVersion,
		Topics:        map[string][]VersionRecord{},
		Checkpoints:   map[string]map[string]int{},
	}
}

// normalize fills missing maps and copies records into a well-defined shape.
func (st *State) normalize() {
	if st.Topics == nil {
		st.Topics = map[string][]VersionRecord{}
	}
	if st.Checkpoints == nil {
		st.Checkpoints = map[string]map[string]int{}
	}
}

// Clone returns an independent deep copy of the state.
func (st *State) Clone() *State {
	out := State{
		FormatVersion: st.FormatVersion,
		Topics:        make(map[string][]VersionRecord, len(st.Topics)),
		Checkpoints:   make(map[string]map[string]int, len(st.Checkpoints)),
	}
	for topic, versions := range st.Topics {
		cp := make([]VersionRecord, len(versions))
		copy(cp, versions)
		out.Topics[topic] = cp
	}
	for topic, consumers := range st.Checkpoints {
		cp := make(map[string]int, len(consumers))
		for c, v := range consumers {
			cp[c] = v
		}
		out.Checkpoints[topic] = cp
	}
	return &out
}

// Snapshot returns a deep copy of the current state.
func (s *Store) Snapshot() *State {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.Clone()
}

// Mutate runs fn under the store lock against a deep copy of the state. If fn
// succeeds the copy is atomically persisted and installed; if fn fails (or the
// write fails) on-disk and in-memory state are both left untouched.
func (s *Store) Mutate(_ context.Context, fn func(*State) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	candidate := s.state.Clone()
	candidate.FormatVersion = stateFormatVersion
	if err := fn(candidate); err != nil {
		return err
	}
	if err := persist(s.path, candidate); err != nil {
		return err
	}
	s.state = candidate
	return nil
}

// persist writes state atomically: encode -> temp file in same directory ->
// fsync file -> rename over target -> fsync directory.
func persist(path string, st *State) error {
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return fmt.Errorf("store: encode state: %w", err)
	}
	data = append(data, '\n')

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("store: create dir: %w", err)
	}

	tmp, err := os.CreateTemp(dir, ".registry-state-*.tmp")
	if err != nil {
		return fmt.Errorf("store: create temp file: %w", err)
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("store: write temp file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("store: fsync temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return fmt.Errorf("store: close temp file: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		cleanup()
		return fmt.Errorf("store: rename into place: %w", err)
	}
	// Best-effort fsync of the directory so the rename itself is durable.
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}
