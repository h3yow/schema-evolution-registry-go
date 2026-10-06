package storage

import (
	"context"
	"fmt"

	"github.com/h3yow/schema-evolution-registry-go/schema"
)

// VersionedSchema is one published schema version of a topic.
type VersionedSchema struct {
	Topic       string        `json:"topic"`
	Version     int           `json:"version"`
	Schema      schema.Schema `json:"schema"`
	Fingerprint string        `json:"fingerprint"`
}

// ReviewMode selects how many historical versions a new candidate is
// evaluated against during registration.
type ReviewMode string

const (
	// ScopeLatest only reviews the newest published version (the original
	// behaviour and the default).
	ScopeLatest ReviewMode = "LATEST"
	// ScopeRecent reviews the most recent Scope.Limit versions.
	ScopeRecent ReviewMode = "RECENT"
	// ScopeAll reviews every published version of the topic.
	ScopeAll ReviewMode = "ALL"
)

// Scope is the per-topic compatibility review range.
type Scope struct {
	Mode  ReviewMode `json:"mode"`
	Limit int        `json:"limit,omitempty"` // only meaningful for ScopeRecent
}

// LatestScope returns the default review range (newest version only).
func LatestScope() Scope { return Scope{Mode: ScopeLatest} }

// RecentScope reviews up to n most recent versions.
func RecentScope(n int) Scope { return Scope{Mode: ScopeRecent, Limit: n} }

// AllScope reviews the full version history.
func AllScope() Scope { return Scope{Mode: ScopeAll} }

// WithDefaults fills the zero value with the default scope. Snaptons written
// before the scope existed decode to an empty Scope and must keep meaning
// LATEST rather than silently becoming some other range.
func (sc Scope) WithDefaults() Scope {
	if sc.Mode == "" {
		return Scope{Mode: ScopeLatest}
	}
	return sc
}

// Valid reports whether the scope is usable.
func (sc Scope) Valid() bool {
	switch sc.Mode {
	case ScopeLatest, ScopeAll:
		return true
	case ScopeRecent:
		return sc.Limit > 0
	default:
		return false
	}
}

func (sc Scope) String() string {
	if sc.Mode == ScopeRecent {
		return fmt.Sprintf("RECENT(%d)", sc.Limit)
	}
	return string(sc.Mode)
}

// TopicConfig is the per-topic configuration.
type TopicConfig struct {
	Topic         string               `json:"topic"`
	Compatibility schema.Compatibility `json:"compatibility"`
	NextVersion   int                  `json:"nextVersion"`
	Scope         Scope                `json:"scope"`
}

// State is the whole snapshot persisted to disk.
type State struct {
	Topics        map[string]TopicConfig       `json:"topics"`
	Versions      map[string][]VersionedSchema `json:"versions"`
	Confirmations map[string]map[string]int    `json:"confirmations"`
}

// Store persists the registry state atomically.
type Store interface {
	Load(ctx context.Context) (State, error)
	Save(ctx context.Context, state State) error
}
