package storage

import (
	"context"

	"github.com/h3yow/schema-evolution-registry-go/schema"
)

// VersionedSchema is one published schema version of a topic.
type VersionedSchema struct {
	Topic       string        `json:"topic"`
	Version     int           `json:"version"`
	Schema      schema.Schema `json:"schema"`
	Fingerprint string        `json:"fingerprint"`
}

// TopicConfig is the per-topic configuration.
type TopicConfig struct {
	Topic         string               `json:"topic"`
	Compatibility schema.Compatibility `json:"compatibility"`
	NextVersion   int                  `json:"nextVersion"`
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
