package schema

import "fmt"

// ScopeKind selects how many historical versions of a topic a new candidate
// schema is checked against during registration.
type ScopeKind string

const (
	// ScopeLatest checks only the latest published version (default).
	ScopeLatest ScopeKind = "LATEST"
	// ScopeRecent checks the most recent N published versions.
	ScopeRecent ScopeKind = "RECENT"
	// ScopeAll checks every published version.
	ScopeAll ScopeKind = "ALL"
)

// AuditScope is the per-topic compatibility audit range. The zero value
// (empty Kind) is normalized to ScopeLatest so snapshots written before
// scopes existed keep their original "latest only" behaviour.
type AuditScope struct {
	Kind ScopeKind `json:"kind"`
	// N is only meaningful for ScopeRecent and must be >= 1.
	N int `json:"n,omitempty"`
}

// DefaultScope is the implicit scope: latest version only.
func DefaultScope() AuditScope { return AuditScope{Kind: ScopeLatest} }

// Normalize maps the zero value to the default scope.
func (s AuditScope) Normalize() AuditScope {
	if s.Kind == "" {
		return DefaultScope()
	}
	return s
}

// Validate reports whether the scope is well formed.
func (s AuditScope) Validate() error {
	s = s.Normalize()
	switch s.Kind {
	case ScopeLatest, ScopeAll:
		return nil
	case ScopeRecent:
		if s.N < 1 {
			return fmt.Errorf("RECENT scope requires N >= 1, got %d", s.N)
		}
		return nil
	}
	return fmt.Errorf("unknown audit scope kind %q", s.Kind)
}

func (s AuditScope) String() string {
	s = s.Normalize()
	if s.Kind == ScopeRecent {
		return fmt.Sprintf("RECENT(%d)", s.N)
	}
	return string(s.Kind)
}
