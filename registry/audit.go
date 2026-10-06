package registry

import (
	"context"
	"fmt"
	"sort"

	"github.com/h3yow/schema-evolution-registry-go/compat"
	"github.com/h3yow/schema-evolution-registry-go/schema"
	"github.com/h3yow/schema-evolution-registry-go/storage"
)

// PrecheckOptions optionally overrides the topic's configured strategy and
// audit scope for a dry-run compatibility evaluation.
type PrecheckOptions struct {
	// Strategy, when empty, falls back to the topic's configured strategy.
	Strategy schema.Compatibility
	// Scope, when nil, falls back to the topic's configured audit scope.
	Scope *schema.AuditScope
}

// VersionVerdict is the outcome of checking the candidate against one
// historical baseline version.
type VersionVerdict struct {
	Version    int
	Compatible bool
	Changes    []compat.FieldChange
	Reasons    []string
}

// PrecheckReport is the full read-only dry-run result: one verdict per
// in-scope baseline version, ordered by version ascending.
type PrecheckReport struct {
	Topic      string
	Strategy   schema.Compatibility
	Scope      schema.AuditScope
	Compatible bool
	Verdicts   []VersionVerdict
}

// VersionPage is one stable page of a topic's version history.
type VersionPage struct {
	Topic    string
	Versions []storage.VersionedSchema
	// Total is the full history length, independent of Offset/Limit.
	Total  int
	Offset int
}

// scopeVersions selects the baseline versions a candidate must be checked
// against, oldest first so verdicts and rejection reasons are deterministic.
// versions must already be ordered by version ascending.
func scopeVersions(versions []storage.VersionedSchema, scope schema.AuditScope) []storage.VersionedSchema {
	scope = scope.Normalize()
	switch scope.Kind {
	case schema.ScopeAll:
		return versions
	case schema.ScopeRecent:
		if scope.N < len(versions) {
			return versions[len(versions)-scope.N:]
		}
		return versions
	default: // ScopeLatest
		if len(versions) > 1 {
			return versions[len(versions)-1:]
		}
		return versions
	}
}

// checkAgainst evaluates the candidate against each baseline in order and
// returns one verdict per baseline. A malformed candidate or an unusable
// strategy is an error, never a verdict.
func checkAgainst(ctx context.Context, checker compat.Checker, baselines []storage.VersionedSchema, candidate schema.Schema, strategy schema.Compatibility) ([]VersionVerdict, error) {
	verdicts := make([]VersionVerdict, 0, len(baselines))
	for _, base := range baselines {
		res, err := checker.Check(ctx, base.Schema, candidate, strategy)
		if err != nil {
			if compat.IsMalformed(err) {
				return nil, regErr(ErrMalformedSchema, err.Error())
			}
			return nil, regErr(ErrInvalidStrategy, err.Error())
		}
		verdicts = append(verdicts, VersionVerdict{
			Version:    base.Version,
			Compatible: res.Compatible,
			Changes:    res.Changes,
			Reasons:    res.Reasons,
		})
	}
	return verdicts, nil
}

// rejectionError builds an INCOMPATIBLE_SCHEMA error that names every
// blocking baseline version and the exact changes that triggered it.
func rejectionError(topic string, strategy schema.Compatibility, scope schema.AuditScope, verdicts []VersionVerdict) *Error {
	var failed []int
	var reasons []string
	for _, v := range verdicts {
		if v.Compatible {
			continue
		}
		failed = append(failed, v.Version)
		for _, reason := range v.Reasons {
			reasons = append(reasons, fmt.Sprintf("baseline v%d: %s", v.Version, reason))
		}
		for _, ch := range v.Changes {
			reasons = append(reasons, fmt.Sprintf("baseline v%d: observed: %s", v.Version, ch.String()))
		}
	}
	return regErr(ErrIncompatibleSchema,
		fmt.Sprintf("schema rejected by %s strategy of topic %q under scope %s: incompatible with baseline version(s) %v",
			strategy, topic, scope, failed),
		reasons...)
}

// SetAuditScope configures the compatibility audit range of a topic. The
// scope is persisted in the same atomic snapshot as everything else, so it
// survives restarts exactly as configured. A missing topic is created with
// the registry's default strategy.
func (r *localRegistry) SetAuditScope(ctx context.Context, topic string, scope schema.AuditScope) error {
	if topic == "" {
		return regErr(ErrTopicNotFound, "topic name must not be empty")
	}
	scope = scope.Normalize()
	if err := scope.Validate(); err != nil {
		return regErr(ErrInvalidScope, err.Error())
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	cfg := r.ensureTopicLocked(topic)
	if cfg.Scope.Normalize() == scope {
		return nil // idempotent
	}
	next := cloneState(r.state)
	cfg.Scope = scope
	next.Topics[topic] = cfg
	return r.persist(ctx, next)
}

// TopicConfig returns the effective configuration of a topic, with the audit
// scope normalized (a never-configured topic reports LATEST).
func (r *localRegistry) TopicConfig(_ context.Context, topic string) (storage.TopicConfig, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cfg, ok := r.state.Topics[topic]
	if !ok {
		return storage.TopicConfig{}, regErr(ErrTopicNotFound, fmt.Sprintf("topic %q does not exist", topic))
	}
	cfg.Scope = cfg.Scope.Normalize()
	return cfg, nil
}

// Precheck dry-runs a candidate schema against the in-scope history of a
// topic and reports one verdict per baseline version. It is strictly
// read-only: no version is published, no confirmation is touched and the
// snapshot on disk is never written. A malformed candidate is an explicit
// error, never a compatible report and never an empty result.
func (r *localRegistry) Precheck(ctx context.Context, topic string, candidate schema.Schema, opts PrecheckOptions) (PrecheckReport, error) {
	// Structural validation first: a malformed candidate can never be
	// mistaken for a compatible one.
	if err := r.checker.Validate(ctx, candidate); err != nil {
		return PrecheckReport{}, regErr(ErrMalformedSchema, err.Error())
	}

	r.mu.Lock()
	cfg, ok := r.state.Topics[topic]
	versions := append([]storage.VersionedSchema(nil), r.state.Versions[topic]...)
	r.mu.Unlock()
	if !ok {
		return PrecheckReport{}, regErr(ErrTopicNotFound, fmt.Sprintf("topic %q does not exist", topic))
	}

	strategy := opts.Strategy
	if strategy == "" {
		strategy = cfg.Compatibility
	}
	if !validStrategy(strategy) {
		return PrecheckReport{}, regErr(ErrInvalidStrategy, fmt.Sprintf("unknown strategy %q", strategy))
	}
	scope := cfg.Scope
	if opts.Scope != nil {
		scope = *opts.Scope
	}
	scope = scope.Normalize()
	if err := scope.Validate(); err != nil {
		return PrecheckReport{}, regErr(ErrInvalidScope, err.Error())
	}

	report := PrecheckReport{Topic: topic, Strategy: strategy, Scope: scope, Compatible: true}
	if strategy == schema.CompatNone || len(versions) == 0 {
		// NONE audits nothing; an empty history has no baselines to fail.
		return report, nil
	}

	verdicts, err := checkAgainst(ctx, r.checker, scopeVersions(versions, scope), candidate, strategy)
	if err != nil {
		return PrecheckReport{}, err
	}
	report.Verdicts = verdicts
	for _, v := range verdicts {
		if !v.Compatible {
			report.Compatible = false
		}
	}
	return report, nil
}

// ListVersions returns the topic's version history ordered by version
// ascending — an order that is stable across repeated calls. offset skips
// that many entries (negative is clamped to 0); limit <= 0 returns
// everything from offset on. Total always reflects the full history length
// so callers can page or resume deterministically.
func (r *localRegistry) ListVersions(_ context.Context, topic string, offset, limit int) (VersionPage, error) {
	r.mu.Lock()
	_, ok := r.state.Topics[topic]
	versions := append([]storage.VersionedSchema(nil), r.state.Versions[topic]...)
	r.mu.Unlock()
	if !ok {
		return VersionPage{}, regErr(ErrTopicNotFound, fmt.Sprintf("topic %q does not exist", topic))
	}
	sort.Slice(versions, func(i, j int) bool { return versions[i].Version < versions[j].Version })

	total := len(versions)
	if offset < 0 {
		offset = 0
	}
	if offset > total {
		offset = total
	}
	page := versions[offset:]
	if limit > 0 && limit < len(page) {
		page = page[:limit]
	}
	return VersionPage{Topic: topic, Versions: page, Total: total, Offset: offset}, nil
}
