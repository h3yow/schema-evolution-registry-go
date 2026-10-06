package registry

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/h3yow/schema-evolution-registry-go/compat"
	"github.com/h3yow/schema-evolution-registry-go/schema"
	"github.com/h3yow/schema-evolution-registry-go/storage"
)

// ErrorCode classifies registry errors so callers can distinguish
// conflicts, rejections and malformed input.
type ErrorCode string

const (
	ErrMalformedSchema     ErrorCode = "MALFORMED_SCHEMA"
	ErrInvalidStrategy     ErrorCode = "INVALID_STRATEGY"
	ErrVersionConflict     ErrorCode = "VERSION_CONFLICT"
	ErrVersionOutOfOrder   ErrorCode = "VERSION_OUT_OF_ORDER"
	ErrVersionNotFound     ErrorCode = "VERSION_NOT_FOUND"
	ErrIncompatibleSchema  ErrorCode = "INCOMPATIBLE_SCHEMA"
	ErrTopicNotFound       ErrorCode = "TOPIC_NOT_FOUND"
	ErrConsumerNotFound    ErrorCode = "CONSUMER_NOT_FOUND"
	ErrInvalidConfirmation ErrorCode = "INVALID_CONFIRMATION"
	ErrInvalidScope        ErrorCode = "INVALID_SCOPE"
	ErrPersistence         ErrorCode = "PERSISTENCE_FAILED"
)

// Error is a classified registry error.
type Error struct {
	Code    ErrorCode
	Message string
	Reasons []string
}

func (e *Error) Error() string {
	if len(e.Reasons) > 0 {
		return string(e.Code) + ": " + e.Message + " [" + strings.Join(e.Reasons, "; ") + "]"
	}
	return string(e.Code) + ": " + e.Message
}

// RegistrationResult is the deterministic outcome of a register call.
// Reused is true when the identical schema content already had a version,
// in which case Version identifies the pre-existing version and nothing new
// is published.
type RegistrationResult struct {
	Topic       string
	Version     int
	Reused      bool
	Fingerprint string
}

// ConsumerView describes a consumer relative to the topic frontier.
type ConsumerView struct {
	Consumer        string
	Topic           string
	Confirmed       int
	Latest          int
	UpToDate        bool
	PendingVersions []int
}

// Options configures a local registry.
type Options struct {
	Path            string
	DefaultStrategy schema.Compatibility
	Checker         compat.Checker
}

// Registry is the local mode schema registry.
type Registry interface {
	EnsureTopic(ctx context.Context, topic string, strategy schema.Compatibility) error
	Register(ctx context.Context, topic string, s schema.Schema) (RegistrationResult, error)
	RegisterVersion(ctx context.Context, topic string, version int, s schema.Schema) (RegistrationResult, error)
	GetVersion(ctx context.Context, topic string, version int) (storage.VersionedSchema, error)
	GetLatest(ctx context.Context, topic string) (storage.VersionedSchema, error)
	QueryByStrategy(ctx context.Context, topic string, strategy schema.Compatibility, candidate schema.Schema) (storage.VersionedSchema, error)
	Confirm(ctx context.Context, topic, consumer string, version int) error
	GetConfirmation(ctx context.Context, topic, consumer string) (int, error)
	ConsumersBehind(ctx context.Context, topic string) ([]ConsumerView, error)
	SetAuditScope(ctx context.Context, topic string, scope schema.AuditScope) error
	TopicConfig(ctx context.Context, topic string) (storage.TopicConfig, error)
	Precheck(ctx context.Context, topic string, candidate schema.Schema, opts PrecheckOptions) (PrecheckReport, error)
	ListVersions(ctx context.Context, topic string, offset, limit int) (VersionPage, error)
	Close() error
}

type localRegistry struct {
	mu              sync.Mutex
	store           storage.Store
	checker         compat.Checker
	defaultStrategy schema.Compatibility
	state           storage.State
}

// New opens (or creates) a local registry. When opts.Path is set state is
// persisted as an atomic JSON snapshot and recovered on restart.
func New(ctx context.Context, opts Options) (Registry, error) {
	strategy := opts.DefaultStrategy
	if strategy == "" {
		strategy = schema.CompatBackward
	}
	if !validStrategy(strategy) {
		return nil, &Error{Code: ErrInvalidStrategy, Message: fmt.Sprintf("unknown default strategy %q", strategy)}
	}
	checker := opts.Checker
	if checker == nil {
		checker = compat.NewChecker()
	}
	var store storage.Store
	if opts.Path != "" {
		store = storage.NewFileStore(opts.Path)
	}
	r := &localRegistry{
		store:           store,
		checker:         checker,
		defaultStrategy: strategy,
		state:           storage.Normalize(storage.State{}),
	}
	if store != nil {
		s, err := store.Load(ctx)
		if err != nil {
			return nil, &Error{Code: ErrPersistence, Message: "cannot recover registry state", Reasons: []string{err.Error()}}
		}
		r.state = s
	}
	return r, nil
}

func validStrategy(s schema.Compatibility) bool {
	switch s {
	case schema.CompatNone, schema.CompatBackward, schema.CompatForward, schema.CompatFull:
		return true
	}
	return false
}

// persist writes candidateState durably before it becomes visible. On error
// the previously loaded state stays in effect, so no partial update survives.
func (r *localRegistry) persist(ctx context.Context, next storage.State) error {
	if r.store == nil {
		r.state = next
		return nil
	}
	if err := r.store.Save(ctx, next); err != nil {
		return &Error{Code: ErrPersistence, Message: "failed to persist state update", Reasons: []string{err.Error()}}
	}
	r.state = next
	return nil
}

func cloneState(s storage.State) storage.State {
	c := storage.Normalize(storage.State{})
	for k, v := range s.Topics {
		c.Topics[k] = v
	}
	for k, v := range s.Versions {
		dup := make([]storage.VersionedSchema, len(v))
		copy(dup, v)
		c.Versions[k] = dup
	}
	for topic, consumers := range s.Confirmations {
		m := make(map[string]int, len(consumers))
		for consumer, ver := range consumers {
			m[consumer] = ver
		}
		c.Confirmations[topic] = m
	}
	return c
}

func regErr(code ErrorCode, msg string, reasons ...string) *Error {
	return &Error{Code: code, Message: msg, Reasons: reasons}
}

// ensureTopicLocked auto-creates a topic with the default strategy on first
// registration.
func (r *localRegistry) ensureTopicLocked(topic string) storage.TopicConfig {
	if cfg, ok := r.state.Topics[topic]; ok {
		return cfg
	}
	cfg := storage.TopicConfig{Topic: topic, Compatibility: r.defaultStrategy, NextVersion: 1}
	r.state.Topics[topic] = cfg
	return cfg
}

// EnsureTopic creates a topic with an explicit strategy. It is idempotent:
// an existing topic keeps its original strategy.
func (r *localRegistry) EnsureTopic(_ context.Context, topic string, strategy schema.Compatibility) error {
	if topic == "" {
		return regErr(ErrTopicNotFound, "topic name must not be empty")
	}
	if !validStrategy(strategy) {
		return regErr(ErrInvalidStrategy, fmt.Sprintf("unknown strategy %q", strategy))
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.state.Topics[topic]; ok {
		return nil
	}
	next := cloneState(r.state)
	next.Topics[topic] = storage.TopicConfig{Topic: topic, Compatibility: strategy, NextVersion: 1}
	return r.persist(context.Background(), next)
}

func (r *localRegistry) findByFingerprintLocked(topic, fp string) (storage.VersionedSchema, bool) {
	for _, v := range r.state.Versions[topic] {
		if v.Fingerprint == fp {
			return v, true
		}
	}
	return storage.VersionedSchema{}, false
}

// registerLocked validates and appends a new version. wantVersion <= 0 means
// "assign the next version automatically".
func (r *localRegistry) registerLocked(ctx context.Context, topic string, wantVersion int, s schema.Schema) (RegistrationResult, error) {
	cfg := r.ensureTopicLocked(topic)
	versions := r.state.Versions[topic]
	fp := s.Fingerprint()

	// 1. Deterministic duplicate handling: identical content reuses its version.
	if existing, ok := r.findByFingerprintLocked(topic, fp); ok {
		return RegistrationResult{Topic: topic, Version: existing.Version, Reused: true, Fingerprint: fp}, nil
	}

	// 2. Explicit-version callers can never occupy a used version differently.
	if wantVersion > 0 {
		if wantVersion < cfg.NextVersion {
			return RegistrationResult{}, regErr(ErrVersionConflict,
				fmt.Sprintf("topic %q version %d is already published with different content", topic, wantVersion))
		}
		if wantVersion > cfg.NextVersion {
			return RegistrationResult{}, regErr(ErrVersionOutOfOrder,
				fmt.Sprintf("topic %q expects version %d next, got %d: versions must be consecutive and monotonic", topic, cfg.NextVersion, wantVersion))
		}
	}

	// 3. Structural validation: malformed schemas are never silently accepted.
	if err := r.checker.Validate(ctx, s); err != nil {
		return RegistrationResult{}, regErr(ErrMalformedSchema, err.Error())
	}

	// 4. Compatibility verdict against every baseline in the topic's audit
	// scope. Any single in-scope failure rejects the whole registration and
	// the error names the blocking versions and triggering changes.
	if len(versions) > 0 && cfg.Compatibility != schema.CompatNone {
		scope := cfg.Scope.Normalize()
		verdicts, err := checkAgainst(ctx, r.checker, scopeVersions(versions, scope), s, cfg.Compatibility)
		if err != nil {
			return RegistrationResult{}, err
		}
		for _, v := range verdicts {
			if !v.Compatible {
				return RegistrationResult{}, rejectionError(topic, cfg.Compatibility, scope, verdicts)
			}
		}
	}

	// 5. Stage the whole new state, persist atomically, then publish it.
	assigned := cfg.NextVersion
	if wantVersion > 0 {
		assigned = wantVersion
	}
	next := cloneState(r.state)
	next.Topics[topic] = storage.TopicConfig{
		Topic:         topic,
		Compatibility: cfg.Compatibility,
		NextVersion:   assigned + 1,
	}
	next.Versions[topic] = append(append([]storage.VersionedSchema(nil), versions...), storage.VersionedSchema{
		Topic:       topic,
		Version:     assigned,
		Schema:      s,
		Fingerprint: fp,
	})
	if err := r.persist(ctx, next); err != nil {
		return RegistrationResult{}, err
	}
	return RegistrationResult{Topic: topic, Version: assigned, Reused: false, Fingerprint: fp}, nil
}

func (r *localRegistry) Register(ctx context.Context, topic string, s schema.Schema) (RegistrationResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.registerLocked(ctx, topic, 0, s)
}

func (r *localRegistry) RegisterVersion(ctx context.Context, topic string, version int, s schema.Schema) (RegistrationResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if version <= 0 {
		return RegistrationResult{}, regErr(ErrVersionOutOfOrder, fmt.Sprintf("version must be positive, got %d", version))
	}
	return r.registerLocked(ctx, topic, version, s)
}

func (r *localRegistry) GetVersion(_ context.Context, topic string, version int) (storage.VersionedSchema, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, v := range r.state.Versions[topic] {
		if v.Version == version {
			return v, nil
		}
	}
	if _, ok := r.state.Topics[topic]; !ok {
		return storage.VersionedSchema{}, regErr(ErrTopicNotFound, fmt.Sprintf("topic %q does not exist", topic))
	}
	return storage.VersionedSchema{}, regErr(ErrVersionNotFound, fmt.Sprintf("topic %q has no version %d", topic, version))
}

func (r *localRegistry) GetLatest(_ context.Context, topic string) (storage.VersionedSchema, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	versions := r.state.Versions[topic]
	if len(versions) == 0 {
		if _, ok := r.state.Topics[topic]; !ok {
			return storage.VersionedSchema{}, regErr(ErrTopicNotFound, fmt.Sprintf("topic %q does not exist", topic))
		}
		return storage.VersionedSchema{}, regErr(ErrVersionNotFound, fmt.Sprintf("topic %q has no published versions", topic))
	}
	return versions[len(versions)-1], nil
}

// QueryByStrategy returns the newest registered version against which the
// candidate schema passes the given strategy. The candidate is treated as a
// proposed new schema and each registered version as the existing baseline
// (the same orientation used during registration).
func (r *localRegistry) QueryByStrategy(ctx context.Context, topic string, strategy schema.Compatibility, candidate schema.Schema) (storage.VersionedSchema, error) {
	if !validStrategy(strategy) || strategy == schema.CompatNone {
		return storage.VersionedSchema{}, regErr(ErrInvalidStrategy, fmt.Sprintf("cannot query with strategy %q", strategy))
	}
	r.mu.Lock()
	versions := append([]storage.VersionedSchema(nil), r.state.Versions[topic]...)
	_, topicExists := r.state.Topics[topic]
	r.mu.Unlock()

	if !topicExists {
		return storage.VersionedSchema{}, regErr(ErrTopicNotFound, fmt.Sprintf("topic %q does not exist", topic))
	}
	if err := r.checker.Validate(ctx, candidate); err != nil {
		return storage.VersionedSchema{}, regErr(ErrMalformedSchema, err.Error())
	}
	for i := len(versions) - 1; i >= 0; i-- {
		v := versions[i]
		if strategy == schema.CompatNone {
			return v, nil
		}
		res, err := r.checker.Check(ctx, v.Schema, candidate, strategy)
		if err != nil {
			if compat.IsMalformed(err) {
				return storage.VersionedSchema{}, regErr(ErrMalformedSchema, err.Error())
			}
			return storage.VersionedSchema{}, regErr(ErrInvalidStrategy, err.Error())
		}
		if res.Compatible {
			return v, nil
		}
	}
	return storage.VersionedSchema{}, regErr(ErrVersionNotFound,
		fmt.Sprintf("no registered version of topic %q is compatible with the candidate under %s", topic, strategy))
}

// Confirm records a consumer's acknowledged version. Positions are monotonic:
// a confirmation can never move backwards, and the whole update is one atomic
// snapshot write.
func (r *localRegistry) Confirm(ctx context.Context, topic, consumer string, version int) error {
	if consumer == "" {
		return regErr(ErrInvalidConfirmation, "consumer name must not be empty")
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	cfg, ok := r.state.Topics[topic]
	if !ok {
		return regErr(ErrTopicNotFound, fmt.Sprintf("topic %q does not exist", topic))
	}
	if version < 1 || version >= cfg.NextVersion {
		return regErr(ErrVersionNotFound, fmt.Sprintf("topic %q has no published version %d (latest is %d)", topic, version, cfg.NextVersion-1))
	}
	if existing := r.state.Confirmations[topic][consumer]; version < existing {
		return regErr(ErrInvalidConfirmation,
			fmt.Sprintf("consumer %q already confirmed v%d, cannot roll back to v%d", consumer, existing, version))
	} else if version == existing {
		return nil // idempotent
	}

	next := cloneState(r.state)
	if next.Confirmations[topic] == nil {
		next.Confirmations[topic] = map[string]int{}
	}
	next.Confirmations[topic][consumer] = version
	return r.persist(ctx, next)
}

func (r *localRegistry) GetConfirmation(_ context.Context, topic, consumer string) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	conf, ok := r.state.Confirmations[topic]
	if !ok {
		if _, exists := r.state.Topics[topic]; !exists {
			return 0, regErr(ErrTopicNotFound, fmt.Sprintf("topic %q does not exist", topic))
		}
		return 0, regErr(ErrConsumerNotFound, fmt.Sprintf("consumer %q has no confirmation for topic %q", consumer, topic))
	}
	v, ok := conf[consumer]
	if !ok {
		return 0, regErr(ErrConsumerNotFound, fmt.Sprintf("consumer %q has no confirmation for topic %q", consumer, topic))
	}
	return v, nil
}

// ConsumersBehind returns the views of consumers whose confirmed version is
// older than the latest published version. The order is stable across calls:
// larger lag (latest - confirmed) first, ties broken by consumer name, so
// repeated queries over the same data are identical and safe to page over.
func (r *localRegistry) ConsumersBehind(_ context.Context, topic string) ([]ConsumerView, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cfg, ok := r.state.Topics[topic]
	if !ok {
		return nil, regErr(ErrTopicNotFound, fmt.Sprintf("topic %q does not exist", topic))
	}
	latest := cfg.NextVersion - 1
	if latest < 1 {
		return nil, regErr(ErrVersionNotFound, fmt.Sprintf("topic %q has no published versions", topic))
	}
	var views []ConsumerView
	for consumer, confirmed := range r.state.Confirmations[topic] {
		if confirmed >= latest {
			continue
		}
		view := ConsumerView{Consumer: consumer, Topic: topic, Confirmed: confirmed, Latest: latest, UpToDate: false}
		for v := confirmed + 1; v <= latest; v++ {
			view.PendingVersions = append(view.PendingVersions, v)
		}
		views = append(views, view)
	}
	sort.Slice(views, func(i, j int) bool {
		lagI, lagJ := views[i].Latest-views[i].Confirmed, views[j].Latest-views[j].Confirmed
		if lagI != lagJ {
			return lagI > lagJ
		}
		return views[i].Consumer < views[j].Consumer
	})
	return views, nil
}

func (r *localRegistry) Close() error { return nil }
