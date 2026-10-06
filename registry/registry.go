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
	ErrInvalidScope        ErrorCode = "INVALID_SCOPE"
	ErrVersionConflict     ErrorCode = "VERSION_CONFLICT"
	ErrVersionOutOfOrder   ErrorCode = "VERSION_OUT_OF_ORDER"
	ErrVersionNotFound     ErrorCode = "VERSION_NOT_FOUND"
	ErrIncompatibleSchema  ErrorCode = "INCOMPATIBLE_SCHEMA"
	ErrTopicNotFound       ErrorCode = "TOPIC_NOT_FOUND"
	ErrConsumerNotFound    ErrorCode = "CONSUMER_NOT_FOUND"
	ErrInvalidConfirmation ErrorCode = "INVALID_CONFIRMATION"
	ErrInvalidPage         ErrorCode = "INVALID_PAGE"
	ErrPersistence         ErrorCode = "PERSISTENCE_FAILED"
)

// Error is a classified registry error.
type Error struct {
	Code    ErrorCode
	Message string
	Reasons []string
	// Blocking carries the per-version findings that caused an INCOMPATIBLE_SCHEMA
	// rejection under a multi-version review scope.
	Blocking []VersionVerdict
}

// VersionVerdict is the judgement of a candidate against one historical
// version. When Compatible is false, Changes and Reasons explain exactly what
// triggered the rejection against that version.
type VersionVerdict struct {
	Version    int                  `json:"version"`
	Strategy   schema.Compatibility `json:"strategy"`
	Compatible bool                 `json:"compatible"`
	Changes    []compat.FieldChange `json:"changes,omitempty"`
	Reasons    []string             `json:"reasons,omitempty"`
}

// TopicInfo is the read-only view of a topic's configuration.
type TopicInfo struct {
	Topic       string
	Strategy    schema.Compatibility
	Scope       storage.Scope
	NextVersion int
	Latest      int
}

// VersionPage is one stable, bounded slice of a topic's version history.
// Versions are always ordered ascending by version number. NextCursor is 0
// when there are no further versions; otherwise it is passed back verbatim
// into the next ListVersions call.
type VersionPage struct {
	Topic      string
	Versions   []storage.VersionedSchema
	Limit      int
	Cursor     int // first version number this page starts after (0 on the first page)
	NextCursor int // cursor for the following page, 0 when exhausted
}

// PreflightOptions optionally overrides the topic's strategy and scope for a
// single read-only dry run. Zero values mean "use the topic configuration".
type PreflightOptions struct {
	Strategy schema.Compatibility
	Scope    storage.Scope
}

// PreflightReport is the full result of a register dry run. It never mutates
// registry state. BlockingVersion is the first (oldest-in-scope) version that
// rejects the candidate, 0 when every reviewed version passes.
type PreflightReport struct {
	Topic           string
	Strategy        schema.Compatibility
	Scope           storage.Scope
	Compatible      bool
	Verdicts        []VersionVerdict // ascending version order
	CheckedVersions []int            // ascending version order
	BlockingVersion int
	WouldRegister   bool // true when an actual Register would publish a new version
	ReusedVersion   int  // version reused by fingerprint, 0 otherwise
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
	GetTopic(ctx context.Context, topic string) (TopicInfo, error)
	ConfigureScope(ctx context.Context, topic string, scope storage.Scope) error
	Register(ctx context.Context, topic string, s schema.Schema) (RegistrationResult, error)
	RegisterVersion(ctx context.Context, topic string, version int, s schema.Schema) (RegistrationResult, error)
	GetVersion(ctx context.Context, topic string, version int) (storage.VersionedSchema, error)
	GetLatest(ctx context.Context, topic string) (storage.VersionedSchema, error)
	ListVersions(ctx context.Context, topic string, cursor, limit int) (VersionPage, error)
	Preflight(ctx context.Context, topic string, candidate schema.Schema, opts PreflightOptions) (PreflightReport, error)
	QueryByStrategy(ctx context.Context, topic string, strategy schema.Compatibility, candidate schema.Schema) (storage.VersionedSchema, error)
	Confirm(ctx context.Context, topic, consumer string, version int) error
	GetConfirmation(ctx context.Context, topic, consumer string) (int, error)
	ConsumersBehind(ctx context.Context, topic string) ([]ConsumerView, error)
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
	cfg := storage.TopicConfig{Topic: topic, Compatibility: r.defaultStrategy, NextVersion: 1, Scope: storage.LatestScope()}
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
	next.Topics[topic] = storage.TopicConfig{
		Topic:         topic,
		Compatibility: strategy,
		NextVersion:   1,
		Scope:         storage.LatestScope(),
	}
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

// versionsInScope returns the historical baselines a candidate must be checked
// against, in ascending version order. Versions are stored ascending, so each
// returned slice is a stable prefix/suffix/window.
func versionsInScope(versions []storage.VersionedSchema, sc storage.Scope) []storage.VersionedSchema {
	switch sc.Mode {
	case storage.ScopeAll:
		return versions
	case storage.ScopeRecent:
		start := len(versions) - sc.Limit
		if start < 0 {
			start = 0
		}
		return versions[start:]
	default: // ScopeLatest (also the zero value)
		if len(versions) == 0 {
			return nil
		}
		return versions[len(versions)-1:]
	}
}

// checkScopeLocked evaluates candidate against every baseline in ascending
// version order and returns one verdict per baseline. The first returned
// verdict (if any) that is incompatible is the oldest blocking version.
func (r *localRegistry) checkScopeLocked(ctx context.Context, baselines []storage.VersionedSchema, candidate schema.Schema, strategy schema.Compatibility) ([]VersionVerdict, error) {
	verdicts := make([]VersionVerdict, 0, len(baselines))
	for _, base := range baselines {
		res, err := r.checker.Check(ctx, base.Schema, candidate, strategy)
		if err != nil {
			return nil, err
		}
		verdicts = append(verdicts, VersionVerdict{
			Version:    base.Version,
			Strategy:   strategy,
			Compatible: res.Compatible,
			Changes:    res.Changes,
			Reasons:    res.Reasons,
		})
	}
	return verdicts, nil
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

	// 4. Compatibility verdicts against every historical version in scope.
	//    One rejection blocks the whole registration; every rejected baseline
	//    is reported so the caller sees exactly which versions break.
	if len(versions) > 0 && cfg.Compatibility != schema.CompatNone {
		baselines := versionsInScope(versions, cfg.Scope)
		verdicts, err := r.checkScopeLocked(ctx, baselines, s, cfg.Compatibility)
		if err != nil {
			if compat.IsMalformed(err) {
				return RegistrationResult{}, regErr(ErrMalformedSchema, err.Error())
			}
			return RegistrationResult{}, regErr(ErrInvalidStrategy, err.Error())
		}
		var blocking []VersionVerdict
		reasons := make([]string, 0)
		firstBlocked := 0
		for _, vd := range verdicts {
			if vd.Compatible {
				continue
			}
			blocking = append(blocking, vd)
			if firstBlocked == 0 {
				firstBlocked = vd.Version
			}
			for _, reason := range vd.Reasons {
				reasons = append(reasons, fmt.Sprintf("v%d: %s", vd.Version, reason))
			}
			for _, ch := range vd.Changes {
				reasons = append(reasons, fmt.Sprintf("v%d observed: %s", vd.Version, ch.String()))
			}
		}
		if len(blocking) > 0 {
			return RegistrationResult{}, &Error{
				Code:     ErrIncompatibleSchema,
				Message:  fmt.Sprintf("schema rejected by %s strategy of topic %q under review scope %s; first blocked at v%d", cfg.Compatibility, topic, cfg.Scope, firstBlocked),
				Reasons:  reasons,
				Blocking: blocking,
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
		Scope:         cfg.Scope,
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
// older than the latest published version, oldest lag first.
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
	// Deterministic ordering: furthest behind first; ties break by consumer
	// name so repeated queries against unchanged data return the exact same
	// order, which makes keyset pagination and resumption safe.
	sort.Slice(views, func(i, j int) bool {
		if views[i].Confirmed != views[j].Confirmed {
			return views[i].Confirmed < views[j].Confirmed
		}
		return views[i].Consumer < views[j].Consumer
	})
	return views, nil
}

func (r *localRegistry) Close() error { return nil }

// GetTopic returns the read-only configuration view of a topic.
func (r *localRegistry) GetTopic(_ context.Context, topic string) (TopicInfo, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cfg, ok := r.state.Topics[topic]
	if !ok {
		return TopicInfo{}, regErr(ErrTopicNotFound, fmt.Sprintf("topic %q does not exist", topic))
	}
	return TopicInfo{
		Topic:       cfg.Topic,
		Strategy:    cfg.Compatibility,
		Scope:       cfg.Scope,
		NextVersion: cfg.NextVersion,
		Latest:      cfg.NextVersion - 1,
	}, nil
}

// ConfigureScope sets the per-topic compatibility review range. The choice is
// persisted in the same snapshot as the topic config, so a restart recovers
// exactly the configured range instead of reverting to the default.
func (r *localRegistry) ConfigureScope(ctx context.Context, topic string, scope storage.Scope) error {
	scope = scope.WithDefaults()
	if !scope.Valid() {
		return regErr(ErrInvalidScope,
			fmt.Sprintf("invalid review scope %q: RECENT requires a positive limit", scope.Mode))
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	cfg, ok := r.state.Topics[topic]
	if !ok {
		return regErr(ErrTopicNotFound, fmt.Sprintf("topic %q does not exist", topic))
	}
	next := cloneState(r.state)
	cfg.Scope = scope
	next.Topics[topic] = cfg
	return r.persist(ctx, next)
}

// Default and maximum page sizes for version history listing.
const (
	defaultVersionPageLimit = 20
	maxVersionPageLimit     = 200
)

// ListVersions returns an ascending, version-number ordered page of the topic's
// history. cursor is the version number to start strictly after (0 for the
// first page); limit<=0 means the default size and values above the cap are
// clamped. The same data always yields identical page order, and the returned
// NextCursor resumes the following page exactly.
func (r *localRegistry) ListVersions(_ context.Context, topic string, cursor, limit int) (VersionPage, error) {
	if cursor < 0 {
		return VersionPage{}, regErr(ErrInvalidPage, fmt.Sprintf("cursor must not be negative, got %d", cursor))
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	versions := r.state.Versions[topic]
	if _, ok := r.state.Topics[topic]; !ok {
		return VersionPage{}, regErr(ErrTopicNotFound, fmt.Sprintf("topic %q does not exist", topic))
	}

	pageLimit := limit
	if pageLimit <= 0 {
		pageLimit = defaultVersionPageLimit
	}
	if pageLimit > maxVersionPageLimit {
		pageLimit = maxVersionPageLimit
	}

	start := 0
	for start < len(versions) && versions[start].Version <= cursor {
		start++
	}
	end := start + pageLimit
	if end > len(versions) {
		end = len(versions)
	}
	pageVersions := append([]storage.VersionedSchema(nil), versions[start:end]...)

	page := VersionPage{Topic: topic, Versions: pageVersions, Limit: pageLimit, Cursor: cursor}
	if end < len(versions) {
		page.NextCursor = pageVersions[len(pageVersions)-1].Version
	}
	return page, nil
}

// Preflight is the read-only register dry run. It never publishes a version
// and never touches consumer confirmations: state is only read. It evaluates
// the candidate against every version in the (optionally overridden) scope and
// reports the verdict and reasons for each baseline, so callers can see before
// committing whether the registration would pass and which version blocks it.
// A malformed candidate is an error, never a compatible verdict or an empty
// report.
func (r *localRegistry) Preflight(ctx context.Context, topic string, candidate schema.Schema, opts PreflightOptions) (PreflightReport, error) {
	r.mu.Lock()
	cfg, topicExists := r.state.Topics[topic]
	versions := append([]storage.VersionedSchema(nil), r.state.Versions[topic]...)
	r.mu.Unlock()

	if !topicExists {
		return PreflightReport{}, regErr(ErrTopicNotFound, fmt.Sprintf("topic %q does not exist", topic))
	}

	// Resolve and validate the effective configuration so bad overrides fail
	// fast with a classified error.
	strategy := opts.Strategy
	if strategy == "" {
		strategy = cfg.Compatibility
	}
	if !validStrategy(strategy) {
		return PreflightReport{}, regErr(ErrInvalidStrategy, fmt.Sprintf("unknown strategy %q", strategy))
	}
	scope := cfg.Scope.WithDefaults()
	if opts.Scope.Mode != "" {
		scope = opts.Scope.WithDefaults()
	}
	if !scope.Valid() {
		return PreflightReport{}, regErr(ErrInvalidScope,
			fmt.Sprintf("invalid review scope %q: RECENT requires a positive limit", scope.Mode))
	}

	// Structural validation first: a malformed candidate can never pass.
	if err := r.checker.Validate(ctx, candidate); err != nil {
		return PreflightReport{}, regErr(ErrMalformedSchema, err.Error())
	}

	report := PreflightReport{
		Topic:         topic,
		Strategy:      strategy,
		Scope:         scope,
		Compatible:    true,
		WouldRegister: true,
	}

	// Identical content already published: a real Register would reuse it and
	// publish nothing new.
	if existing, ok := findByFingerprint(versions, candidate.Fingerprint()); ok {
		report.WouldRegister = false
		report.ReusedVersion = existing.Version
		return report, nil
	}

	if strategy == schema.CompatNone || len(versions) == 0 {
		// Nothing to compare against; a valid candidate registers directly.
		return report, nil
	}

	baselines := versionsInScope(versions, scope)
	verdicts, err := r.checkScopeLocked(ctx, baselines, candidate, strategy)
	if err != nil {
		if compat.IsMalformed(err) {
			return PreflightReport{}, regErr(ErrMalformedSchema, err.Error())
		}
		return PreflightReport{}, regErr(ErrInvalidStrategy, err.Error())
	}
	report.Verdicts = verdicts
	report.CheckedVersions = make([]int, 0, len(verdicts))
	for _, vd := range verdicts {
		report.CheckedVersions = append(report.CheckedVersions, vd.Version)
		if !vd.Compatible && report.BlockingVersion == 0 {
			report.BlockingVersion = vd.Version
		}
	}
	report.Compatible = report.BlockingVersion == 0
	return report, nil
}

func findByFingerprint(versions []storage.VersionedSchema, fp string) (storage.VersionedSchema, bool) {
	for _, v := range versions {
		if v.Fingerprint == fp {
			return v, true
		}
	}
	return storage.VersionedSchema{}, false
}
