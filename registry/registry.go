// Package registry is the public facade of the local schema evolution
// registry: version registration with monotonic version numbers,
// strategy-based compatibility judgement and durable per-consumer
// acknowledgement checkpoints.
//
// All state transitions are serialized through the internal store and committed
// atomically, so concurrent registrations are deterministic (exactly one
// winner per version), failed writes leave no partial state, and a process
// restart restores versions and checkpoints byte-for-byte.
package registry

import (
	"context"
	"fmt"
	"sort"

	"github.com/h3yow/schema-evolution-registry-go/internal/compat"
	"github.com/h3yow/schema-evolution-registry-go/internal/schemadef"
	"github.com/h3yow/schema-evolution-registry-go/internal/store"
)

// defaultStrategy is enforced when WithStrategy is not supplied.
const defaultStrategy = compat.StrategyBackward

// Option configures a Registry.
type Option func(*Registry)

// WithStrategy sets the evolution strategy enforced on registration.
func WithStrategy(s compat.Strategy) Option {
	return func(r *Registry) {
		if s.Valid() {
			r.strategy = s
		}
	}
}

// RegisteredVersion describes one version returned by queries.
type RegisteredVersion struct {
	Version  int
	SchemaID string
	Schema   *schemadef.Schema
}

// Lag describes one consumer that has not caught up with the latest version.
// Consumers that never acknowledged anything are reported at version 0.
type Lag struct {
	Consumer      string
	AckedVersion  int
	LatestVersion int
}

// Registry coordinates parsing, compatibility checking and persistence.
type Registry struct {
	store    *store.Store
	strategy compat.Strategy
}

// Open loads or creates a registry backed by file path.
func Open(ctx context.Context, path string, opts ...Option) (*Registry, error) {
	st, err := store.New(ctx, path)
	if err != nil {
		return nil, &Error{Class: ClassStorage, Op: "Open", Reason: err.Error()}
	}
	r := &Registry{store: st, strategy: defaultStrategy}
	for _, opt := range opts {
		opt(r)
	}
	return r, nil
}

func regErr(class ErrorClass, op, reason string) *Error {
	return &Error{Class: class, Op: op, Reason: reason}
}

// parseSchema parses raw and maps parse failures to ClassMalformed.
func parseSchema(op string, raw []byte) (*schemadef.Schema, string, error) {
	sc, err := schemadef.Parse(context.Background(), raw)
	if err != nil {
		return nil, "", regErr(ClassMalformed, op, err.Error())
	}
	id, err := sc.ID()
	if err != nil {
		return nil, "", regErr(ClassStorage, op, "cannot compute schema id: "+err.Error())
	}
	return sc, id, nil
}

// Register assigns the next monotonic version to raw under topic, enforcing the
// configured evolution strategy against the previous version. Registering the
// exact same content again is idempotent and returns the already-assigned
// version without any write.
//
// Returns (version, alreadyExisted, err).
func (r *Registry) Register(ctx context.Context, topic string, raw []byte) (int, bool, error) {
	const op = "Register"
	sc, id, err := parseSchema(op, raw)
	if err != nil {
		return 0, false, err
	}

	var assigned int
	var existed bool
	err = r.store.Mutate(ctx, func(st *store.State) error {
		versions := st.Topics[topic]

		// Idempotent path: identical content may already occupy some version.
		for _, v := range versions {
			if v.SchemaID == id {
				assigned = v.Version
				existed = true
				return nil
			}
		}

		// Compatibility against the current latest version.
		if len(versions) > 0 {
			prevRec := versions[len(versions)-1]
			prev, perr := schemadef.Parse(ctx, []byte(prevRec.Raw))
			if perr != nil {
				return regErr(ClassStorage, op, "stored schema v"+fmt.Sprintf("%d", prevRec.Version)+" is unparseable: "+perr.Error())
			}
			rep, cerr := compat.Check(ctx, r.strategy, prev, sc)
			if cerr != nil {
				return regErr(ClassStorage, op, cerr.Error())
			}
			if !rep.Compatible {
				return &Error{Class: ClassIncompatible, Op: op, Reason: summarizeViolations(rep), Report: rep}
			}
		}

		next := 1
		if len(versions) > 0 {
			next = versions[len(versions)-1].Version + 1
		}
		st.Topics[topic] = append(versions, store.VersionRecord{
			Version:  next,
			SchemaID: id,
			Raw:      string(raw),
		})
		assigned = next
		existed = false
		return nil
	})
	if err != nil {
		return 0, false, mapStoreErr(op, err)
	}
	return assigned, existed, nil
}

// RegisterVersion registers raw at an explicit version number. Version numbers
// must be exactly one greater than the topic's current latest version. The
// same content at the same version is idempotent; different content occupying
// the same version, or a non-monotonic version, is rejected with a classified
// error. Compatibility is enforced exactly as in Register.
func (r *Registry) RegisterVersion(ctx context.Context, topic string, version int, raw []byte) error {
	const op = "RegisterVersion"
	sc, id, err := parseSchema(op, raw)
	if err != nil {
		return err
	}

	return mapStoreErr(op, r.store.Mutate(ctx, func(st *store.State) error {
		versions := st.Topics[topic]
		latest := 0
		if len(versions) > 0 {
			latest = versions[len(versions)-1].Version
		}

		// Targeting an already existing version: idempotent or hard conflict.
		if version <= latest {
			for _, v := range versions {
				if v.Version == version {
					if v.SchemaID == id {
						return nil // idempotent replay
					}
					return regErr(ClassVersionExists, op,
						fmt.Sprintf("topic %q version %d is already occupied by schema %s; cannot overwrite with %s", topic, version, v.SchemaID, id))
				}
			}
			// Version below latest but not present means a gap; monotonic
			// append is the only allowed shape.
			return regErr(ClassVersionOrder, op,
				fmt.Sprintf("topic %q version %d is below latest %d and not registered; versions must be appended monotonically", topic, version, latest))
		}
		if version != latest+1 {
			return regErr(ClassVersionOrder, op,
				fmt.Sprintf("topic %q next version must be %d, got %d", topic, latest+1, version))
		}

		if len(versions) > 0 {
			prevRec := versions[len(versions)-1]
			prev, perr := schemadef.Parse(ctx, []byte(prevRec.Raw))
			if perr != nil {
				return regErr(ClassStorage, op, "stored schema v"+fmt.Sprintf("%d", prevRec.Version)+" is unparseable: "+perr.Error())
			}
			rep, cerr := compat.Check(ctx, r.strategy, prev, sc)
			if cerr != nil {
				return regErr(ClassStorage, op, cerr.Error())
			}
			if !rep.Compatible {
				return &Error{Class: ClassIncompatible, Op: op, Reason: summarizeViolations(rep), Report: rep}
			}
		}

		st.Topics[topic] = append(versions, store.VersionRecord{
			Version:  version,
			SchemaID: id,
			Raw:      string(raw),
		})
		return nil
	}))
}

// GetVersion returns the schema registered at version under topic.
func (r *Registry) GetVersion(ctx context.Context, topic string, version int) (*RegisteredVersion, error) {
	const op = "GetVersion"
	st := r.store.Snapshot()
	for _, v := range st.Topics[topic] {
		if v.Version == version {
			sc, _, err := parseSchema(op, []byte(v.Raw))
			if err != nil {
				return nil, regErr(ClassStorage, op, "stored schema v"+fmt.Sprintf("%d", version)+" is unparseable: "+err.Error())
			}
			return &RegisteredVersion{Version: v.Version, SchemaID: v.SchemaID, Schema: sc}, nil
		}
	}
	if _, ok := st.Topics[topic]; !ok {
		return nil, regErr(ClassUnknownTopic, op, fmt.Sprintf("no schemas registered for topic %q", topic))
	}
	return nil, regErr(ClassUnknownVersion, op, fmt.Sprintf("topic %q has no version %d", topic, version))
}

// Latest returns the highest registered version under topic.
func (r *Registry) Latest(ctx context.Context, topic string) (*RegisteredVersion, error) {
	const op = "Latest"
	st := r.store.Snapshot()
	versions := st.Topics[topic]
	if len(versions) == 0 {
		return nil, regErr(ClassUnknownTopic, op, fmt.Sprintf("no schemas registered for topic %q", topic))
	}
	return r.GetVersion(ctx, topic, versions[len(versions)-1].Version)
}

// Versions returns all versions under topic in ascending order.
func (r *Registry) Versions(_ context.Context, topic string) ([]RegisteredVersion, error) {
	const op = "Versions"
	st := r.store.Snapshot()
	versions, ok := st.Topics[topic]
	if !ok {
		return nil, regErr(ClassUnknownTopic, op, fmt.Sprintf("no schemas registered for topic %q", topic))
	}
	out := make([]RegisteredVersion, 0, len(versions))
	for _, v := range versions {
		sc, _, err := parseSchema(op, []byte(v.Raw))
		if err != nil {
			return nil, regErr(ClassStorage, op, "stored schema v"+fmt.Sprintf("%d", v.Version)+" is unparseable: "+err.Error())
		}
		out = append(out, RegisteredVersion{Version: v.Version, SchemaID: v.SchemaID, Schema: sc})
	}
	return out, nil
}

// CheckCompatibility reports how raw would evolve from the topic's latest
// version under strategy, with explainable findings. On an unknown topic the
// report is trivially compatible (nothing to evolve from).
func (r *Registry) CheckCompatibility(ctx context.Context, topic string, strategy compat.Strategy, raw []byte) (*compat.Report, error) {
	const op = "CheckCompatibility"
	if !strategy.Valid() {
		return nil, regErr(ClassStorage, op, fmt.Sprintf("unknown strategy %q", strategy))
	}
	sc, _, err := parseSchema(op, raw)
	if err != nil {
		return nil, err
	}
	st := r.store.Snapshot()
	versions := st.Topics[topic]
	if len(versions) == 0 {
		return &compat.Report{Compatible: true, Strategy: strategy, Findings: nil, Violations: nil}, nil
	}
	prevRec := versions[len(versions)-1]
	prev, perr := schemadef.Parse(ctx, []byte(prevRec.Raw))
	if perr != nil {
		return nil, regErr(ClassStorage, op, "stored schema v"+fmt.Sprintf("%d", prevRec.Version)+" is unparseable: "+perr.Error())
	}
	rep, cerr := compat.Check(ctx, strategy, prev, sc)
	if cerr != nil {
		return nil, regErr(ClassStorage, op, cerr.Error())
	}
	return rep, nil
}

// Ack records that consumer has acknowledged version under topic. The
// checkpoint is durable and monotonic: acking an older version than the current
// checkpoint is rejected, and the target version must already be registered.
func (r *Registry) Ack(ctx context.Context, topic, consumer string, version int) error {
	const op = "Ack"
	if consumer == "" {
		return regErr(ClassCheckpoint, op, "consumer must not be empty")
	}
	if version <= 0 {
		return regErr(ClassCheckpoint, op, fmt.Sprintf("ack version must be positive, got %d", version))
	}

	return mapStoreErr(op, r.store.Mutate(ctx, func(st *store.State) error {
		versions := st.Topics[topic]
		found := false
		for _, v := range versions {
			if v.Version == version {
				found = true
				break
			}
		}
		if !found {
			if len(versions) == 0 {
				return regErr(ClassUnknownTopic, op, fmt.Sprintf("no schemas registered for topic %q", topic))
			}
			return regErr(ClassUnknownVersion, op, fmt.Sprintf("topic %q has no version %d to acknowledge", topic, version))
		}

		consumers := st.Checkpoints[topic]
		if consumers == nil {
			consumers = map[string]int{}
			st.Checkpoints[topic] = consumers
		}
		if cur := consumers[consumer]; cur > version {
			return regErr(ClassCheckpoint, op,
				fmt.Sprintf("consumer %q checkpoint cannot move backwards: current=%d requested=%d", consumer, cur, version))
		}
		consumers[consumer] = version
		return nil
	}))
}

// Checkpoint returns the acknowledged version for consumer (0 when never
// acknowledged) and the topic's latest version (0 when the topic is unknown).
func (r *Registry) Checkpoint(_ context.Context, topic, consumer string) (acked, latest int, err error) {
	st := r.store.Snapshot()
	if versions, ok := st.Topics[topic]; ok && len(versions) > 0 {
		latest = versions[len(versions)-1].Version
	}
	acked = st.Checkpoints[topic][consumer]
	return acked, latest, nil
}

// Lagging returns consumers whose acked version is below the topic's latest
// version, including consumers that never acknowledged (at version 0).
// Consumers are known from both checkpoints and the provided interaction set
// stored under checkpoints; topics without schemas return an empty list.
func (r *Registry) Lagging(_ context.Context, topic string) ([]Lag, error) {
	st := r.store.Snapshot()
	versions := st.Topics[topic]
	if len(versions) == 0 {
		return nil, nil
	}
	latest := versions[len(versions)-1].Version
	consumers := st.Checkpoints[topic]
	names := make([]string, 0, len(consumers))
	for name := range consumers {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]Lag, 0)
	for _, name := range names {
		acked := consumers[name]
		if acked < latest {
			out = append(out, Lag{Consumer: name, AckedVersion: acked, LatestVersion: latest})
		}
	}
	return out, nil
}

// mapStoreErr passes *Error rejections through untouched and wraps other
// (storage) failures with ClassStorage.
func mapStoreErr(op string, err error) error {
	if err == nil {
		return nil
	}
	if re, ok := err.(*Error); ok {
		return re
	}
	return regErr(ClassStorage, op, err.Error())
}
