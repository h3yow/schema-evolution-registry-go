// Package compat implements schema evolution compatibility judgement.
//
// Three strategies are supported:
//
//   - BACKWARD: new-schema consumers can read data written with the old schema
//     ("new reads old"). Adding a field needs a default; removing is harmless.
//   - FORWARD:  old-schema consumers can read data written with the new schema
//     ("old reads new"). Removing/renaming away a field needs the old field to
//     carry a default; adding is harmless.
//   - FULL:     both directions must hold.
//
// Field matching is by exact name first, then by aliases (renames). Type
// changes follow a fixed promotion table, e.g. int -> long -> float -> double
// and string -> bytes. Every conclusion is returned as an explainable Finding;
// there is no silent downgrade to "compatible".
package compat

import (
	"context"
	"fmt"
	"sort"

	"github.com/h3yow/schema-evolution-registry-go/internal/schemadef"
)

// Strategy selects an evolution compatibility policy.
type Strategy string

const (
	StrategyBackward Strategy = "BACKWARD"
	StrategyForward  Strategy = "FORWARD"
	StrategyFull     Strategy = "FULL"
)

// Valid reports whether s is a known strategy.
func (s Strategy) Valid() bool {
	switch s {
	case StrategyBackward, StrategyForward, StrategyFull:
		return true
	default:
		return false
	}
}

// ChangeKind categorizes the difference detected for a field.
type ChangeKind string

const (
	ChangeAdded      ChangeKind = "FIELD_ADDED"
	ChangeRemoved    ChangeKind = "FIELD_REMOVED"
	ChangeRenamed    ChangeKind = "FIELD_RENAMED"
	ChangeTypeChange ChangeKind = "TYPE_CHANGED"
)

// Finding is one explainable difference between two schemas.
type Finding struct {
	Kind    ChangeKind
	Field   string // primary field: new name for add/rename/type, old name for remove
	OldName string // populated for renames
	NewName string // populated for renames
	OldType string
	NewType string
	// Detail explains the judgement basis, including whether a default resolved it.
	Detail string
}

// Report is the explainable result of a compatibility check.
type Report struct {
	Compatible bool
	Strategy   Strategy
	Findings   []Finding
	// Violations lists the findings that make the schemas incompatible under
	// the chosen strategy; empty when Compatible is true.
	Violations []Finding
}

// enrichedFinding carries default-presence flags used during direction checks.
type enrichedFinding struct {
	Finding
	oldHasDefault bool
	newHasDefault bool
	oldType       string
	newType       string
}

// Checker judges schema pairs against a fixed strategy.
type Checker struct {
	strategy Strategy
}

// NewChecker returns a checker for the given strategy.
func NewChecker(strategy Strategy) *Checker { return &Checker{strategy: strategy} }

// Check compares old and new and returns an explainable report.
func (c *Checker) Check(ctx context.Context, old, new *schemadef.Schema) (*Report, error) {
	return Check(ctx, c.strategy, old, new)
}

// Check validates a standalone strategy value and performs the comparison.
func Check(_ context.Context, strategy Strategy, old, new *schemadef.Schema) (*Report, error) {
	if !strategy.Valid() {
		return nil, fmt.Errorf("unknown compatibility strategy %q", strategy)
	}
	if old == nil || new == nil {
		return nil, fmt.Errorf("compat: both schemas must be non-nil")
	}

	diffs := diff(old, new)

	findings := make([]Finding, 0, len(diffs))
	violationSet := make(map[int]bool)
	for i := range diffs {
		findings = append(findings, diffs[i].Finding)
	}
	for _, dir := range directionsFor(strategy) {
		for i := range diffs {
			if violates(dir, &diffs[i]) {
				violationSet[i] = true
			}
		}
	}
	violations := make([]Finding, 0, len(violationSet))
	for i := range diffs {
		if violationSet[i] {
			violations = append(violations, diffs[i].Finding)
		}
	}
	return &Report{
		Compatible: len(violations) == 0,
		Strategy:   strategy,
		Findings:   findings,
		Violations: violations,
	}, nil
}

type direction int

const (
	dirBackward direction = iota // new reader, old writer
	dirForward                   // old reader, new writer
)

func directionsFor(s Strategy) []direction {
	switch s {
	case StrategyBackward:
		return []direction{dirBackward}
	case StrategyForward:
		return []direction{dirForward}
	case StrategyFull:
		return []direction{dirBackward, dirForward}
	default:
		return nil
	}
}

// violates reports whether f blocks evolution in the given read direction.
func violates(dir direction, f *enrichedFinding) bool {
	switch f.Kind {
	case ChangeAdded:
		// New schema has a field the old data does not contain. A new reader
		// needs a default (BACKWARD); an old reader simply ignores the extra
		// field (FORWARD is fine).
		if dir == dirBackward && !f.newHasDefault {
			f.Finding.Detail = fmt.Sprintf("added field %q has no default; old data cannot populate it", f.Field)
			return true
		}
		return false

	case ChangeRemoved:
		// New data lacks a field the old reader still expects. The old reader
		// needs a default (FORWARD); a new reader ignoring the surplus old
		// field is fine (BACKWARD).
		if dir == dirForward && !f.oldHasDefault {
			f.Finding.Detail = fmt.Sprintf("removed field %q has no default; old readers cannot resolve it in new data", f.Field)
			return true
		}
		return false

	case ChangeRenamed:
		if dir == dirBackward {
			// The new schema declares the old name as alias (or vice versa), so
			// the new reader projects old data onto the new name.
			return false
		}
		// Old readers know nothing about the new name; they can only resolve
		// the missing old field when that old field declares a default.
		if !f.oldHasDefault {
			f.Finding.Detail = fmt.Sprintf("renamed field %q -> %q has no default on the old field; old readers cannot resolve it", f.OldName, f.NewName)
			return true
		}
		return false

	case ChangeTypeChange:
		from, to := f.oldType, f.newType
		if dir == dirForward {
			// Old reader reading new data: the NEW type must promote to OLD.
			from, to = f.newType, f.oldType
		}
		if !promotable(from, to) {
			f.Finding.Detail = fmt.Sprintf("type %q is not promotable to %q", from, to)
			return true
		}
		return false
	}
	return false
}

// promotions defines one-way readable type promotions: data of the key type can
// be read as a value of one of the listed types.
var promotions = map[string][]string{
	schemadef.TypeInt:    {schemadef.TypeLong, schemadef.TypeFloat, schemadef.TypeDouble},
	schemadef.TypeLong:   {schemadef.TypeFloat, schemadef.TypeDouble},
	schemadef.TypeFloat:  {schemadef.TypeDouble},
	schemadef.TypeString: {schemadef.TypeBytes},
}

func promotable(from, to string) bool {
	if from == to {
		return true
	}
	for _, t := range promotions[from] {
		if t == to {
			return true
		}
	}
	return false
}

// diff computes the field-level differences. Names resolve first; remaining
// fields resolve through aliases deterministically.
func diff(old, new *schemadef.Schema) []enrichedFinding {
	oldByName := map[string]*schemadef.Field{}
	for i := range old.Fields {
		oldByName[old.Fields[i].Name] = &old.Fields[i]
	}
	newByName := map[string]*schemadef.Field{}
	for i := range new.Fields {
		newByName[new.Fields[i].Name] = &new.Fields[i]
	}

	matchedOld := map[string]bool{}
	matchedNew := map[string]bool{}

	var out []enrichedFinding

	// Pass 1: exact-name matches surface type changes.
	for nname, nf := range newByName {
		if of, ok := oldByName[nname]; ok {
			matchedOld[of.Name] = true
			matchedNew[nname] = true
			if of.Type != nf.Type {
				out = append(out, enrichedFinding{
					Finding: Finding{
						Kind:    ChangeTypeChange,
						Field:   nname,
						OldType: of.Type,
						NewType: nf.Type,
						Detail:  fmt.Sprintf("field %q changed type %s -> %s", nname, of.Type, nf.Type),
					},
					oldHasDefault: of.HasDefault,
					newHasDefault: nf.HasDefault,
					oldType:       of.Type,
					newType:       nf.Type,
				})
			}
		}
	}

	// Pass 2: deterministic alias resolution for unmatched fields. A new field
	// renames an old field when either side lists the other's name as an alias.
	var unmatchedOld, unmatchedNew []string
	for n := range oldByName {
		if !matchedOld[n] {
			unmatchedOld = append(unmatchedOld, n)
		}
	}
	for n := range newByName {
		if !matchedNew[n] {
			unmatchedNew = append(unmatchedNew, n)
		}
	}
	sort.Strings(unmatchedOld)
	sort.Strings(unmatchedNew)

	for _, on := range unmatchedOld {
		if matchedOld[on] {
			continue
		}
		of := oldByName[on]
		candidate := ""
		for _, nn := range unmatchedNew {
			if matchedNew[nn] {
				continue
			}
			nf := newByName[nn]
			if contains(nf.Aliases, of.Name) || contains(of.Aliases, nf.Name) {
				candidate = nn
				break // smallest candidate name, unmatchedNew is sorted
			}
		}
		if candidate != "" {
			nf := newByName[candidate]
			matchedOld[on] = true
			matchedNew[candidate] = true
			out = append(out, enrichedFinding{
				Finding: Finding{
					Kind:    ChangeRenamed,
					Field:   candidate,
					OldName: on,
					NewName: candidate,
					OldType: of.Type,
					NewType: nf.Type,
					Detail:  fmt.Sprintf("field %q renamed to %q (alias match)", on, candidate),
				},
				oldHasDefault: of.HasDefault,
				newHasDefault: nf.HasDefault,
				oldType:       of.Type,
				newType:       nf.Type,
			})
			if of.Type != nf.Type {
				out = append(out, enrichedFinding{
					Finding: Finding{
						Kind:    ChangeTypeChange,
						Field:   candidate,
						OldName: on,
						NewName: candidate,
						OldType: of.Type,
						NewType: nf.Type,
						Detail:  fmt.Sprintf("renamed field %q -> %q also changed type %s -> %s", on, candidate, of.Type, nf.Type),
					},
					oldHasDefault: of.HasDefault,
					newHasDefault: nf.HasDefault,
					oldType:       of.Type,
					newType:       nf.Type,
				})
			}
		}
	}

	// Pass 3: everything still unmatched is added or removed.
	for _, n := range unmatchedNew {
		if matchedNew[n] {
			continue
		}
		nf := newByName[n]
		detail := fmt.Sprintf("field %q added in new schema", n)
		if nf.HasDefault {
			detail += " with default"
		} else {
			detail += " without default"
		}
		out = append(out, enrichedFinding{
			Finding: Finding{
				Kind:   ChangeAdded,
				Field:  n,
				Detail: detail,
			},
			newHasDefault: nf.HasDefault,
			newType:       nf.Type,
		})
	}
	for _, n := range unmatchedOld {
		if matchedOld[n] {
			continue
		}
		of := oldByName[n]
		detail := fmt.Sprintf("field %q removed from new schema", n)
		if of.HasDefault {
			detail += " (old field has default)"
		}
		out = append(out, enrichedFinding{
			Finding: Finding{
				Kind:   ChangeRemoved,
				Field:  n,
				Detail: detail,
			},
			oldHasDefault: of.HasDefault,
			oldType:       of.Type,
		})
	}
	return out
}

func contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}
