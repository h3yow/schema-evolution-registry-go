package compat

import (
	"context"
	"errors"
	"fmt"

	"github.com/h3yow/schema-evolution-registry-go/schema"
)

// MalformedError is returned when a schema structure cannot be evaluated.
// It is never downgraded into a compatible verdict.
type MalformedError struct{ Msg string }

func (e *MalformedError) Error() string { return "malformed schema: " + e.Msg }

// IsMalformed reports whether err is a malformed-schema error.
func IsMalformed(err error) bool {
	var m *MalformedError
	return errors.As(err, &m)
}

// ChangeType distinguishes the kind of difference found between two schemas.
type ChangeType string

const (
	ChangeAddedField   ChangeType = "FIELD_ADDED"
	ChangeRemovedField ChangeType = "FIELD_REMOVED"
	ChangeRenamedField ChangeType = "FIELD_RENAMED"
	ChangeTypeChanged  ChangeType = "FIELD_TYPE_CHANGED"
)

// FieldChange describes one difference between two schemas.
// For FIELD_ADDED From is empty; for FIELD_REMOVED To is empty;
// for FIELD_RENAMED From/To carry the old/new names in Field/NewName.
type FieldChange struct {
	Type    ChangeType
	Field   string
	NewName string
	From    schema.Kind
	To      schema.Kind
}

func (ch FieldChange) String() string {
	switch ch.Type {
	case ChangeAddedField:
		return fmt.Sprintf("%s(field=%s,type=%s)", ch.Type, ch.Field, ch.To)
	case ChangeRemovedField:
		return fmt.Sprintf("%s(field=%s,type=%s)", ch.Type, ch.Field, ch.From)
	case ChangeRenamedField:
		return fmt.Sprintf("%s(from=%s,to=%s,type=%s)", ch.Type, ch.Field, ch.NewName, ch.To)
	case ChangeTypeChanged:
		return fmt.Sprintf("%s(field=%s,from=%s,to=%s)", ch.Type, ch.Field, ch.From, ch.To)
	default:
		return string(ch.Type)
	}
}

// Result is the explainable conclusion of a compatibility check.
type Result struct {
	Compatible bool
	Strategy   schema.Compatibility
	Changes    []FieldChange
	Reasons    []string
}

// Checker validates and compares schemas under an evolution strategy.
type Checker interface {
	Validate(ctx context.Context, s schema.Schema) error
	Check(ctx context.Context, existing, candidate schema.Schema, strategy schema.Compatibility) (Result, error)
}

type checker struct{}

// NewChecker returns the built-in rule based checker.
func NewChecker() Checker { return &checker{} }

var validKinds = map[schema.Kind]bool{
	schema.KindString: true,
	schema.KindInt:    true,
	schema.KindLong:   true,
	schema.KindFloat:  true,
	schema.KindDouble: true,
	schema.KindBool:   true,
	schema.KindBytes:  true,
}

// Validate enforces the structural preconditions of any judgement.
// A schema that fails validation can never be registered or compared.
func (c *checker) Validate(_ context.Context, s schema.Schema) error {
	if len(s.Fields) == 0 {
		return &MalformedError{Msg: "schema must declare at least one field"}
	}
	seen := make(map[string]bool, len(s.Fields))
	for i, f := range s.Fields {
		if f.Name == "" {
			return &MalformedError{Msg: fmt.Sprintf("field at index %d has an empty name", i)}
		}
		if seen[f.Name] {
			return &MalformedError{Msg: fmt.Sprintf("duplicate field name %q", f.Name)}
		}
		seen[f.Name] = true
		if !validKinds[f.Type] {
			return &MalformedError{Msg: fmt.Sprintf("field %q has unknown type %q", f.Name, f.Type)}
		}
	}
	return nil
}

// diff computes the field level changes. A rename is only inferred when the
// candidate removes exactly one field and adds exactly one field carrying the
// same kind; any other add/remove pattern is reported literally so that
// conclusions stay deterministic and explainable.
func diff(existing, candidate schema.Schema) []FieldChange {
	oldFields := make(map[string]schema.Field, len(existing.Fields))
	newFields := make(map[string]schema.Field, len(candidate.Fields))
	for _, f := range existing.Fields {
		oldFields[f.Name] = f
	}
	for _, f := range candidate.Fields {
		newFields[f.Name] = f
	}

	var removed, added []schema.Field
	for _, f := range existing.Fields {
		if _, ok := newFields[f.Name]; !ok {
			removed = append(removed, f)
		}
	}
	for _, f := range candidate.Fields {
		if _, ok := oldFields[f.Name]; !ok {
			added = append(added, f)
		}
	}

	changes := make([]FieldChange, 0)

	// Same-named fields: detect type changes.
	for _, of := range existing.Fields {
		nf, ok := newFields[of.Name]
		if !ok {
			continue
		}
		if of.Type != nf.Type {
			changes = append(changes, FieldChange{
				Type:  ChangeTypeChanged,
				Field: of.Name,
				From:  of.Type,
				To:    nf.Type,
			})
		}
	}

	switch {
	case len(removed) == 1 && len(added) == 1 && removed[0].Type == added[0].Type:
		changes = append(changes, FieldChange{
			Type:    ChangeRenamedField,
			Field:   removed[0].Name,
			NewName: added[0].Name,
			From:    removed[0].Type,
			To:      added[0].Type,
		})
	default:
		for _, f := range removed {
			changes = append(changes, FieldChange{Type: ChangeRemovedField, Field: f.Name, From: f.Type})
		}
		for _, f := range added {
			changes = append(changes, FieldChange{Type: ChangeAddedField, Field: f.Name, To: f.Type})
		}
	}
	return changes
}

// kindDirection describes resolvability between two primitive kinds.
type kindDirection int

const (
	kindEqual      kindDirection = 0
	kindWidening   kindDirection = 1 // old value safely read as new kind
	kindNarrowing  kindDirection = 2 // new value required to fit old kind
	kindUnresolved kindDirection = 3
)

var promotionChains = [][]schema.Kind{
	{schema.KindInt, schema.KindLong, schema.KindFloat, schema.KindDouble},
}

func compareKind(from, to schema.Kind) kindDirection {
	if from == to {
		return kindEqual
	}
	// Avro treats string and bytes as mutually promotable.
	if (from == schema.KindString && to == schema.KindBytes) ||
		(from == schema.KindBytes && to == schema.KindString) {
		return kindWidening
	}
	for _, chain := range promotionChains {
		fi, ti := -1, -1
		for i, k := range chain {
			if k == from {
				fi = i
			}
			if k == to {
				ti = i
			}
		}
		if fi >= 0 && ti >= 0 {
			switch {
			case ti > fi:
				return kindWidening
			default:
				return kindNarrowing
			}
		}
	}
	return kindUnresolved
}

// allowChange decides whether one change is legal under the strategy and
// returns a human readable rejection reason when it is not.
func allowChange(ch FieldChange, oldFields, newFields map[string]schema.Field, strategy schema.Compatibility) (bool, string) {
	switch ch.Type {
	case ChangeRenamedField:
		return false, fmt.Sprintf("field %q renamed to %q: a rename is neither a pure addition nor a pure removal and is rejected under %s", ch.Field, ch.NewName, strategy)
	case ChangeAddedField:
		nf := newFields[ch.Field]
		switch strategy {
		case schema.CompatBackward, schema.CompatFull:
			if !nf.Optional {
				return false, fmt.Sprintf("required field %q added: existing data has no value for it (rejected under %s)", ch.Field, strategy)
			}
		case schema.CompatForward:
			// old readers simply ignore the extra field
		}
	case ChangeRemovedField:
		of := oldFields[ch.Field]
		switch strategy {
		case schema.CompatForward, schema.CompatFull:
			if !of.Optional {
				return false, fmt.Sprintf("required field %q removed: new data lacks a value old readers require (rejected under %s)", ch.Field, strategy)
			}
		case schema.CompatBackward:
			// new readers simply ignore the stale field
		}
	case ChangeTypeChanged:
		dir := compareKind(ch.From, ch.To)
		switch strategy {
		case schema.CompatBackward:
			if dir != kindWidening {
				return false, fmt.Sprintf("field %q type %s->%s is not a safe widening for reading old data (rejected under %s)", ch.Field, ch.From, ch.To, strategy)
			}
		case schema.CompatForward:
			if dir != kindNarrowing {
				return false, fmt.Sprintf("field %q type %s->%s is not a safe narrowing for old readers of new data (rejected under %s)", ch.Field, ch.From, ch.To, strategy)
			}
		case schema.CompatFull:
			return false, fmt.Sprintf("field %q type %s->%s: any type change fails bidirectional FULL compatibility", ch.Field, ch.From, ch.To)
		}
	}
	return true, ""
}

// Check validates both schemas and evaluates the candidate against the
// existing one under the given strategy. Malformed schemas produce an error,
// never a compatible result; strategy NONE is not evaluable here.
func (c *checker) Check(ctx context.Context, existing, candidate schema.Schema, strategy schema.Compatibility) (Result, error) {
	if err := c.Validate(ctx, existing); err != nil {
		return Result{}, err
	}
	if err := c.Validate(ctx, candidate); err != nil {
		return Result{}, err
	}
	switch strategy {
	case schema.CompatBackward, schema.CompatForward, schema.CompatFull:
	default:
		return Result{}, fmt.Errorf("invalid compatibility strategy %q", strategy)
	}

	changes := diff(existing, candidate)
	oldFields := make(map[string]schema.Field, len(existing.Fields))
	newFields := make(map[string]schema.Field, len(candidate.Fields))
	for _, f := range existing.Fields {
		oldFields[f.Name] = f
	}
	for _, f := range candidate.Fields {
		newFields[f.Name] = f
	}

	res := Result{Compatible: true, Strategy: strategy, Changes: changes}
	for _, ch := range changes {
		if ok, reason := allowChange(ch, oldFields, newFields, strategy); !ok {
			res.Compatible = false
			res.Reasons = append(res.Reasons, reason)
		}
	}
	return res, nil
}
