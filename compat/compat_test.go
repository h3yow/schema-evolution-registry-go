package compat

import (
	"context"
	"testing"

	"github.com/h3yow/schema-evolution-registry-go/schema"
)

func field(name string, k schema.Kind, optional bool) schema.Field {
	return schema.Field{Name: name, Type: k, Optional: optional}
}

func TestValidateMalformed(t *testing.T) {
	c := NewChecker()
	ctx := context.Background()
	cases := []struct {
		name string
		s    schema.Schema
	}{
		{"empty", schema.Schema{}},
		{"empty field name", schema.Schema{Fields: []schema.Field{{Name: "", Type: schema.KindInt}}}},
		{"duplicate names", schema.Schema{Fields: []schema.Field{field("a", schema.KindInt, false), field("a", schema.KindString, false)}}},
		{"unknown type", schema.Schema{Fields: []schema.Field{field("a", "decimal", false)}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := c.Validate(ctx, tc.s); err == nil {
				t.Fatalf("expected malformed error, got nil")
			} else if !IsMalformed(err) {
				t.Fatalf("expected *MalformedError, got %T: %v", err, err)
			}
		})
	}
}

func TestCompatibilityStrategies(t *testing.T) {
	c := NewChecker()
	ctx := context.Background()
	base := schema.Schema{Fields: []schema.Field{
		field("id", schema.KindLong, false),
		field("name", schema.KindString, false),
		field("nick", schema.KindString, true),
	}}

	cases := []struct {
		name       string
		candidate  schema.Schema
		strategy   schema.Compatibility
		wantOK     bool
		wantChange ChangeType
	}{
		{"backward/optional-added", schema.Schema{Fields: append(base.Fields, field("email", schema.KindString, true))}, schema.CompatBackward, true, ChangeAddedField},
		{"backward/required-added-rejected", schema.Schema{Fields: append(base.Fields, field("email", schema.KindString, false))}, schema.CompatBackward, false, ChangeAddedField},
		{"forward/required-added-ok", schema.Schema{Fields: append(base.Fields, field("email", schema.KindString, false))}, schema.CompatForward, true, ChangeAddedField},
		{"backward/removed-required-ok", schema.Schema{Fields: []schema.Field{base.Fields[0], base.Fields[2]}}, schema.CompatBackward, true, ChangeRemovedField},
		{"forward/removed-required-rejected", schema.Schema{Fields: []schema.Field{base.Fields[0], base.Fields[2]}}, schema.CompatForward, false, ChangeRemovedField},
		{"backward/rename-rejected", schema.Schema{Fields: []schema.Field{base.Fields[0], {Name: "fullname", Type: schema.KindString, Optional: false}, base.Fields[2]}}, schema.CompatBackward, false, ChangeRenamedField},
		{"full/rename-rejected", schema.Schema{Fields: []schema.Field{base.Fields[0], {Name: "fullname", Type: schema.KindString, Optional: false}, base.Fields[2]}}, schema.CompatFull, false, ChangeRenamedField},
		{"backward/widen-long-to-double-ok", schema.Schema{Fields: []schema.Field{field("id", schema.KindDouble, false), base.Fields[1], base.Fields[2]}}, schema.CompatBackward, true, ChangeTypeChanged},
		{"backward/narrow-double-to-long-rejected", schema.Schema{Fields: []schema.Field{field("id", schema.KindString, false), base.Fields[1], base.Fields[2]}}, schema.CompatBackward, false, ChangeTypeChanged},
		{"forward/narrow-double-to-long-ok", schema.Schema{Fields: []schema.Field{field("id", schema.KindInt, false), base.Fields[1], base.Fields[2]}}, schema.CompatForward, true, ChangeTypeChanged},
		{"forward/widen-int-to-double-rejected", schema.Schema{Fields: []schema.Field{field("id", schema.KindDouble, false), base.Fields[1], base.Fields[2]}}, schema.CompatForward, false, ChangeTypeChanged},
		{"full/typechange-rejected", schema.Schema{Fields: []schema.Field{field("id", schema.KindDouble, false), base.Fields[1], base.Fields[2]}}, schema.CompatFull, false, ChangeTypeChanged},
		{"full/identical-ok", base, schema.CompatFull, true, ""},
		{"backward/string-bytes-widen-ok", schema.Schema{Fields: []schema.Field{base.Fields[0], field("name", schema.KindBytes, false), base.Fields[2]}}, schema.CompatBackward, true, ChangeTypeChanged},
		{"backward/bool-int-unresolved", schema.Schema{Fields: []schema.Field{field("id", schema.KindBool, false), base.Fields[1], base.Fields[2]}}, schema.CompatBackward, false, ChangeTypeChanged},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := c.Check(ctx, base, tc.candidate, tc.strategy)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			t.Logf("strategy=%s compatible=%v changes=%v reasons=%v", res.Strategy, res.Compatible, res.Changes, res.Reasons)
			if res.Compatible != tc.wantOK {
				t.Fatalf("compatible=%v want %v; reasons=%v", res.Compatible, tc.wantOK, res.Reasons)
			}
			if tc.wantChange != "" {
				found := false
				for _, ch := range res.Changes {
					if ch.Type == tc.wantChange {
						found = true
					}
				}
				if !found {
					t.Fatalf("expected change %q in %v", tc.wantChange, res.Changes)
				}
			}
			if !res.Compatible && len(res.Reasons) == 0 {
				t.Fatalf("rejection must carry an explainable reason")
			}
		})
	}
}

func TestCheckMalformedCandidate(t *testing.T) {
	c := NewChecker()
	base := schema.Schema{Fields: []schema.Field{field("id", schema.KindLong, false)}}
	bad := schema.Schema{Fields: []schema.Field{{Name: "id", Type: "weird"}}}
	if _, err := c.Check(context.Background(), base, bad, schema.CompatBackward); err == nil || !IsMalformed(err) {
		t.Fatalf("expected malformed error, got %v", err)
	}
}
