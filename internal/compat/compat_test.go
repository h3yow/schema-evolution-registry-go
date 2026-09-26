package compat

import (
	"context"
	"strings"
	"testing"

	"github.com/h3yow/schema-evolution-registry-go/internal/schemadef"
)

func mustParse(t *testing.T, raw string) *schemadef.Schema {
	t.Helper()
	s, err := schemadef.Parse(context.Background(), []byte(raw))
	if err != nil {
		t.Fatalf("parse: %v\nraw: %s", err, raw)
	}
	return s
}

const baseSchema = `{"name":"E","fields":[
	{"name":"id","type":"long"},
	{"name":"name","type":"string"},
	{"name":"age","type":"int"}
]}`

func findByKind(fs []Finding, k ChangeKind) *Finding {
	for i := range fs {
		if fs[i].Kind == k {
			return &fs[i]
		}
	}
	return nil
}

func TestAddedField(t *testing.T) {
	old := mustParse(t, baseSchema)

	t.Run("no default backward rejects forward accepts", func(t *testing.T) {
		newS := mustParse(t, `{"name":"E","fields":[
			{"name":"id","type":"long"},{"name":"name","type":"string"},
			{"name":"age","type":"int"},{"name":"nick","type":"string"}]}`)

		rb, err := Check(context.Background(), StrategyBackward, old, newS)
		if err != nil || rb.Compatible {
			t.Fatalf("backward must reject added field without default: compat=%v err=%v", rb.Compatible, err)
		}
		if f := findByKind(rb.Violations, ChangeAdded); f == nil || f.Field != "nick" {
			t.Fatalf("expected FIELD_ADDED violation on nick, got %+v", rb.Violations)
		}

		rf, err := Check(context.Background(), StrategyForward, old, newS)
		if err != nil || !rf.Compatible {
			t.Fatalf("forward must accept added field: compat=%v err=%v", rf.Compatible, err)
		}
		if f := findByKind(rf.Findings, ChangeAdded); f == nil {
			t.Fatal("finding must still be recorded even when compatible")
		}
	})

	t.Run("with default full accepts", func(t *testing.T) {
		newS := mustParse(t, `{"name":"E","fields":[
			{"name":"id","type":"long"},{"name":"name","type":"string"},
			{"name":"age","type":"int"},{"name":"nick","type":"string","default":"x"}]}`)
		rf, err := Check(context.Background(), StrategyFull, old, newS)
		if err != nil || !rf.Compatible {
			t.Fatalf("full must accept added field with default: %v %v", rf, err)
		}
	})
}

func TestRemovedField(t *testing.T) {
	newS := mustParse(t, `{"name":"E","fields":[
		{"name":"id","type":"long"},{"name":"name","type":"string"}]}`)
	old := mustParse(t, baseSchema)

	rb, _ := Check(context.Background(), StrategyBackward, old, newS)
	if !rb.Compatible {
		t.Fatalf("backward must accept removal (new reader ignores old surplus): %v", rb.Violations)
	}
	rf, _ := Check(context.Background(), StrategyForward, old, newS)
	if rf.Compatible {
		t.Fatalf("forward must reject removal of field without default: %v", rf.Violations)
	}
	if f := findByKind(rf.Violations, ChangeRemoved); f == nil || f.Field != "age" {
		t.Fatalf("expected FIELD_REMOVED violation on age, got %+v", rf.Violations)
	}

	// When the removed old field carries a default, forward is fine.
	oldDef := mustParse(t, `{"name":"E","fields":[
		{"name":"id","type":"long"},{"name":"name","type":"string"},
		{"name":"age","type":"int","default":0}]}`)
	rf2, _ := Check(context.Background(), StrategyForward, oldDef, newS)
	if !rf2.Compatible {
		t.Fatalf("forward must accept removal when old field has default: %v", rf2.Violations)
	}
}

func TestRenamedField(t *testing.T) {
	old := mustParse(t, baseSchema)
	// new schema renames name -> full_name, declaring the old name as alias.
	newS := mustParse(t, `{"name":"E","fields":[
		{"name":"id","type":"long"},
		{"name":"full_name","type":"string","aliases":["name"]},
		{"name":"age","type":"int"}]}`)

	rb, _ := Check(context.Background(), StrategyBackward, old, newS)
	if !rb.Compatible {
		t.Fatalf("backward accepts alias rename: %v", rb.Violations)
	}
	rf, _ := Check(context.Background(), StrategyForward, old, newS)
	if rf.Compatible {
		t.Fatal("forward rejects alias rename without default on old field")
	}
	f := findByKind(rf.Violations, ChangeRenamed)
	if f == nil || f.OldName != "name" || f.NewName != "full_name" {
		t.Fatalf("expected RENAMED name->full_name, got %+v", rf.Violations)
	}

	// Full needs both directions: a default on the old field resolves forward.
	oldDef := mustParse(t, `{"name":"E","fields":[
		{"name":"id","type":"long"},
		{"name":"name","type":"string","default":""},
		{"name":"age","type":"int"}]}`)
	rf2, _ := Check(context.Background(), StrategyFull, oldDef, newS)
	if !rf2.Compatible {
		t.Fatalf("full accepts rename when old field has default: %v", rf2.Violations)
	}
}

func TestTypeChange(t *testing.T) {
	old := mustParse(t, baseSchema)

	// int -> double is a valid widening for new-reader (backward).
	widen := mustParse(t, `{"name":"E","fields":[
		{"name":"id","type":"long"},{"name":"name","type":"string"},
		{"name":"age","type":"double"}]}`)
	rb, _ := Check(context.Background(), StrategyBackward, old, widen)
	if !rb.Compatible {
		t.Fatalf("backward accepts int->double widening: %v", rb.Violations)
	}
	// Old reader sees double where it expects int: double -> int is not
	// promotable, so forward rejects with a distinct TYPE_CHANGED finding.
	rf, _ := Check(context.Background(), StrategyForward, old, widen)
	if rf.Compatible {
		t.Fatal("forward must reject double->int (not promotable)")
	}
	f := findByKind(rf.Violations, ChangeTypeChange)
	if f == nil || f.OldType != "int" || f.NewType != "double" {
		t.Fatalf("expected TYPE_CHANGED int->double, got %+v", rf.Violations)
	}
	if !strings.Contains(f.Detail, "not promotable") {
		t.Fatalf("detail must explain promotion failure, got %q", f.Detail)
	}

	// string -> int is incompatible in both directions => full rejects.
	s2i := mustParse(t, `{"name":"E","fields":[
		{"name":"id","type":"long"},{"name":"name","type":"int"},
		{"name":"age","type":"int"}]}`)
	rf2, _ := Check(context.Background(), StrategyFull, old, s2i)
	if rf2.Compatible {
		t.Fatal("full must reject string->int")
	}
}

func TestIdenticalAndStrategyValidation(t *testing.T) {
	s := mustParse(t, baseSchema)
	rep, err := Check(context.Background(), StrategyFull, s, s)
	if err != nil || !rep.Compatible || len(rep.Findings) != 0 {
		t.Fatalf("identical schemas must be fully compatible: %+v err=%v", rep, err)
	}
	if _, err := Check(context.Background(), "SIDEWAYS", s, s); err == nil {
		t.Fatal("unknown strategy must be an error, never silently compatible")
	}
}

func TestFindingsAreExplainable(t *testing.T) {
	old := mustParse(t, baseSchema)
	newS := mustParse(t, `{"name":"E","fields":[
		{"name":"id","type":"long"},{"name":"name","type":"string"},
		{"name":"age","type":"int"},{"name":"extra","type":"bytes"}]}`)
	rep, err := Check(context.Background(), StrategyFull, old, newS)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Compatible {
		t.Fatal("full must reject added field without default")
	}
	for _, v := range rep.Violations {
		if v.Detail == "" || v.Kind == "" {
			t.Fatalf("each violation must be explainable: %+v", v)
		}
		t.Logf("violation: kind=%s field=%s basis=%s", v.Kind, v.Field, v.Detail)
	}
}
