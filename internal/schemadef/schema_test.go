package schemadef

import (
	"context"
	"strings"
	"testing"
)

func TestParseValid(t *testing.T) {
	raw := []byte(`{
		"name": "User",
		"fields": [
			{"name": "id", "type": "long"},
			{"name": "email", "type": "string", "default": null},
			{"name": "name", "type": "string", "aliases": ["full_name"], "default": "anon"}
		]
	}`)
	s, err := Parse(context.Background(), raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.Name != "User" || len(s.Fields) != 3 {
		t.Fatalf("unexpected schema: %+v", s)
	}
	// Fields are canonicalized to name order: email, id, name.
	if s.Fields[0].Name != "email" || s.Fields[2].Name != "name" {
		t.Fatalf("fields not sorted: %v", s.Fields)
	}
	if !s.FieldByName("email").HasDefault || string(s.FieldByName("email").Default) != "null" {
		t.Fatalf("null default must be preserved with HasDefault=true")
	}
}

func TestParseMalformed(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want string
	}{
		{"empty", ``, "empty document"},
		{"bad json", `{"name":`, "invalid JSON"},
		{"unknown attr", `{"name":"X","fields":[],"extra":1}`, "unknown attribute"},
		{"missing name", `{"fields":[]}`, `"name"`},
		{"missing fields", `{"name":"X"}`, `"fields"`},
		{"empty field name", `{"name":"X","fields":[{"type":"string"}]}`, `"name"`},
		{"duplicate fields", `{"name":"X","fields":[{"name":"a","type":"string"},{"name":"a","type":"string"}]}`, "duplicate field"},
		{"bad type", `{"name":"X","fields":[{"name":"a","type":"map"}]}`, "unsupported type"},
		{"default wrong type", `{"name":"X","fields":[{"name":"a","type":"int","default":"x"}]}`, "JSON integer"},
		{"default float for int", `{"name":"X","fields":[{"name":"a","type":"int","default":1.5}]}`, "integral"},
		{"default bool for string", `{"name":"X","fields":[{"name":"a","type":"string","default":true}]}`, "JSON string"},
		{"alias collides", `{"name":"X","fields":[{"name":"a","type":"string"},{"name":"b","type":"string","aliases":["a"]}]}`, "collides"},
		{"duplicate alias", `{"name":"X","fields":[{"name":"a","type":"string","aliases":["x"]},{"name":"b","type":"string","aliases":["x"]}]}`, "duplicate alias"},
		{"trailing tokens", `{"name":"X","fields":[]} {"name":"Y"}`, "trailing"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse(context.Background(), []byte(tc.raw))
			if err == nil {
				t.Fatalf("expected malformed error containing %q, got nil", tc.want)
			}
			pe, ok := err.(*ParseError)
			if !ok {
				t.Fatalf("expected *ParseError, got %T: %v", err, err)
			}
			if !strings.Contains(pe.Error(), tc.want) {
				t.Fatalf("error %q does not contain %q", pe.Error(), tc.want)
			}
		})
	}
}

func TestCanonicalContentIdentity(t *testing.T) {
	// Same meaning, different formatting / field order / alias order.
	a := []byte(`{"name":"X","fields":[{"name":"b","type":"long"},{"name":"a","type":"string","aliases":["z","y"]}]}`)
	b := []byte(`{
		"fields": [
			{"aliases": ["y", "z"], "type": "string", "name": "a"},
			{"type": "long", "name": "b"}
		],
		"name": "X"
	}`)
	sa, err := Parse(context.Background(), a)
	if err != nil {
		t.Fatal(err)
	}
	sb, err := Parse(context.Background(), b)
	if err != nil {
		t.Fatal(err)
	}
	ca, _ := sa.Canonical()
	cb, _ := sb.Canonical()
	if string(ca) != string(cb) {
		t.Fatalf("canonical forms differ:\n%s\n%s", ca, cb)
	}
	idA, _ := sa.ID()
	idB, _ := sb.ID()
	if idA != idB {
		t.Fatalf("content ids differ: %s vs %s", idA, idB)
	}

	// Different type -> different identity.
	diff := []byte(`{"name":"X","fields":[{"name":"a","type":"int"},{"name":"b","type":"long"}]}`)
	sc, _ := Parse(context.Background(), diff)
	idC, _ := sc.ID()
	if idC == idA {
		t.Fatal("schemas with different field types must have different ids")
	}
}

func TestDefaultNullVersusAbsent(t *testing.T) {
	withNull := []byte(`{"name":"X","fields":[{"name":"a","type":"string","default":null}]}`)
	without := []byte(`{"name":"X","fields":[{"name":"a","type":"string"}]}`)
	s1, _ := Parse(context.Background(), withNull)
	s2, _ := Parse(context.Background(), without)
	if !s1.Fields[0].HasDefault || s2.Fields[0].HasDefault {
		t.Fatal("explicit null default must differ from absent default")
	}
	id1, _ := s1.ID()
	id2, _ := s2.ID()
	if id1 == id2 {
		t.Fatal("explicit null default and absent default must produce different ids")
	}
}
