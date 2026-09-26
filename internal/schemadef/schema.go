// Package schemadef defines the local record schema model used by the registry.
//
// A schema describes a single record type: a set of named fields, each with a
// primitive type and an optional default value. Schemas are expressed as JSON
// documents so they can be stored and compared by content identity.
//
// Example document:
//
//	{
//	  "name": "User",
//	  "fields": [
//	    {"name": "id", "type": "long"},
//	    {"name": "email", "type": "string", "default": null},
//	    {"name": "user_name", "type": "string", "aliases": ["username"]}
//	  ]
//	}
//
// Parsing is strict: unknown keys, duplicate field/alias names, unsupported
// types and defaults that do not match the declared type are all reported as
// explicit malformed-schema errors rather than silently ignored.
package schemadef

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
)

// Primitive field types supported by the local registry.
const (
	TypeString = "string"
	TypeInt    = "int"
	TypeLong   = "long"
	TypeFloat  = "float"
	TypeDouble = "double"
	TypeBool   = "bool"
	TypeBytes  = "bytes"
)

var validTypes = map[string]bool{
	TypeString: true,
	TypeInt:    true,
	TypeLong:   true,
	TypeFloat:  true,
	TypeDouble: true,
	TypeBool:   true,
	TypeBytes:  true,
}

// Schema is a parsed, validated record schema.
type Schema struct {
	Name   string  `json:"name"`
	Fields []Field `json:"fields"`
}

// Field describes one field of a record schema.
type Field struct {
	Name    string          `json:"name"`
	Type    string          `json:"type"`
	Aliases []string        `json:"aliases,omitempty"`
	Default json.RawMessage `json:"default,omitempty"`
	// HasDefault reports whether an explicit default was provided. JSON null is
	// a legitimate default, so key presence cannot be inferred from the value.
	HasDefault bool `json:"-"`
}

// ParseError describes why a raw schema document is malformed.
type ParseError struct {
	Reason string
}

func (e *ParseError) Error() string { return "malformed schema: " + e.Reason }

// wireField is the on-disk JSON shape of one field.
type wireField struct {
	Name    string   `json:"name"`
	Type    string   `json:"type"`
	Aliases []string `json:"aliases,omitempty"`
	// Default is a value (not pointer): a missing key decodes to nil bytes,
	// while an explicit "default": null retains the literal "null" bytes via
	// json.RawMessage.UnmarshalJSON, so the two cases stay distinguishable.
	Default json.RawMessage `json:"default"`
}

// wireSchema is the on-disk JSON shape of a schema. Pointer fields let us
// distinguish "key missing" from "key present with zero value".
type wireSchema struct {
	Name   string       `json:"name"`
	Fields *[]wireField `json:"fields"`
}

// Parse decodes and validates a raw JSON schema document.
func Parse(_ context.Context, raw []byte) (*Schema, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil, &ParseError{Reason: "empty document"}
	}

	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var ws wireSchema
	if err := dec.Decode(&ws); err != nil {
		return nil, &ParseError{Reason: "invalid JSON or unknown attribute: " + err.Error()}
	}
	// Reject trailing tokens/garbage after the first JSON value.
	if dec.More() {
		return nil, &ParseError{Reason: "unexpected trailing content after schema document"}
	}

	if ws.Name == "" {
		return nil, &ParseError{Reason: `attribute "name" is required and must be non-empty`}
	}
	if ws.Fields == nil {
		return nil, &ParseError{Reason: `attribute "fields" is required and must be an array`}
	}

	out := &Schema{Name: ws.Name, Fields: make([]Field, 0, len(*ws.Fields))}
	seenNames := make(map[string]bool)
	seenAliases := make(map[string]bool)

	for i, wf := range *ws.Fields {
		loc := fmt.Sprintf("fields[%d]", i)
		if wf.Name == "" {
			return nil, &ParseError{Reason: loc + `: attribute "name" is required and must be non-empty`}
		}
		if seenNames[wf.Name] {
			return nil, &ParseError{Reason: loc + ": duplicate field name " + wf.Name}
		}
		if seenAliases[wf.Name] {
			return nil, &ParseError{Reason: loc + ": field name " + wf.Name + " collides with an alias of another field"}
		}
		seenNames[wf.Name] = true

		if !validTypes[wf.Type] {
			return nil, &ParseError{Reason: fmt.Sprintf("%s (%s): unsupported type %q", loc, wf.Name, wf.Type)}
		}

		aliases := append([]string(nil), wf.Aliases...)
		for _, a := range aliases {
			if a == "" {
				return nil, &ParseError{Reason: loc + " (" + wf.Name + "): empty alias"}
			}
			if a == wf.Name {
				return nil, &ParseError{Reason: loc + " (" + wf.Name + "): alias duplicates field name"}
			}
			if seenNames[a] {
				return nil, &ParseError{Reason: loc + " (" + wf.Name + "): alias " + a + " collides with an existing field name"}
			}
			if seenAliases[a] {
				return nil, &ParseError{Reason: loc + " (" + wf.Name + "): duplicate alias " + a}
			}
			seenAliases[a] = true
		}
		sort.Strings(aliases)

		f := Field{Name: wf.Name, Type: wf.Type, Aliases: aliases}
		if len(wf.Default) > 0 {
			compact := bytes.TrimSpace(wf.Default)
			if err := validateDefault(f.Name, wf.Type, compact); err != nil {
				return nil, err
			}
			f.HasDefault = true
			f.Default = append(json.RawMessage(nil), compact...)
		}
		out.Fields = append(out.Fields, f)
	}

	// Canonical field order is by name; record order is not semantically
	// significant for these record schemas.
	sort.Slice(out.Fields, func(i, j int) bool { return out.Fields[i].Name < out.Fields[j].Name })
	return out, nil
}

// validateDefault ensures a default literal is representable as the field type.
// JSON null is accepted as a default for every type (absence marker).
func validateDefault(fieldName, typ string, raw []byte) *ParseError {
	loc := "field " + fieldName + ": default"
	if string(raw) == "null" {
		return nil
	}
	switch typ {
	case TypeString, TypeBytes:
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return &ParseError{Reason: loc + " must be a JSON string for type " + typ}
		}
	case TypeBool:
		var b bool
		if err := json.Unmarshal(raw, &b); err != nil {
			return &ParseError{Reason: loc + " must be a JSON boolean for type bool"}
		}
	case TypeInt, TypeLong:
		var n json.Number
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		if err := dec.Decode(&n); err != nil {
			return &ParseError{Reason: loc + " must be a JSON integer for type " + typ}
		}
		if _, err := n.Int64(); err != nil {
			return &ParseError{Reason: loc + " must be an integral number for type " + typ}
		}
	case TypeFloat, TypeDouble:
		var n json.Number
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		if err := dec.Decode(&n); err != nil {
			return &ParseError{Reason: loc + " must be a JSON number for type " + typ}
		}
		if _, err := n.Float64(); err != nil {
			return &ParseError{Reason: loc + " must be numeric for type " + typ}
		}
	default:
		return &ParseError{Reason: loc + ": unsupported type " + typ}
	}
	return nil
}

// Canonical serializes the schema into a stable byte representation so that two
// documents with identical meaning (same name, fields, types, aliases and
// defaults) share the same content identity regardless of formatting, key
// ordering, alias ordering or field ordering.
func (s *Schema) Canonical() ([]byte, error) {
	cfgs := make([]struct {
		Name    string          `json:"name"`
		Type    string          `json:"type"`
		Aliases []string        `json:"aliases,omitempty"`
		Default json.RawMessage `json:"default,omitempty"`
	}, len(s.Fields))
	for i, f := range s.Fields {
		cfgs[i].Name = f.Name
		cfgs[i].Type = f.Type
		cfgs[i].Aliases = f.Aliases
		if f.HasDefault {
			cfgs[i].Default = f.Default
		}
	}
	canon := struct {
		Name   string `json:"name"`
		Fields []struct {
			Name    string          `json:"name"`
			Type    string          `json:"type"`
			Aliases []string        `json:"aliases,omitempty"`
			Default json.RawMessage `json:"default,omitempty"`
		} `json:"fields"`
	}{Name: s.Name, Fields: cfgs}
	return json.Marshal(canon)
}

// ID returns the hex SHA-256 content identity of the canonical schema.
func (s *Schema) ID() (string, error) {
	b, err := s.Canonical()
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// FieldByName returns the field with the given name, or nil if absent.
func (s *Schema) FieldByName(name string) *Field {
	for i := range s.Fields {
		if s.Fields[i].Name == name {
			return &s.Fields[i]
		}
	}
	return nil
}
