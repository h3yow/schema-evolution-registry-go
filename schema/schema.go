package schema

// Kind describes the primitive type of a field.
type Kind string

const (
	KindString Kind = "string"
	KindInt    Kind = "int"
	KindLong   Kind = "long"
	KindFloat  Kind = "float"
	KindDouble Kind = "double"
	KindBool   Kind = "bool"
	KindBytes  Kind = "bytes"
)

// Field is a single top-level attribute of a record schema.
type Field struct {
	Name     string `json:"name"`
	Type     Kind   `json:"type"`
	Optional bool   `json:"optional"`
}

// Schema is a flat record schema identified by content within a topic.
type Schema struct {
	Fields []Field `json:"fields"`
}

// Compatibility is the evolution strategy of a topic.
type Compatibility string

const (
	CompatNone     Compatibility = "NONE"
	CompatBackward Compatibility = "BACKWARD"
	CompatForward  Compatibility = "FORWARD"
	CompatFull     Compatibility = "FULL"
)
